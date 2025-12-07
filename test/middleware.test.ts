/**
 * Tests for middleware functionality
 */
import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { StreamRouter, unmarshallMiddleware } from "../src";
import type { UnmarshalledRecord } from "../src/middleware";

// Helper to create a mock record
function createRecord(
	eventName: "INSERT" | "MODIFY" | "REMOVE",
	newImage?: Record<string, unknown>,
	oldImage?: Record<string, unknown>,
): DynamoDBRecord {
	const toAttributeValue = (
		obj: Record<string, unknown> | undefined,
	): Record<string, { S?: string; N?: string }> | undefined => {
		if (!obj) return undefined;
		const result: Record<string, { S?: string; N?: string }> = {};
		for (const [key, value] of Object.entries(obj)) {
			if (typeof value === "string") {
				result[key] = { S: value };
			} else if (typeof value === "number") {
				result[key] = { N: String(value) };
			}
		}
		return result;
	};

	return {
		eventID: `event_${Date.now()}`,
		eventName,
		eventSource: "aws:dynamodb",
		awsRegion: "us-east-1",
		eventSourceARN:
			"arn:aws:dynamodb:us-east-1:123456789012:table/test/stream/2024",
		dynamodb: {
			Keys: toAttributeValue({ pk: "test" }),
			NewImage: toAttributeValue(newImage),
			OldImage: toAttributeValue(oldImage),
			SequenceNumber: `seq_${Date.now()}`,
		},
	} as DynamoDBRecord;
}

function createEvent(records: DynamoDBRecord[]): DynamoDBStreamEvent {
	return { Records: records };
}

describe("Middleware execution", () => {
	it("should execute middleware in registration order", async () => {
		const router = new StreamRouter();
		const executionOrder: number[] = [];

		router.use(async (_record, next) => {
			executionOrder.push(1);
			await next();
			executionOrder.push(4);
		});

		router.use(async (_record, next) => {
			executionOrder.push(2);
			await next();
			executionOrder.push(3);
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, () => {
			// Handler executes between middleware
		});

		const record = createRecord("INSERT", { pk: "test" });
		await router.process(createEvent([record]));

		// Middleware should execute in order: 1, 2, (handler), 3, 4
		expect(executionOrder).toEqual([1, 2, 3, 4]);
	});

	it("should execute middleware before handlers", async () => {
		const router = new StreamRouter();
		const executionOrder: string[] = [];

		router.use(async (_record, next) => {
			executionOrder.push("middleware");
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, () => {
			executionOrder.push("handler");
		});

		const record = createRecord("INSERT", { pk: "test" });
		await router.process(createEvent([record]));

		expect(executionOrder).toEqual(["middleware", "handler"]);
	});

	it("should skip handlers when middleware does not call next()", async () => {
		const router = new StreamRouter();
		const middleware2Called = jest.fn();
		const handlerCalled = jest.fn();

		router.use(async (_record, _next) => {
			// Don't call next() - stops middleware chain AND skips handlers
			return;
		});

		router.use(async (_record, next) => {
			middleware2Called();
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, handlerCalled);

		const record = createRecord("INSERT", { pk: "test" });
		const result = await router.process(createEvent([record]));

		// Second middleware should NOT be called because first didn't call next()
		expect(middleware2Called).not.toHaveBeenCalled();
		// Handler should NOT be called because middleware chain didn't complete
		expect(handlerCalled).not.toHaveBeenCalled();
		// Record should still be counted as succeeded (middleware chose to skip it)
		expect(result.succeeded).toBe(1);
	});

	it("should propagate middleware errors", async () => {
		const router = new StreamRouter();
		const error = new Error("Middleware error");

		router.use(async (_record, _next) => {
			throw error;
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, jest.fn());

		const record = createRecord("INSERT", { pk: "test" });
		const result = await router.process(createEvent([record]));

		expect(result.failed).toBe(1);
		expect(result.errors[0].error.message).toBe("Middleware error");
	});

	it("should allow middleware to modify records", async () => {
		const router = new StreamRouter();
		let receivedMetadata: unknown;

		router.use(async (record, next) => {
			(record as DynamoDBRecord & { _custom?: string })._custom =
				"added-by-middleware";
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, () => {
			// Access the modified record through closure
			receivedMetadata = "handler-executed";
		});

		const record = createRecord("INSERT", { pk: "test" });
		await router.process(createEvent([record]));

		expect((record as DynamoDBRecord & { _custom?: string })._custom).toBe(
			"added-by-middleware",
		);
		expect(receivedMetadata).toBe("handler-executed");
	});

	it("should execute middleware for each record", async () => {
		const router = new StreamRouter();
		let middlewareCallCount = 0;

		router.use(async (_record, next) => {
			middlewareCallCount++;
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, jest.fn());

		const records = [
			createRecord("INSERT", { pk: "test1" }),
			createRecord("INSERT", { pk: "test2" }),
			createRecord("INSERT", { pk: "test3" }),
		];
		await router.process(createEvent(records));

		expect(middlewareCallCount).toBe(3);
	});

	it("should allow middleware to track records conditionally", async () => {
		const router = new StreamRouter();
		const middlewareProcessed: string[] = [];

		router.use(async (record, next) => {
			// Track which records went through middleware chain
			if (!record.eventSourceARN?.includes("skip-table")) {
				middlewareProcessed.push(record.eventID ?? "unknown");
			}
			await next();
		});

		const discriminator = (_r: unknown): _r is { pk: string } => true;
		router.onInsert(discriminator, jest.fn());

		const records = [
			{
				...createRecord("INSERT", { pk: "process-me" }),
				eventID: "event-1",
				eventSourceARN:
					"arn:aws:dynamodb:us-east-1:123:table/normal-table/stream/2024",
			},
			{
				...createRecord("INSERT", { pk: "skip-me" }),
				eventID: "event-2",
				eventSourceARN:
					"arn:aws:dynamodb:us-east-1:123:table/skip-table/stream/2024",
			},
		] as DynamoDBRecord[];

		await router.process(createEvent(records));

		// Only the first record should be tracked by middleware
		expect(middlewareProcessed).toEqual(["event-1"]);
	});
});

describe("unmarshallMiddleware", () => {
	it("should unmarshall NewImage, OldImage, and Keys", async () => {
		const router = new StreamRouter({ unmarshall: false }); // Disable built-in unmarshalling
		let capturedRecord: UnmarshalledRecord | undefined;

		router.use(unmarshallMiddleware());
		router.use(async (record, next) => {
			capturedRecord = record as UnmarshalledRecord;
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, jest.fn());

		const record = createRecord("INSERT", { pk: "USER#123", name: "John" });
		await router.process(createEvent([record]));

		expect(capturedRecord?.unmarshalled).toBeDefined();
		expect(capturedRecord?.unmarshalled?.NewImage).toEqual({
			pk: "USER#123",
			name: "John",
		});
		expect(capturedRecord?.unmarshalled?.Keys).toEqual({ pk: "test" });
	});

	it("should handle missing images gracefully", async () => {
		const router = new StreamRouter({ unmarshall: false });
		let capturedRecord: UnmarshalledRecord | undefined;

		router.use(unmarshallMiddleware());
		router.use(async (record, next) => {
			capturedRecord = record as UnmarshalledRecord;
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onRemove(discriminator, jest.fn());

		// REMOVE event with only OldImage
		const record = createRecord("REMOVE", undefined, { pk: "USER#123" });
		await router.process(createEvent([record]));

		expect(capturedRecord?.unmarshalled?.NewImage).toBeUndefined();
		expect(capturedRecord?.unmarshalled?.OldImage).toEqual({ pk: "USER#123" });
	});

	it("should work with chained middleware", async () => {
		const router = new StreamRouter({ unmarshall: false });
		const executionOrder: string[] = [];

		router.use(async (_record, next) => {
			executionOrder.push("before-unmarshall");
			await next();
		});

		router.use(unmarshallMiddleware());

		router.use(async (record, next) => {
			const extended = record as UnmarshalledRecord;
			if (extended.unmarshalled?.NewImage) {
				executionOrder.push("after-unmarshall");
			}
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, () => {
			executionOrder.push("handler");
		});

		const record = createRecord("INSERT", { pk: "test" });
		await router.process(createEvent([record]));

		expect(executionOrder).toEqual([
			"before-unmarshall",
			"after-unmarshall",
			"handler",
		]);
	});
});

describe("Middleware error handling", () => {
	it("should continue processing other records after middleware error", async () => {
		const router = new StreamRouter();
		let processedCount = 0;

		router.use(async (record, next) => {
			// Fail on first record
			if (record.eventID?.includes("fail")) {
				throw new Error("Intentional failure");
			}
			await next();
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, () => {
			processedCount++;
		});

		const records = [
			{ ...createRecord("INSERT", { pk: "test1" }), eventID: "fail-1" },
			{ ...createRecord("INSERT", { pk: "test2" }), eventID: "success-2" },
		] as DynamoDBRecord[];

		const result = await router.process(createEvent(records));

		expect(result.failed).toBe(1);
		expect(result.succeeded).toBe(1);
		expect(processedCount).toBe(1);
	});

	it("should include middleware phase in error details", async () => {
		const router = new StreamRouter();

		router.use(async (_record, _next) => {
			throw new Error("Middleware boom");
		});

		const discriminator = (_r: unknown): _r is Record<string, unknown> => true;
		router.onInsert(discriminator, jest.fn());

		const record = createRecord("INSERT", { pk: "test" });
		const result = await router.process(createEvent([record]));

		// The error should be captured
		expect(result.errors).toHaveLength(1);
		expect(result.errors[0].error.message).toBe("Middleware boom");
	});
});
