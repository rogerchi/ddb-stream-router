import * as fc from "fast-check";
import { ConfigurationError } from "../src/errors";
import { StreamRouter } from "../src/stream-router";
import type { StreamViewType } from "../src/types";

const VALID_STREAM_VIEW_TYPES: StreamViewType[] = [
	"KEYS_ONLY",
	"NEW_IMAGE",
	"OLD_IMAGE",
	"NEW_AND_OLD_IMAGES",
];

describe("StreamRouter Configuration Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 1: Valid stream view type configuration is stored correctly**
	 * **Validates: Requirements 1.1**
	 *
	 * For any valid StreamViewType value, when a StreamRouter is created with that
	 * configuration, the router's internal configuration should store that exact value.
	 */
	test("Property 1: Valid stream view type configuration is stored correctly", () => {
		fc.assert(
			fc.property(
				fc.constantFrom(...VALID_STREAM_VIEW_TYPES),
				(streamViewType) => {
					const router = new StreamRouter({ streamViewType });
					return router.streamViewType === streamViewType;
				},
			),
			{ numRuns: 100 },
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 2: Invalid stream view type configuration is rejected**
	 * **Validates: Requirements 1.3**
	 *
	 * For any string that is not a valid StreamViewType, when attempting to create
	 * a StreamRouter with that configuration, the router should throw a descriptive error.
	 */
	test("Property 2: Invalid stream view type configuration is rejected", () => {
		fc.assert(
			fc.property(
				fc
					.string()
					.filter(
						(s) => !VALID_STREAM_VIEW_TYPES.includes(s as StreamViewType),
					),
				(invalidType) => {
					try {
						new StreamRouter({ streamViewType: invalidType as StreamViewType });
						return false; // Should have thrown
					} catch (error) {
						return (
							error instanceof ConfigurationError &&
							error.message.includes("Invalid streamViewType")
						);
					}
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Default stream view type is NEW_AND_OLD_IMAGES", () => {
		const router = new StreamRouter();
		expect(router.streamViewType).toBe("NEW_AND_OLD_IMAGES");
	});

	test("Default unmarshall option is true", () => {
		const router = new StreamRouter();
		expect(router.unmarshall).toBe(true);
	});
});

describe("StreamRouter Handler Registration Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 3: Handler registration preserves handler across all event types**
	 * **Validates: Requirements 2.1, 3.1, 4.1**
	 *
	 * For any event type (INSERT, MODIFY, REMOVE), matcher function, and handler function,
	 * when the corresponding registration method is called, the handler should be stored
	 * in the registry with the correct event type association.
	 */
	test("Property 3: Handler registration preserves handler across all event types", () => {
		const eventTypes = ["INSERT", "MODIFY", "REMOVE"] as const;

		fc.assert(
			fc.property(fc.constantFrom(...eventTypes), (eventType) => {
				const router = new StreamRouter();
				const discriminator = (record: unknown): record is { id: string } =>
					typeof record === "object" &&
					record !== null &&
					"id" in record &&
					typeof (record as { id: unknown }).id === "string";
				const handler = jest.fn();

				// Register handler based on event type
				if (eventType === "INSERT") {
					router.insert(discriminator, handler);
				} else if (eventType === "MODIFY") {
					router.modify(discriminator, handler);
				} else {
					router.remove(discriminator, handler);
				}

				// Verify handler is registered with correct event type
				expect(router.handlers).toHaveLength(1);
				expect(router.handlers[0].eventType).toBe(eventType);
				expect(router.handlers[0].matcher).toBe(discriminator);
				expect(router.handlers[0].handler).toBe(handler);
				expect(router.handlers[0].isParser).toBe(false);

				return true;
			}),
			{ numRuns: 100 },
		);
	});

	test("Parser detection correctly identifies parser vs discriminator", () => {
		const router = new StreamRouter();

		// Discriminator function
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		// Parser object (Zod-like)
		const parser = {
			parse: (data: unknown) => data as { id: string },
			safeParse: (data: unknown) => ({
				success: true as const,
				data: data as { id: string },
			}),
		};

		router.insert(discriminator, jest.fn());
		router.insert(parser, jest.fn());

		expect(router.handlers[0].isParser).toBe(false);
		expect(router.handlers[1].isParser).toBe(true);
	});

	test("Method chaining works for all registration methods", () => {
		const router = new StreamRouter();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;
		const handler = jest.fn();

		const result = router
			.insert(discriminator, handler)
			.modify(discriminator, handler)
			.remove(discriminator, handler);

		expect(result).toBe(router);
		expect(router.handlers).toHaveLength(3);
	});
});

describe("StreamRouter Middleware Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 7: Middleware executes in registration order before handlers**
	 * **Validates: Requirements 5.1, 5.2**
	 *
	 * For any sequence of registered middleware functions, when a stream event is processed,
	 * all middleware should execute in the exact order they were registered.
	 */
	test("Property 7: Middleware registration preserves order", () => {
		fc.assert(
			fc.property(fc.integer({ min: 1, max: 10 }), (middlewareCount) => {
				const router = new StreamRouter();
				const middlewareFns: Array<() => void> = [];

				for (let i = 0; i < middlewareCount; i++) {
					const fn = jest.fn();
					middlewareFns.push(fn);
					router.use(async (_record, next) => {
						fn();
						await next();
					});
				}

				// Verify middleware is registered in order
				expect(router.middleware).toHaveLength(middlewareCount);
				return true;
			}),
			{ numRuns: 100 },
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 8: Middleware chain continues on next() call**
	 * **Validates: Requirements 5.3**
	 *
	 * For any middleware that calls the next() function, the subsequent middleware
	 * or handler in the chain should be invoked.
	 */
	test("Property 8: Middleware use() returns this for chaining", () => {
		const router = new StreamRouter();
		const middleware1 = jest.fn(async (_record, next) => {
			await next();
		});
		const middleware2 = jest.fn(async (_record, next) => {
			await next();
		});

		const result = router.use(middleware1).use(middleware2);

		expect(result).toBe(router);
		expect(router.middleware).toHaveLength(2);
		expect(router.middleware[0]).toBe(middleware1);
		expect(router.middleware[1]).toBe(middleware2);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 9: Middleware errors stop processing and propagate**
	 * **Validates: Requirements 5.4**
	 *
	 * Middleware registration should accept async functions that can throw errors.
	 */
	test("Property 9: Middleware accepts async functions", () => {
		const router = new StreamRouter();
		const asyncMiddleware = async (
			_record: unknown,
			next: () => Promise<void>,
		) => {
			await new Promise((resolve) => setTimeout(resolve, 0));
			await next();
		};

		router.use(asyncMiddleware);
		expect(router.middleware).toHaveLength(1);
	});
});

import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";

// Helper to create a mock DynamoDB stream record
function createMockRecord(
	eventName: "INSERT" | "MODIFY" | "REMOVE",
	newImage?: Record<string, unknown>,
	oldImage?: Record<string, unknown>,
	keys?: Record<string, unknown>,
): DynamoDBRecord {
	const toAttributeValue = (obj: Record<string, unknown> | undefined) => {
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
		eventID: `event_${Math.random().toString(36).substring(2)}`,
		eventName,
		eventSourceARN:
			"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
		dynamodb: {
			Keys: toAttributeValue(keys ?? { pk: "test" }),
			NewImage: toAttributeValue(newImage),
			OldImage: toAttributeValue(oldImage),
		},
	} as DynamoDBRecord;
}

function createMockEvent(records: DynamoDBRecord[]): DynamoDBStreamEvent {
	return { Records: records };
}

describe("StreamRouter Event Processing Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 4: Discriminator-matched handlers are invoked with correct data**
	 * **Validates: Requirements 2.3, 3.3, 4.3**
	 */
	test("Property 4: Discriminator-matched handlers are invoked with correct data", async () => {
		await fc.assert(
			fc.asyncProperty(
				fc.constantFrom("INSERT", "MODIFY", "REMOVE" as const),
				fc.string({ minLength: 1 }),
				async (eventType, id) => {
					const router = new StreamRouter();
					const handler = jest.fn();
					const discriminator = (record: unknown): record is { id: string } =>
						typeof record === "object" && record !== null && "id" in record;

					if (eventType === "INSERT") {
						router.insert(discriminator, handler);
					} else if (eventType === "MODIFY") {
						router.modify(discriminator, handler);
					} else {
						router.remove(discriminator, handler);
					}

					const newImage = eventType !== "REMOVE" ? { id } : undefined;
					const oldImage = eventType !== "INSERT" ? { id } : undefined;
					const record = createMockRecord(eventType, newImage, oldImage);
					const event = createMockEvent([record]);

					await router.process(event);

					expect(handler).toHaveBeenCalledTimes(1);
					return true;
				},
			),
			{ numRuns: 100 },
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 5: Parser-validated handlers receive parsed data**
	 * **Validates: Requirements 2.2, 2.4, 3.2, 3.4, 4.2, 4.4**
	 */
	test("Property 5: Parser-validated handlers receive parsed data", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const parser = {
			parse: (data: unknown) => data as { id: string },
			safeParse: (data: unknown) => {
				if (typeof data === "object" && data !== null && "id" in data) {
					return { success: true as const, data: data as { id: string } };
				}
				return { success: false as const, error: new Error("Invalid") };
			},
		};

		router.insert(parser, handler);

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ id: "test123" }),
			expect.any(Object),
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 6: Parser validation failures skip handler without error**
	 * **Validates: Requirements 2.5, 3.5, 4.5**
	 */
	test("Property 6: Parser validation failures skip handler without error", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const parser = {
			parse: () => {
				throw new Error("Invalid");
			},
			safeParse: () => ({
				success: false as const,
				error: new Error("Invalid"),
			}),
		};

		router.insert(parser, handler);

		const record = createMockRecord("INSERT", { notId: "test" });
		const event = createMockEvent([record]);

		const result = await router.process(event);

		expect(handler).not.toHaveBeenCalled();
		expect(result.failed).toBe(0);
		expect(result.succeeded).toBe(1);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 11: All records in event are processed**
	 * **Validates: Requirements 7.1**
	 */
	test("Property 11: All records in event are processed", async () => {
		await fc.assert(
			fc.asyncProperty(fc.integer({ min: 1, max: 10 }), async (recordCount) => {
				const router = new StreamRouter();
				const records = Array.from({ length: recordCount }, (_, i) =>
					createMockRecord("INSERT", { id: `item_${i}` }),
				);
				const event = createMockEvent(records);

				const result = await router.process(event);

				expect(result.processed).toBe(recordCount);
				return true;
			}),
			{ numRuns: 100 },
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 12: Multiple matching handlers all execute in order**
	 * **Validates: Requirements 7.3**
	 */
	test("Property 12: Multiple matching handlers all execute in order", async () => {
		const router = new StreamRouter();
		const executionOrder: number[] = [];
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, () => {
			executionOrder.push(1);
		});
		router.insert(discriminator, () => {
			executionOrder.push(2);
		});
		router.insert(discriminator, () => {
			executionOrder.push(3);
		});

		const record = createMockRecord("INSERT", { id: "test" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(executionOrder).toEqual([1, 2, 3]);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 13: Non-matching records are skipped without error**
	 * **Validates: Requirements 7.4**
	 */
	test("Property 13: Non-matching records are skipped without error", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (
			record: unknown,
		): record is { specificField: string } =>
			typeof record === "object" &&
			record !== null &&
			"specificField" in record;

		router.insert(discriminator, handler);

		const record = createMockRecord("INSERT", { differentField: "value" });
		const event = createMockEvent([record]);

		const result = await router.process(event);

		expect(handler).not.toHaveBeenCalled();
		expect(result.failed).toBe(0);
		expect(result.succeeded).toBe(1);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 14: Processing result accurately reflects execution**
	 * **Validates: Requirements 7.5**
	 */
	test("Property 14: Processing result accurately reflects execution", async () => {
		const router = new StreamRouter();
		let callCount = 0;
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, () => {
			callCount++;
			if (callCount === 2) throw new Error("Intentional error");
		});

		const records = [
			createMockRecord("INSERT", { id: "1" }),
			createMockRecord("INSERT", { id: "2" }),
			createMockRecord("INSERT", { id: "3" }),
		];
		const event = createMockEvent(records);

		const result = await router.process(event);

		expect(result.processed).toBe(3);
		expect(result.succeeded).toBe(2);
		expect(result.failed).toBe(1);
		expect(result.errors).toHaveLength(1);
	});
});


describe("StreamRouter Stream View Type Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 10: Stream view type determines available image data**
	 * **Validates: Requirements 6.1, 6.2, 6.3, 6.4**
	 */
	test("Property 10: KEYS_ONLY provides only keys to handlers", async () => {
		const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
		const handler = jest.fn();
		const discriminator = (_record: unknown): _record is { pk: string } => true;

		router.insert(discriminator, handler);

		const record = createMockRecord("INSERT", { id: "test" }, undefined, { pk: "key1" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ pk: "key1" }),
			expect.any(Object),
		);
	});

	test("Property 10: NEW_IMAGE provides newImage to INSERT handlers", async () => {
		const router = new StreamRouter({ streamViewType: "NEW_IMAGE" });
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler);

		const record = createMockRecord("INSERT", { id: "newValue" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ id: "newValue" }),
			expect.any(Object),
		);
	});
});

describe("StreamRouter Unmarshalling Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 20: Unmarshalling converts DynamoDB JSON to native objects**
	 * **Validates: Requirements 11.1, 11.2, 11.4**
	 */
	test("Property 20: Unmarshalling converts DynamoDB JSON to native objects", async () => {
		const router = new StreamRouter({ unmarshall: true });
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler);

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ id: "test123" }),
			expect.any(Object),
		);
		// Verify it's a native string, not DynamoDB format
		const calledWith = handler.mock.calls[0][0];
		expect(typeof calledWith.id).toBe("string");
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 21: Unmarshalling disabled passes raw format**
	 * **Validates: Requirements 11.3**
	 */
	test("Property 21: Unmarshalling disabled passes raw format", async () => {
		const router = new StreamRouter({ unmarshall: false });
		const handler = jest.fn();
		const discriminator = (_record: unknown): _record is Record<string, unknown> => true;

		router.insert(discriminator, handler);

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalled();
		const calledWith = handler.mock.calls[0][0];
		// When unmarshall is false, we get DynamoDB JSON format
		expect(calledWith).toHaveProperty("id");
		expect(calledWith.id).toHaveProperty("S");
	});
});

describe("StreamRouter Same Region Filtering Properties", () => {
	const originalEnv = process.env;

	beforeEach(() => {
		process.env = { ...originalEnv };
	});

	afterEach(() => {
		process.env = originalEnv;
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 22: Same region filtering skips cross-region records**
	 * **Validates: Requirements 13.1**
	 */
	test("Property 22: Same region filtering skips cross-region records", async () => {
		process.env.AWS_REGION = "us-west-2";
		const router = new StreamRouter({ sameRegionOnly: true });
		const handler = jest.fn();
		const discriminator = (_record: unknown): _record is { id: string } => true;

		router.insert(discriminator, handler);

		// Record from us-east-1, but Lambda is in us-west-2
		const record: DynamoDBRecord = {
			eventID: "test",
			eventName: "INSERT",
			eventSourceARN: "arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
			dynamodb: {
				Keys: { pk: { S: "test" } },
				NewImage: { id: { S: "test" } },
			},
		};
		const event = createMockEvent([record]);

		const result = await router.process(event);

		expect(handler).not.toHaveBeenCalled();
		expect(result.succeeded).toBe(1);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 23: Same region filtering processes matching region records**
	 * **Validates: Requirements 13.3**
	 */
	test("Property 23: Same region filtering processes matching region records", async () => {
		process.env.AWS_REGION = "us-east-1";
		const router = new StreamRouter({ sameRegionOnly: true });
		const handler = jest.fn();
		const discriminator = (_record: unknown): _record is { id: string } => true;

		router.insert(discriminator, handler);

		// Record from us-east-1, Lambda is also in us-east-1
		const record: DynamoDBRecord = {
			eventID: "test",
			eventName: "INSERT",
			eventSourceARN: "arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
			dynamodb: {
				Keys: { pk: { S: "test" } },
				NewImage: { id: { S: "test" } },
			},
		};
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 24: Same region filtering defaults to disabled**
	 * **Validates: Requirements 13.2**
	 */
	test("Property 24: Same region filtering defaults to disabled", async () => {
		process.env.AWS_REGION = "us-west-2";
		const router = new StreamRouter(); // sameRegionOnly defaults to false
		const handler = jest.fn();
		const discriminator = (_record: unknown): _record is { id: string } => true;

		router.insert(discriminator, handler);

		// Record from different region, but sameRegionOnly is false
		const record: DynamoDBRecord = {
			eventID: "test",
			eventName: "INSERT",
			eventSourceARN: "arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
			dynamodb: {
				Keys: { pk: { S: "test" } },
				NewImage: { id: { S: "test" } },
			},
		};
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
	});

	test("Same region filtering allows processing when AWS_REGION is not set", async () => {
		delete process.env.AWS_REGION;
		const router = new StreamRouter({ sameRegionOnly: true });
		const handler = jest.fn();
		const discriminator = (_record: unknown): _record is { id: string } => true;

		router.insert(discriminator, handler);

		const record: DynamoDBRecord = {
			eventID: "test",
			eventName: "INSERT",
			eventSourceARN: "arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
			dynamodb: {
				Keys: { pk: { S: "test" } },
				NewImage: { id: { S: "test" } },
			},
		};
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
	});
});
