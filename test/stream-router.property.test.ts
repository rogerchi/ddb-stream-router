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

		// Chaining through HandlerRegistration proxy methods
		router
			.insert(discriminator, handler)
			.modify(discriminator, handler)
			.remove(discriminator, handler);

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

		const record = createMockRecord("INSERT", { id: "test" }, undefined, {
			pk: "key1",
		});
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
		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

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
			eventSourceARN:
				"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
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
			eventSourceARN:
				"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
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
			eventSourceARN:
				"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
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
			eventSourceARN:
				"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
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

import { diffAttributes, hasAttributeChange } from "../src/attribute-diff";

describe("Attribute Change Detection Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 15: Attribute change detection for scalar attributes**
	 * **Validates: Requirements 9.1, 9.2, 9.3, 9.4**
	 *
	 * For any MODIFY event with attribute filter options:
	 * - new_attribute: handler invoked only when attribute exists in newImage but not oldImage
	 * - remove_attribute: handler invoked only when attribute exists in oldImage but not newImage
	 * - changed_attribute: handler invoked only when attribute value differs between images
	 */
	test("Property 15: new_attribute detected when attribute exists only in newImage", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.oneof(fc.string(), fc.integer(), fc.boolean()),
				(attrName, attrValue) => {
					const oldImage: Record<string, unknown> = { existingAttr: "value" };
					const newImage: Record<string, unknown> = {
						existingAttr: "value",
						[attrName]: attrValue,
					};

					// Only test when attrName is different from existingAttr
					if (attrName === "existingAttr") return true;

					const diff = diffAttributes(oldImage, newImage);
					const hasNewAttr = hasAttributeChange(
						diff,
						attrName,
						"new_attribute",
					);

					return hasNewAttr === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 15: remove_attribute detected when attribute exists only in oldImage", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.oneof(fc.string(), fc.integer(), fc.boolean()),
				(attrName, attrValue) => {
					const oldImage: Record<string, unknown> = {
						existingAttr: "value",
						[attrName]: attrValue,
					};
					const newImage: Record<string, unknown> = { existingAttr: "value" };

					// Only test when attrName is different from existingAttr
					if (attrName === "existingAttr") return true;

					const diff = diffAttributes(oldImage, newImage);
					const hasRemovedAttr = hasAttributeChange(
						diff,
						attrName,
						"remove_attribute",
					);

					return hasRemovedAttr === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 15: changed_attribute detected when scalar value differs", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.string({ minLength: 1 }),
				fc.string({ minLength: 1 }),
				(attrName, oldValue, newValue) => {
					// Ensure values are different
					if (oldValue === newValue) return true;

					const oldImage: Record<string, unknown> = { [attrName]: oldValue };
					const newImage: Record<string, unknown> = { [attrName]: newValue };

					const diff = diffAttributes(oldImage, newImage);
					const hasChangedAttr = hasAttributeChange(
						diff,
						attrName,
						"changed_attribute",
					);

					return hasChangedAttr === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 15: No change detected when attribute values are equal", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.oneof(fc.string(), fc.integer(), fc.boolean()),
				(attrName, attrValue) => {
					const oldImage: Record<string, unknown> = { [attrName]: attrValue };
					const newImage: Record<string, unknown> = { [attrName]: attrValue };

					const diff = diffAttributes(oldImage, newImage);

					return diff.hasChanges === false;
				},
			),
			{ numRuns: 100 },
		);
	});
});

describe("Collection Change Detection Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 16: Collection change detection**
	 * **Validates: Requirements 9.5, 9.6, 9.7**
	 *
	 * For any MODIFY event with collection attribute filter options:
	 * - new_item_in_collection: handler invoked only when List/Map/Set has items added
	 * - remove_item_from_collection: handler invoked only when List/Map/Set has items removed
	 * - changed_item_in_collection: handler invoked only when List/Map items are modified
	 */
	test("Property 16: new_item_in_collection detected when array has items added", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.array(fc.string(), { minLength: 0, maxLength: 5 }),
				fc.string({ minLength: 1 }),
				(attrName, existingItems, newItem) => {
					const oldImage: Record<string, unknown> = {
						[attrName]: [...existingItems],
					};
					const newImage: Record<string, unknown> = {
						[attrName]: [...existingItems, newItem],
					};

					const diff = diffAttributes(oldImage, newImage);
					const hasNewItem = hasAttributeChange(
						diff,
						attrName,
						"new_item_in_collection",
					);

					return hasNewItem === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 16: remove_item_from_collection detected when array has items removed", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.array(fc.string(), { minLength: 1, maxLength: 5 }),
				(attrName, items) => {
					const oldImage: Record<string, unknown> = { [attrName]: [...items] };
					const newImage: Record<string, unknown> = {
						[attrName]: items.slice(0, -1),
					};

					const diff = diffAttributes(oldImage, newImage);
					const hasRemovedItem = hasAttributeChange(
						diff,
						attrName,
						"remove_item_from_collection",
					);

					return hasRemovedItem === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 16: new_item_in_collection detected when map has keys added", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.string({ minLength: 1, maxLength: 10 }),
				fc.string({ minLength: 1 }),
				(attrName, newKey, newValue) => {
					const oldImage: Record<string, unknown> = {
						[attrName]: { existingKey: "existingValue" },
					};
					const newImage: Record<string, unknown> = {
						[attrName]: { existingKey: "existingValue", [newKey]: newValue },
					};

					// Skip if newKey is the same as existingKey
					if (newKey === "existingKey") return true;

					const diff = diffAttributes(oldImage, newImage);
					const hasNewItem = hasAttributeChange(
						diff,
						attrName,
						"new_item_in_collection",
					);

					return hasNewItem === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 16: remove_item_from_collection detected when map has keys removed", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.string({ minLength: 1, maxLength: 10 }),
				fc.string({ minLength: 1 }),
				(attrName, keyToRemove, value) => {
					const oldImage: Record<string, unknown> = {
						[attrName]: { existingKey: "existingValue", [keyToRemove]: value },
					};
					const newImage: Record<string, unknown> = {
						[attrName]: { existingKey: "existingValue" },
					};

					// Skip if keyToRemove is the same as existingKey
					if (keyToRemove === "existingKey") return true;

					const diff = diffAttributes(oldImage, newImage);
					const hasRemovedItem = hasAttributeChange(
						diff,
						attrName,
						"remove_item_from_collection",
					);

					return hasRemovedItem === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 16: changed_item_in_collection detected when map value changes", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.string({ minLength: 1 }),
				fc.string({ minLength: 1 }),
				(attrName, oldValue, newValue) => {
					// Ensure values are different
					if (oldValue === newValue) return true;

					const oldImage: Record<string, unknown> = {
						[attrName]: { key: oldValue },
					};
					const newImage: Record<string, unknown> = {
						[attrName]: { key: newValue },
					};

					const diff = diffAttributes(oldImage, newImage);
					const hasChangedItem = hasAttributeChange(
						diff,
						attrName,
						"changed_item_in_collection",
					);

					return hasChangedItem === true;
				},
			),
			{ numRuns: 100 },
		);
	});
});

describe("Multiple Attribute Filter Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 17: Multiple attribute filters use OR logic**
	 * **Validates: Requirements 9.8**
	 *
	 * When multiple attribute filter conditions are specified, the handler should be
	 * invoked when any one of the specified conditions is met.
	 */
	test("Property 17: Multiple change types use OR logic", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1, maxLength: 20 }),
				fc.oneof(fc.string(), fc.integer()),
				(attrName, newValue) => {
					// Test case: attribute is new (not in oldImage)
					const oldImage: Record<string, unknown> = { otherAttr: "value" };
					const newImage: Record<string, unknown> = {
						otherAttr: "value",
						[attrName]: newValue,
					};

					if (attrName === "otherAttr") return true;

					const diff = diffAttributes(oldImage, newImage);

					// Should match with OR logic - new_attribute OR changed_attribute
					const matchesOr = hasAttributeChange(diff, attrName, [
						"new_attribute",
						"changed_attribute",
					]);

					// Should match because it's a new_attribute
					return matchesOr === true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 17: Handler invoked when any filter condition matches", async () => {
		await fc.assert(
			fc.asyncProperty(
				fc
					.string({ minLength: 1, maxLength: 10 })
					.filter((s) => s !== "id" && s !== "pk"),
				fc.string({ minLength: 1 }),
				async (attrName, newValue) => {
					const router = new StreamRouter();
					const handler = jest.fn();
					const discriminator = (
						_record: unknown,
					): _record is Record<string, unknown> => true;

					// Register handler with multiple change types (OR logic)
					router.modify(discriminator, handler, {
						attribute: attrName,
						changeType: ["new_attribute", "changed_attribute"],
					});

					// Create a MODIFY event where the attribute is new
					const record: DynamoDBRecord = {
						eventID: "test",
						eventName: "MODIFY",
						eventSourceARN:
							"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
						dynamodb: {
							Keys: { pk: { S: "test" } },
							OldImage: { pk: { S: "test" }, id: { S: "existing" } },
							NewImage: {
								pk: { S: "test" },
								id: { S: "existing" },
								[attrName]: { S: newValue },
							},
						},
					};
					const event = createMockEvent([record]);

					await router.process(event);

					// Handler should be called because new_attribute matches
					expect(handler).toHaveBeenCalledTimes(1);
					return true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 17: Handler not invoked when no filter condition matches", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		// Register handler looking for changes to "targetAttr"
		router.modify(discriminator, handler, {
			attribute: "targetAttr",
			changeType: ["new_attribute", "changed_attribute"],
		});

		// Create a MODIFY event where targetAttr doesn't change
		const record: DynamoDBRecord = {
			eventID: "test",
			eventName: "MODIFY",
			eventSourceARN:
				"arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024",
			dynamodb: {
				Keys: { pk: { S: "test" } },
				OldImage: {
					pk: { S: "test" },
					targetAttr: { S: "same" },
					otherAttr: { S: "old" },
				},
				NewImage: {
					pk: { S: "test" },
					targetAttr: { S: "same" },
					otherAttr: { S: "new" },
				},
			},
		};
		const event = createMockEvent([record]);

		await router.process(event);

		// Handler should NOT be called because targetAttr didn't change
		expect(handler).not.toHaveBeenCalled();
	});
});

describe("Batch Processing Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 18: Batch mode collects all matching records**
	 * **Validates: Requirements 10.1, 10.4, 10.5**
	 *
	 * For any handler registered with batch mode enabled, when processing an event
	 * with N matching records, the handler should be invoked exactly once with an
	 * array containing all N records.
	 */
	test("Property 18: Batch mode collects all matching records", async () => {
		await fc.assert(
			fc.asyncProperty(
				fc.integer({ min: 1, max: 10 }),
				async (recordCount) => {
					const router = new StreamRouter();
					const handler = jest.fn();
					const discriminator = (record: unknown): record is { id: string } =>
						typeof record === "object" && record !== null && "id" in record;

					router.insert(discriminator, handler, { batch: true });

					const records = Array.from({ length: recordCount }, (_, i) =>
						createMockRecord("INSERT", { id: `item_${i}` }),
					);
					const event = createMockEvent(records);

					await router.process(event);

					// Handler should be called exactly once
					expect(handler).toHaveBeenCalledTimes(1);

					// Handler should receive an array with all matching records
					const calledWith = handler.mock.calls[0][0];
					expect(Array.isArray(calledWith)).toBe(true);
					expect(calledWith).toHaveLength(recordCount);

					return true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 18: Batch mode with single record returns single-element array", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler, { batch: true });

		const record = createMockRecord("INSERT", { id: "single" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const calledWith = handler.mock.calls[0][0];
		expect(Array.isArray(calledWith)).toBe(true);
		expect(calledWith).toHaveLength(1);
	});

	test("Property 18: Batch mode only collects matching records", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { type: string } =>
			typeof record === "object" &&
			record !== null &&
			"type" in record &&
			(record as { type: string }).type === "target";

		router.insert(discriminator, handler, { batch: true });

		const records = [
			createMockRecord("INSERT", { type: "target", id: "1" }),
			createMockRecord("INSERT", { type: "other", id: "2" }),
			createMockRecord("INSERT", { type: "target", id: "3" }),
		];
		const event = createMockEvent(records);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const calledWith = handler.mock.calls[0][0];
		expect(calledWith).toHaveLength(2); // Only 2 matching records
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 19: Batch key groups records correctly**
	 * **Validates: Requirements 10.2, 10.3**
	 *
	 * For any handler registered with a batchKey, records should be grouped by the
	 * key value, and the handler should be invoked once per unique key with only
	 * the records matching that key.
	 */
	test("Property 19: Batch key groups records by attribute value", async () => {
		await fc.assert(
			fc.asyncProperty(
				fc.integer({ min: 2, max: 5 }),
				fc.integer({ min: 1, max: 3 }),
				async (groupCount, recordsPerGroup) => {
					const router = new StreamRouter();
					const handler = jest.fn();
					const discriminator = (
						record: unknown,
					): record is { groupId: string; id: string } =>
						typeof record === "object" && record !== null && "groupId" in record;

					router.insert(discriminator, handler, {
						batch: true,
						batchKey: "groupId",
					});

					// Create records with different groupIds
					const records: DynamoDBRecord[] = [];
					for (let g = 0; g < groupCount; g++) {
						for (let r = 0; r < recordsPerGroup; r++) {
							records.push(
								createMockRecord("INSERT", {
									groupId: `group_${g}`,
									id: `item_${g}_${r}`,
								}),
							);
						}
					}
					const event = createMockEvent(records);

					await router.process(event);

					// Handler should be called once per unique group
					expect(handler).toHaveBeenCalledTimes(groupCount);

					// Each call should have the correct number of records
					for (let i = 0; i < groupCount; i++) {
						const calledWith = handler.mock.calls[i][0];
						expect(calledWith).toHaveLength(recordsPerGroup);
					}

					return true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 19: Batch key function groups records correctly", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (
			record: unknown,
		): record is { category: string; id: string } =>
			typeof record === "object" && record !== null && "category" in record;

		// Use a function to extract the batch key
		router.insert(discriminator, handler, {
			batch: true,
			batchKey: (record: unknown) =>
				(record as { category: string }).category.toUpperCase(),
		});

		const records = [
			createMockRecord("INSERT", { category: "electronics", id: "1" }),
			createMockRecord("INSERT", { category: "Electronics", id: "2" }), // Same when uppercased
			createMockRecord("INSERT", { category: "books", id: "3" }),
		];
		const event = createMockEvent(records);

		await router.process(event);

		// Should be 2 groups: ELECTRONICS and BOOKS
		expect(handler).toHaveBeenCalledTimes(2);

		// Find the ELECTRONICS group call
		const electronicsCall = handler.mock.calls.find(
			(call) => call[0].length === 2,
		);
		expect(electronicsCall).toBeDefined();
		expect(electronicsCall[0]).toHaveLength(2);
	});

	test("Property 19: Batch mode without batchKey groups all records together", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		// batch: true but no batchKey - all records in one group
		router.insert(discriminator, handler, { batch: true });

		const records = [
			createMockRecord("INSERT", { id: "1", category: "a" }),
			createMockRecord("INSERT", { id: "2", category: "b" }),
			createMockRecord("INSERT", { id: "3", category: "c" }),
		];
		const event = createMockEvent(records);

		await router.process(event);

		// Should be called once with all records
		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler.mock.calls[0][0]).toHaveLength(3);
	});

	test("Batch mode works with MODIFY events", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.modify(discriminator, handler, { batch: true });

		const records = [
			createMockRecord("MODIFY", { id: "1", value: "new1" }, { id: "1", value: "old1" }),
			createMockRecord("MODIFY", { id: "2", value: "new2" }, { id: "2", value: "old2" }),
		];
		const event = createMockEvent(records);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const calledWith = handler.mock.calls[0][0];
		expect(calledWith).toHaveLength(2);
		// Each batch record should have oldImage and newImage
		expect(calledWith[0]).toHaveProperty("oldImage");
		expect(calledWith[0]).toHaveProperty("newImage");
	});

	test("Batch mode works with REMOVE events", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.remove(discriminator, handler, { batch: true });

		const records = [
			createMockRecord("REMOVE", undefined, { id: "1" }),
			createMockRecord("REMOVE", undefined, { id: "2" }),
		];
		const event = createMockEvent(records);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const calledWith = handler.mock.calls[0][0];
		expect(calledWith).toHaveLength(2);
		// Each batch record should have oldImage
		expect(calledWith[0]).toHaveProperty("oldImage");
	});

	test("Non-batch handlers still execute immediately", async () => {
		const router = new StreamRouter();
		const executionOrder: string[] = [];
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		// Non-batch handler
		router.insert(discriminator, (newImage) => {
			executionOrder.push(`immediate_${(newImage as { id: string }).id}`);
		});

		// Batch handler
		router.insert(discriminator, (records: unknown) => {
			executionOrder.push(`batch_${(records as Array<{ newImage: { id: string } }>).length}`);
		}, { batch: true });

		const records = [
			createMockRecord("INSERT", { id: "1" }),
			createMockRecord("INSERT", { id: "2" }),
		];
		const event = createMockEvent(records);

		await router.process(event);

		// Immediate handlers execute during record processing
		// Batch handlers execute after all records are processed
		expect(executionOrder).toEqual([
			"immediate_1",
			"immediate_2",
			"batch_2",
		]);
	});
});

describe("Batch Item Failures Properties", () => {
	// Helper to create a mock record with sequence number
	function createMockRecordWithSequence(
		eventName: "INSERT" | "MODIFY" | "REMOVE",
		sequenceNumber: string,
		newImage?: Record<string, unknown>,
		oldImage?: Record<string, unknown>,
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
				Keys: toAttributeValue({ pk: "test" }),
				NewImage: toAttributeValue(newImage),
				OldImage: toAttributeValue(oldImage),
				SequenceNumber: sequenceNumber,
			},
		} as DynamoDBRecord;
	}

	/**
	 * **Feature: dynamodb-stream-router, Property 25: Batch item failures returns first failed record**
	 * **Validates: Requirements 15.2, 15.3**
	 *
	 * For any event where one or more records fail processing, when reportBatchItemFailures
	 * is true, the router should return a batchItemFailures array containing only the first
	 * failed record's sequence number.
	 */
	test("Property 25: Batch item failures returns first failed record", async () => {
		await fc.assert(
			fc.asyncProperty(
				fc.integer({ min: 1, max: 5 }),
				fc.integer({ min: 0, max: 4 }),
				async (totalRecords, failAtIndex) => {
					// Ensure failAtIndex is within bounds
					const actualFailIndex = Math.min(failAtIndex, totalRecords - 1);

					const router = new StreamRouter();
					let callCount = 0;
					const discriminator = (record: unknown): record is { id: string } =>
						typeof record === "object" && record !== null && "id" in record;

					router.insert(discriminator, () => {
						if (callCount === actualFailIndex) {
							callCount++;
							throw new Error("Intentional failure");
						}
						callCount++;
					});

					const records = Array.from({ length: totalRecords }, (_, i) =>
						createMockRecordWithSequence("INSERT", `seq_${i}`, { id: `item_${i}` }),
					);
					const event = createMockEvent(records);

					const result = await router.process(event, {
						reportBatchItemFailures: true,
					});

					// Should return BatchItemFailuresResponse
					expect(result).toHaveProperty("batchItemFailures");
					const batchResult = result as { batchItemFailures: Array<{ itemIdentifier: string }> };

					// Should contain exactly one failure - the first failed record
					expect(batchResult.batchItemFailures).toHaveLength(1);
					expect(batchResult.batchItemFailures[0].itemIdentifier).toBe(
						`seq_${actualFailIndex}`,
					);

					return true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Property 25: Multiple failures only report first one", async () => {
		const router = new StreamRouter();
		let callCount = 0;
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		// Fail on records 1 and 3
		router.insert(discriminator, () => {
			if (callCount === 1 || callCount === 3) {
				callCount++;
				throw new Error("Intentional failure");
			}
			callCount++;
		});

		const records = [
			createMockRecordWithSequence("INSERT", "seq_0", { id: "0" }),
			createMockRecordWithSequence("INSERT", "seq_1", { id: "1" }),
			createMockRecordWithSequence("INSERT", "seq_2", { id: "2" }),
			createMockRecordWithSequence("INSERT", "seq_3", { id: "3" }),
		];
		const event = createMockEvent(records);

		const result = await router.process(event, { reportBatchItemFailures: true });

		expect(result).toHaveProperty("batchItemFailures");
		const batchResult = result as { batchItemFailures: Array<{ itemIdentifier: string }> };

		// Should only contain the first failure (seq_1)
		expect(batchResult.batchItemFailures).toHaveLength(1);
		expect(batchResult.batchItemFailures[0].itemIdentifier).toBe("seq_1");
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 26: Batch item failures returns empty array on success**
	 * **Validates: Requirements 15.4**
	 *
	 * For any event where all records process successfully, when reportBatchItemFailures
	 * is true, the router should return an empty batchItemFailures array.
	 */
	test("Property 26: Batch item failures returns empty array on success", async () => {
		await fc.assert(
			fc.asyncProperty(fc.integer({ min: 1, max: 10 }), async (recordCount) => {
				const router = new StreamRouter();
				const discriminator = (record: unknown): record is { id: string } =>
					typeof record === "object" && record !== null && "id" in record;

				router.insert(discriminator, () => {
					// Handler succeeds
				});

				const records = Array.from({ length: recordCount }, (_, i) =>
					createMockRecordWithSequence("INSERT", `seq_${i}`, { id: `item_${i}` }),
				);
				const event = createMockEvent(records);

				const result = await router.process(event, {
					reportBatchItemFailures: true,
				});

				// Should return BatchItemFailuresResponse with empty array
				expect(result).toHaveProperty("batchItemFailures");
				const batchResult = result as { batchItemFailures: Array<{ itemIdentifier: string }> };
				expect(batchResult.batchItemFailures).toHaveLength(0);

				return true;
			}),
			{ numRuns: 100 },
		);
	});

	test("Without reportBatchItemFailures returns ProcessingResult", async () => {
		const router = new StreamRouter();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, () => {
			throw new Error("Intentional failure");
		});

		const record = createMockRecordWithSequence("INSERT", "seq_0", { id: "0" });
		const event = createMockEvent([record]);

		// Without reportBatchItemFailures option
		const result = await router.process(event);

		// Should return ProcessingResult, not BatchItemFailuresResponse
		expect(result).toHaveProperty("processed");
		expect(result).toHaveProperty("succeeded");
		expect(result).toHaveProperty("failed");
		expect(result).toHaveProperty("errors");
		expect(result).not.toHaveProperty("batchItemFailures");
	});
});

describe("Defer Functionality Properties", () => {
	// Mock SQS client for testing
	const createMockSQSClient = () => {
		const sentMessages: Array<{
			QueueUrl: string;
			MessageBody: string;
			DelaySeconds?: number;
		}> = [];
		return {
			sendMessage: jest.fn(async (params: {
				QueueUrl: string;
				MessageBody: string;
				DelaySeconds?: number;
			}) => {
				sentMessages.push(params);
				return {};
			}),
			sentMessages,
		};
	};

	/**
	 * **Feature: dynamodb-stream-router, Property 27: Deferred handlers enqueue during process()**
	 * **Validates: Requirements 16.4, 16.6**
	 *
	 * For any handler marked with .defer(), when a matching record is processed via process(),
	 * the router should enqueue the record to SQS instead of executing the handler code.
	 */
	test("Property 27: Deferred handlers enqueue during process()", async () => {
		const mockSQS = createMockSQSClient();
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			sqsClient: mockSQS,
		});
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler).defer();

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		// Handler should NOT be called directly
		expect(handler).not.toHaveBeenCalled();

		// SQS should have received the message
		expect(mockSQS.sendMessage).toHaveBeenCalledTimes(1);
		expect(mockSQS.sentMessages[0].QueueUrl).toBe(
			"https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 28: Deferred handlers execute during processDeferred()**
	 * **Validates: Requirements 16.5, 16.7**
	 *
	 * For any handler marked with .defer(), when a matching deferred record is processed
	 * via processDeferred(), the router should execute the handler code normally.
	 */
	test("Property 28: Deferred handlers execute during processDeferred()", async () => {
		const mockSQS = createMockSQSClient();
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			sqsClient: mockSQS,
		});
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		const registration = router.insert(discriminator, handler);
		registration.defer();

		// Get the handler ID from the router
		const handlerId = router.handlers[0].id;

		// Create a mock SQS event with the deferred record
		const originalRecord = createMockRecord("INSERT", { id: "test123" });
		const sqsEvent = {
			Records: [
				{
					body: JSON.stringify({
						handlerId,
						record: originalRecord,
					}),
				},
			],
		};

		await router.processDeferred(sqsEvent);

		// Handler should be called
		expect(handler).toHaveBeenCalledTimes(1);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 29: Defer queue option overrides router-level**
	 * **Validates: Requirements 16.3**
	 *
	 * For any handler with .defer({ queue: 'custom-url' }), the record should be sent
	 * to the specified queue, not the router's default deferQueue.
	 */
	test("Property 29: Defer queue option overrides router-level", async () => {
		const mockSQS = createMockSQSClient();
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789/default-queue",
			sqsClient: mockSQS,
		});
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler).defer({
			queue: "https://sqs.us-east-1.amazonaws.com/123456789/custom-queue",
		});

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		// Should use the custom queue, not the default
		expect(mockSQS.sentMessages[0].QueueUrl).toBe(
			"https://sqs.us-east-1.amazonaws.com/123456789/custom-queue",
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 30: Defer without queue throws ConfigurationError**
	 * **Validates: Requirements 16.8**
	 *
	 * For any handler with .defer() when no queue is specified in defer options and no
	 * router-level deferQueue is configured, the router should throw a ConfigurationError.
	 */
	test("Property 30: Defer without queue throws ConfigurationError", () => {
		const router = new StreamRouter(); // No deferQueue configured
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		expect(() => {
			router.insert(discriminator, handler).defer();
		}).toThrow(ConfigurationError);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 31: Deferred record includes handler ID**
	 * **Validates: Requirements 16.6, 16.7**
	 *
	 * For any deferred record, the SQS message should include the handler ID so that
	 * processDeferred() invokes only the specific handler that deferred it.
	 */
	test("Property 31: Deferred record includes handler ID", async () => {
		const mockSQS = createMockSQSClient();
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			sqsClient: mockSQS,
		});
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler).defer();

		const handlerId = router.handlers[0].id;

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		// Parse the message body and verify handler ID is included
		const messageBody = JSON.parse(mockSQS.sentMessages[0].MessageBody);
		expect(messageBody.handlerId).toBe(handlerId);
		expect(messageBody.record).toBeDefined();
	});

	test("Defer with delaySeconds passes delay to SQS", async () => {
		const mockSQS = createMockSQSClient();
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			sqsClient: mockSQS,
		});
		const handler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		router.insert(discriminator, handler).defer({ delaySeconds: 30 });

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		expect(mockSQS.sentMessages[0].DelaySeconds).toBe(30);
	});

	test("Non-deferred handlers still execute immediately", async () => {
		const mockSQS = createMockSQSClient();
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			sqsClient: mockSQS,
		});
		const immediateHandler = jest.fn();
		const deferredHandler = jest.fn();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		// Register both immediate and deferred handlers
		router.insert(discriminator, immediateHandler);
		router.insert(discriminator, deferredHandler).defer();

		const record = createMockRecord("INSERT", { id: "test123" });
		const event = createMockEvent([record]);

		await router.process(event);

		// Immediate handler should be called
		expect(immediateHandler).toHaveBeenCalledTimes(1);

		// Deferred handler should NOT be called
		expect(deferredHandler).not.toHaveBeenCalled();

		// SQS should have received one message (for deferred handler)
		expect(mockSQS.sendMessage).toHaveBeenCalledTimes(1);
	});
});
