/**
 * Integration tests for StreamRouter with DynamoDB Local.
 *
 * These tests verify end-to-end stream event processing using
 * simulated DynamoDB stream records.
 *
 * Note: These tests simulate stream events rather than using actual
 * DynamoDB Local streams, as DynamoDB Local doesn't emit stream events.
 */

import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { StreamRouter } from "../src/stream-router";

const TABLE_NAME = "test-table";

// Helper to create a simulated stream record from a DynamoDB operation
function createStreamRecord(
	eventName: "INSERT" | "MODIFY" | "REMOVE",
	keys: Record<string, unknown>,
	newImage?: Record<string, unknown>,
	oldImage?: Record<string, unknown>,
): DynamoDBRecord {
	const toAttributeValue = (
		obj: Record<string, unknown> | undefined,
	): Record<string, { S?: string; N?: string; BOOL?: boolean }> | undefined => {
		if (!obj) return undefined;
		const result: Record<string, { S?: string; N?: string; BOOL?: boolean }> =
			{};
		for (const [key, value] of Object.entries(obj)) {
			if (typeof value === "string") {
				result[key] = { S: value };
			} else if (typeof value === "number") {
				result[key] = { N: String(value) };
			} else if (typeof value === "boolean") {
				result[key] = { BOOL: value };
			}
		}
		return result;
	};

	return {
		eventID: `event_${Date.now()}_${Math.random().toString(36).substring(2)}`,
		eventName,
		eventSourceARN: `arn:aws:dynamodb:us-east-1:123456789:table/${TABLE_NAME}/stream/2024`,
		dynamodb: {
			Keys: toAttributeValue(keys) as Record<
				string,
				{ S?: string; N?: string }
			>,
			NewImage: toAttributeValue(newImage) as
				| Record<string, { S?: string; N?: string }>
				| undefined,
			OldImage: toAttributeValue(oldImage) as
				| Record<string, { S?: string; N?: string }>
				| undefined,
			SequenceNumber: `seq_${Date.now()}`,
			StreamViewType: "NEW_AND_OLD_IMAGES",
		},
	} as DynamoDBRecord;
}

function createStreamEvent(records: DynamoDBRecord[]): DynamoDBStreamEvent {
	return { Records: records };
}

describe("Integration Tests - INSERT Events", () => {
	/**
	 * **Validates: Requirements 17.6**
	 * Test INSERT event processing with discriminators and parsers.
	 */
	test("INSERT event processing with discriminator", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const isUser = (
			record: unknown,
		): record is { pk: string; sk: string; name: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			typeof (record as { pk: unknown }).pk === "string" &&
			(record as { pk: string }).pk.startsWith("user#");

		router.insert(isUser, handler);

		// Simulate an INSERT event
		const newItem = { pk: "user#1", sk: "profile", name: "John Doe" };
		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			newItem,
		);
		const event = createStreamEvent([record]);

		const result = await router.process(event);

		expect(result).toHaveProperty("processed", 1);
		expect(result).toHaveProperty("succeeded", 1);
		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ pk: "user#1", name: "John Doe" }),
			expect.any(Object),
		);
	});

	test("INSERT event processing with parser (Zod-like)", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		// Mock Zod-like parser
		const userParser = {
			parse: (data: unknown) =>
				data as { pk: string; sk: string; name: string },
			safeParse: (data: unknown) => {
				if (
					typeof data === "object" &&
					data !== null &&
					"pk" in data &&
					"name" in data
				) {
					return {
						success: true as const,
						data: data as { pk: string; sk: string; name: string },
					};
				}
				return { success: false as const, error: new Error("Invalid") };
			},
		};

		router.insert(userParser, handler);

		const newItem = { pk: "user#1", sk: "profile", name: "Jane Doe" };
		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			newItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ name: "Jane Doe" }),
			expect.any(Object),
		);
	});
});

describe("Integration Tests - MODIFY Events", () => {
	/**
	 * **Validates: Requirements 17.6**
	 * Test MODIFY event processing with attribute filters.
	 */
	test("MODIFY event processing with attribute filter", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const isUser = (record: unknown): record is { pk: string; name: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		router.modify(isUser, handler, {
			attribute: "name",
			changeType: "changed_attribute",
		});

		const oldItem = { pk: "user#1", sk: "profile", name: "John" };
		const newItem = { pk: "user#1", sk: "profile", name: "John Updated" };
		const record = createStreamRecord(
			"MODIFY",
			{ pk: "user#1", sk: "profile" },
			newItem,
			oldItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ name: "John" }),
			expect.objectContaining({ name: "John Updated" }),
			expect.any(Object),
		);
	});

	test("MODIFY event with oldImage and newImage handling", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.modify(isAny, handler);

		const oldItem = { pk: "item#1", sk: "data", value: 100 };
		const newItem = { pk: "item#1", sk: "data", value: 200 };
		const record = createStreamRecord(
			"MODIFY",
			{ pk: "item#1", sk: "data" },
			newItem,
			oldItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		// Verify both old and new images are passed
		const [oldImage, newImage] = handler.mock.calls[0];
		expect(oldImage).toHaveProperty("value", 100);
		expect(newImage).toHaveProperty("value", 200);
	});
});

describe("Integration Tests - REMOVE Events", () => {
	/**
	 * **Validates: Requirements 17.6**
	 * Test REMOVE event processing with discriminators and parsers.
	 */
	test("REMOVE event processing with discriminator", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const isUser = (record: unknown): record is { pk: string; name: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		router.remove(isUser, handler);

		const oldItem = { pk: "user#1", sk: "profile", name: "Deleted User" };
		const record = createStreamRecord(
			"REMOVE",
			{ pk: "user#1", sk: "profile" },
			undefined,
			oldItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ name: "Deleted User" }),
			expect.any(Object),
		);
	});
});

describe("Integration Tests - Stream View Types", () => {
	/**
	 * **Validates: Requirements 17.7**
	 * Test all four stream view type configurations.
	 */
	test("KEYS_ONLY stream view type", async () => {
		const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
		const handler = jest.fn();

		const isAny = (_record: unknown): _record is Record<string, unknown> => true;

		router.insert(isAny, handler);

		const record = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data", name: "Test" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		// With KEYS_ONLY, handler receives keys
		const [keys] = handler.mock.calls[0];
		expect(keys).toHaveProperty("pk", "item#1");
		expect(keys).toHaveProperty("sk", "data");
	});

	test("NEW_IMAGE stream view type", async () => {
		const router = new StreamRouter({ streamViewType: "NEW_IMAGE" });
		const handler = jest.fn();

		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.insert(isAny, handler);

		const record = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data", name: "New Item" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const [newImage] = handler.mock.calls[0];
		expect(newImage).toHaveProperty("name", "New Item");
	});

	test("OLD_IMAGE stream view type for REMOVE", async () => {
		const router = new StreamRouter({ streamViewType: "OLD_IMAGE" });
		const handler = jest.fn();

		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.remove(isAny, handler);

		const record = createStreamRecord(
			"REMOVE",
			{ pk: "item#1", sk: "data" },
			undefined,
			{ pk: "item#1", sk: "data", name: "Old Item" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const [oldImage] = handler.mock.calls[0];
		expect(oldImage).toHaveProperty("name", "Old Item");
	});

	test("NEW_AND_OLD_IMAGES stream view type for MODIFY", async () => {
		const router = new StreamRouter({ streamViewType: "NEW_AND_OLD_IMAGES" });
		const handler = jest.fn();

		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.modify(isAny, handler);

		const record = createStreamRecord(
			"MODIFY",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data", name: "New Name" },
			{ pk: "item#1", sk: "data", name: "Old Name" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const [oldImage, newImage] = handler.mock.calls[0];
		expect(oldImage).toHaveProperty("name", "Old Name");
		expect(newImage).toHaveProperty("name", "New Name");
	});
});

describe("Integration Tests - Multiple Handlers", () => {
	test("Multiple handlers for same event type", async () => {
		const router = new StreamRouter();
		const handler1 = jest.fn();
		const handler2 = jest.fn();

		const isUser = (record: unknown): record is { pk: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		const isAdmin = (record: unknown): record is { pk: string; role: string } =>
			typeof record === "object" &&
			record !== null &&
			"role" in record &&
			(record as { role: string }).role === "admin";

		router.insert(isUser, handler1);
		router.insert(isAdmin, handler2);

		// This record matches both handlers
		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			{ pk: "user#1", sk: "profile", role: "admin" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler1).toHaveBeenCalledTimes(1);
		expect(handler2).toHaveBeenCalledTimes(1);
	});

	test("Mixed event types in single batch", async () => {
		const router = new StreamRouter();
		const insertHandler = jest.fn();
		const modifyHandler = jest.fn();
		const removeHandler = jest.fn();

		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.insert(isAny, insertHandler);
		router.modify(isAny, modifyHandler);
		router.remove(isAny, removeHandler);

		const records = [
			createStreamRecord(
				"INSERT",
				{ pk: "item#1", sk: "data" },
				{ pk: "item#1", sk: "data" },
			),
			createStreamRecord(
				"MODIFY",
				{ pk: "item#2", sk: "data" },
				{ pk: "item#2", sk: "data", v: 2 },
				{ pk: "item#2", sk: "data", v: 1 },
			),
			createStreamRecord("REMOVE", { pk: "item#3", sk: "data" }, undefined, {
				pk: "item#3",
				sk: "data",
			}),
		];
		const event = createStreamEvent(records);

		const result = await router.process(event);

		expect(result).toHaveProperty("processed", 3);
		expect(result).toHaveProperty("succeeded", 3);
		expect(insertHandler).toHaveBeenCalledTimes(1);
		expect(modifyHandler).toHaveBeenCalledTimes(1);
		expect(removeHandler).toHaveBeenCalledTimes(1);
	});
});
