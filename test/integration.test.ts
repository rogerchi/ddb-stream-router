import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { mockClient } from "aws-sdk-client-mock";
import { z } from "zod";
import { StreamRouter } from "../src/stream-router";
import type { DeferredRecordMessage } from "../src/types";

const TABLE_NAME = "test-table";

/**
 * Helper to create a stream record from DynamoDB items.
 * This creates records in the exact format that DynamoDB streams produce.
 */
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
		eventID: `event_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
		eventName,
		eventVersion: "1.1",
		eventSource: "aws:dynamodb",
		awsRegion: "us-east-1",
		eventSourceARN: `arn:aws:dynamodb:us-east-1:123456789012:table/${TABLE_NAME}/stream/2024-01-01T00:00:00.000`,
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

describe("Integration Tests with DynamoDB Local", () => {
	describe("INSERT Events", () => {
		test("INSERT event processing with discriminator", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				typeof (record as { pk: unknown }).pk === "string" &&
				(record as { pk: string }).pk.startsWith("user#");

			router.insert(isUser, handler);

			// Create a stream record
			const newItem = { pk: "user#1", sk: "profile", name: "Test User" };
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
				expect.objectContaining({ pk: "user#1" }),
				expect.any(Object),
			);
		});

		test("INSERT event processing with parser (Zod-like)", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

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
					return { success: false as const, error: new Error("Invalid data") };
				},
			};

			router.insert(userParser, handler);

			const newItem = { pk: "user#1", sk: "profile", name: "Test User" };
			const record = createStreamRecord(
				"INSERT",
				{ pk: "user#1", sk: "profile" },
				newItem,
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			expect(handler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "user#1", name: "Test User" }),
				expect.any(Object),
			);
		});

		test("INSERT event processing with Zod schema", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const UserSchema = z.object({
				pk: z.string(),
				sk: z.string(),
				name: z.string(),
				email: z.string().email(),
			});

			router.insert(UserSchema, handler);

			const newItem = {
				pk: "user#1",
				sk: "profile",
				name: "Test User",
				email: "test@example.com",
			};
			const record = createStreamRecord(
				"INSERT",
				{ pk: "user#1", sk: "profile" },
				newItem,
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			expect(handler).toHaveBeenCalledWith(
				expect.objectContaining({
					pk: "user#1",
					name: "Test User",
					email: "test@example.com",
				}),
				expect.any(Object),
			);
		});

		test("Zod schema validation rejects invalid records", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const UserSchema = z.object({
				pk: z.string(),
				sk: z.string(),
				name: z.string(),
				email: z.string().email(), // Requires valid email
			});

			router.insert(UserSchema, handler);

			// Invalid email format
			const newItem = {
				pk: "user#1",
				sk: "profile",
				name: "Test User",
				email: "not-an-email",
			};
			const record = createStreamRecord(
				"INSERT",
				{ pk: "user#1", sk: "profile" },
				newItem,
			);
			const event = createStreamEvent([record]);

			const result = await router.process(event);

			// Handler should NOT be called because validation failed
			expect(handler).not.toHaveBeenCalled();
			// Record should still be processed successfully (just skipped)
			expect(result.succeeded).toBe(1);
			expect(result.failed).toBe(0);
		});

		test("Zod schema with MODIFY event", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const OrderSchema = z.object({
				pk: z.string(),
				sk: z.string(),
				orderId: z.string(),
				status: z.enum(["pending", "shipped", "delivered"]),
				total: z.number(),
			});

			router.modify(OrderSchema, handler);

			const oldItem = {
				pk: "order#1",
				sk: "details",
				orderId: "ORD-123",
				status: "pending",
				total: 99,
			};
			const newItem = {
				pk: "order#1",
				sk: "details",
				orderId: "ORD-123",
				status: "shipped",
				total: 99,
			};
			const record = createStreamRecord(
				"MODIFY",
				{ pk: "order#1", sk: "details" },
				newItem,
				oldItem,
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const [oldImage, newImage] = handler.mock.calls[0];
			expect(oldImage.status).toBe("pending");
			expect(newImage.status).toBe("shipped");
		});

		test("Zod schema with REMOVE event", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const ProductSchema = z.object({
				pk: z.string(),
				sk: z.string(),
				productId: z.string(),
				name: z.string(),
				price: z.number().positive(),
			});

			router.remove(ProductSchema, handler);

			const oldItem = {
				pk: "product#1",
				sk: "info",
				productId: "PROD-456",
				name: "Widget",
				price: 29,
			};
			const record = createStreamRecord(
				"REMOVE",
				{ pk: "product#1", sk: "info" },
				undefined,
				oldItem,
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			expect(handler).toHaveBeenCalledWith(
				expect.objectContaining({
					productId: "PROD-456",
					name: "Widget",
					price: 29,
				}),
				expect.any(Object),
			);
		});

		test("Multiple Zod schemas matching different entity types", async () => {
			const router = new StreamRouter();
			const userHandler = jest.fn();
			const orderHandler = jest.fn();

			const UserSchema = z.object({
				pk: z.string().startsWith("USER#"),
				sk: z.string(),
				name: z.string(),
			});

			const OrderSchema = z.object({
				pk: z.string().startsWith("ORDER#"),
				sk: z.string(),
				orderId: z.string(),
			});

			router.insert(UserSchema, userHandler);
			router.insert(OrderSchema, orderHandler);

			// User record
			const userRecord = createStreamRecord(
				"INSERT",
				{ pk: "USER#1", sk: "profile" },
				{ pk: "USER#1", sk: "profile", name: "John" },
			);

			// Order record
			const orderRecord = createStreamRecord(
				"INSERT",
				{ pk: "ORDER#1", sk: "details" },
				{ pk: "ORDER#1", sk: "details", orderId: "ORD-789" },
			);

			const event = createStreamEvent([userRecord, orderRecord]);

			await router.process(event);

			expect(userHandler).toHaveBeenCalledTimes(1);
			expect(userHandler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "USER#1", name: "John" }),
				expect.any(Object),
			);

			expect(orderHandler).toHaveBeenCalledTimes(1);
			expect(orderHandler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "ORDER#1", orderId: "ORD-789" }),
				expect.any(Object),
			);
		});
	});

	describe("MODIFY Events", () => {
		test("MODIFY event processing with attribute filter", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const isUser = (
				record: unknown,
			): record is { pk: string; sk: string; name: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("user#");

			router.modify(isUser, handler, {
				attribute: "name",
				changeType: "changed_attribute",
			});

			const oldItem = { pk: "user#1", sk: "profile", name: "Old Name" };
			const newItem = { pk: "user#1", sk: "profile", name: "New Name" };
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
				expect.objectContaining({ name: "Old Name" }),
				expect.objectContaining({ name: "New Name" }),
				expect.any(Object),
			);
		});

		test("MODIFY event with oldImage and newImage", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const isAny = (record: unknown): record is Record<string, unknown> =>
				typeof record === "object" && record !== null;

			router.modify(isAny, handler);

			const oldItem = { pk: "item#1", sk: "data", value: 10 };
			const newItem = { pk: "item#1", sk: "data", value: 20 };
			const record = createStreamRecord(
				"MODIFY",
				{ pk: "item#1", sk: "data" },
				newItem,
				oldItem,
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const [oldImage, newImage] = handler.mock.calls[0];
			expect(oldImage).toHaveProperty("value", 10);
			expect(newImage).toHaveProperty("value", 20);
		});
	});

	describe("REMOVE Events", () => {
		test("REMOVE event processing with discriminator", async () => {
			const router = new StreamRouter();
			const handler = jest.fn();

			const isUser = (
				record: unknown,
			): record is { pk: string; name: string } =>
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

	describe("Stream View Types", () => {
		test("KEYS_ONLY stream view type", async () => {
			const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
			const handler = jest.fn();

			const isAny = (_record: unknown): _record is Record<string, unknown> =>
				true;

			router.insert(isAny, handler);

			const record = createStreamRecord(
				"INSERT",
				{ pk: "item#1", sk: "data" },
				{ pk: "item#1", sk: "data" },
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const [keys] = handler.mock.calls[0];
			expect(keys).toHaveProperty("pk", "item#1");
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
				{ pk: "item#1", sk: "data", name: "Test" },
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const [newImage] = handler.mock.calls[0];
			expect(newImage).toHaveProperty("name", "Test");
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
				{ pk: "item#1", sk: "data", name: "Deleted" },
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const [oldImage] = handler.mock.calls[0];
			expect(oldImage).toHaveProperty("name", "Deleted");
		});

		test("NEW_AND_OLD_IMAGES stream view type for MODIFY", async () => {
			const router = new StreamRouter({
				streamViewType: "NEW_AND_OLD_IMAGES",
			});
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

	describe("Multiple Handlers", () => {
		test("Multiple handlers matching same record", async () => {
			const router = new StreamRouter();
			const handler1 = jest.fn();
			const handler2 = jest.fn();

			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("user#");

			const isAdmin = (
				record: unknown,
			): record is { pk: string; role: string } =>
				typeof record === "object" &&
				record !== null &&
				"role" in record &&
				(record as { role: string }).role === "admin";

			router.insert(isUser, handler1);
			router.insert(isAdmin, handler2);

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
});

describe("Deferred Handler Tests with SQS Mock", () => {
	const sqsMock = mockClient(SQSClient);

	beforeEach(() => {
		sqsMock.reset();
	});

	// Create an SQS client adapter that wraps the AWS SDK client
	function createSqsClientAdapter(client: SQSClient) {
		return {
			sendMessage: async (params: {
				QueueUrl: string;
				MessageBody: string;
				DelaySeconds?: number;
			}) => {
				return client.send(
					new SendMessageCommand({
						QueueUrl: params.QueueUrl,
						MessageBody: params.MessageBody,
						DelaySeconds: params.DelaySeconds,
					}),
				);
			},
		};
	}

	test("deferred INSERT handler enqueues message to SQS", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isUser = (record: unknown): record is { pk: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		router.insert(isUser, handler).defer();

		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			{ pk: "user#1", sk: "profile", name: "Test User" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		// Handler should NOT be called directly
		expect(handler).not.toHaveBeenCalled();

		// SQS should have received the message
		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(1);

		const sentCommand = calls[0].args[0].input;
		expect(sentCommand.QueueUrl).toBe(
			"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
		);

		// Verify message body contains the record and handler ID
		const messageBody: DeferredRecordMessage = JSON.parse(
			sentCommand.MessageBody as string,
		);
		expect(messageBody).toHaveProperty("handlerId");
		expect(messageBody).toHaveProperty("record");
		expect(messageBody.record).toHaveProperty("eventName", "INSERT");
	});

	test("deferred MODIFY handler enqueues message with correct data", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.modify(isAny, handler).defer();

		const record = createStreamRecord(
			"MODIFY",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data", value: 20 },
			{ pk: "item#1", sk: "data", value: 10 },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).not.toHaveBeenCalled();

		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(1);

		const messageBody: DeferredRecordMessage = JSON.parse(
			calls[0].args[0].input.MessageBody as string,
		);
		expect(messageBody.record).toHaveProperty("eventName", "MODIFY");
	});

	test("deferred REMOVE handler enqueues message", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.remove(isAny, handler).defer();

		const record = createStreamRecord(
			"REMOVE",
			{ pk: "item#1", sk: "data" },
			undefined,
			{ pk: "item#1", sk: "data", name: "Deleted" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).not.toHaveBeenCalled();

		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(1);

		const messageBody: DeferredRecordMessage = JSON.parse(
			calls[0].args[0].input.MessageBody as string,
		);
		expect(messageBody.record).toHaveProperty("eventName", "REMOVE");
	});

	test("deferred handler with custom queue URL", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue:
				"https://sqs.us-east-1.amazonaws.com/123456789012/default-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		// Override with custom queue
		router.insert(isAny, handler).defer({
			queue: "https://sqs.us-east-1.amazonaws.com/123456789012/custom-queue",
		});

		const record = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(1);
		expect(calls[0].args[0].input.QueueUrl).toBe(
			"https://sqs.us-east-1.amazonaws.com/123456789012/custom-queue",
		);
	});

	test("deferred handler with delay seconds", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.insert(isAny, handler).defer({ delaySeconds: 60 });

		const record = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(1);
		expect(calls[0].args[0].input.DelaySeconds).toBe(60);
	});

	test("multiple deferred handlers enqueue multiple messages", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler1 = jest.fn();
		const handler2 = jest.fn();

		const isUser = (record: unknown): record is { pk: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		const hasName = (record: unknown): record is { name: string } =>
			typeof record === "object" && record !== null && "name" in record;

		router.insert(isUser, handler1).defer();
		router.insert(hasName, handler2).defer();

		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			{ pk: "user#1", sk: "profile", name: "Test User" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		// Both handlers should NOT be called directly
		expect(handler1).not.toHaveBeenCalled();
		expect(handler2).not.toHaveBeenCalled();

		// SQS should have received two messages (one per handler)
		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(2);

		// Verify each message has a different handler ID
		const message1: DeferredRecordMessage = JSON.parse(
			calls[0].args[0].input.MessageBody as string,
		);
		const message2: DeferredRecordMessage = JSON.parse(
			calls[1].args[0].input.MessageBody as string,
		);
		expect(message1.handlerId).not.toBe(message2.handlerId);
	});

	test("mixed deferred and immediate handlers", async () => {
		sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const deferredHandler = jest.fn();
		const immediateHandler = jest.fn();

		const isUser = (record: unknown): record is { pk: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		const hasName = (record: unknown): record is { name: string } =>
			typeof record === "object" && record !== null && "name" in record;

		router.insert(isUser, deferredHandler).defer();
		router.insert(hasName, immediateHandler); // Not deferred

		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			{ pk: "user#1", sk: "profile", name: "Test User" },
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		// Deferred handler should NOT be called
		expect(deferredHandler).not.toHaveBeenCalled();

		// Immediate handler SHOULD be called
		expect(immediateHandler).toHaveBeenCalledTimes(1);

		// Only one SQS message (for deferred handler)
		const calls = sqsMock.commandCalls(SendMessageCommand);
		expect(calls).toHaveLength(1);
	});

	test("processDeferred executes handler from SQS message", async () => {
		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isUser = (record: unknown): record is { pk: string } =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			(record as { pk: string }).pk.startsWith("user#");

		router.insert(isUser, handler).defer();

		// Get the handler ID from the router
		const handlerId = router.handlers[0].id;

		// Create a mock SQS event with the deferred record
		const originalRecord = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			{ pk: "user#1", sk: "profile", name: "Test User" },
		);

		const sqsEvent = {
			Records: [
				{
					body: JSON.stringify({
						handlerId,
						record: originalRecord,
					} as DeferredRecordMessage),
				},
			],
		};

		const result = await router.processDeferred(sqsEvent);

		expect(result.processed).toBe(1);
		expect(result.succeeded).toBe(1);
		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({ pk: "user#1" }),
			expect.any(Object),
		);
	});

	test("processDeferred returns all failed message IDs in batchItemFailures", async () => {
		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn().mockImplementation(() => {
			throw new Error("Handler failed");
		});
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.insert(isAny, handler).defer();

		const handlerId = router.handlers[0].id;

		const record1 = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data" },
		);
		const record2 = createStreamRecord(
			"INSERT",
			{ pk: "item#2", sk: "data" },
			{ pk: "item#2", sk: "data" },
		);

		const sqsEvent = {
			Records: [
				{
					messageId: "msg-1",
					body: JSON.stringify({ handlerId, record: record1 }),
				},
				{
					messageId: "msg-2",
					body: JSON.stringify({ handlerId, record: record2 }),
				},
			],
		};

		const result = await router.processDeferred(sqsEvent, {
			reportBatchItemFailures: true,
		});

		// Should return ALL failed message IDs (not just the first one)
		expect(result.batchItemFailures).toHaveLength(2);
		expect(result.batchItemFailures).toContainEqual({
			itemIdentifier: "msg-1",
		});
		expect(result.batchItemFailures).toContainEqual({
			itemIdentifier: "msg-2",
		});
	});

	test("processDeferred returns empty batchItemFailures on success", async () => {
		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		const handler = jest.fn();
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.insert(isAny, handler).defer();

		const handlerId = router.handlers[0].id;

		const record = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data" },
		);

		const sqsEvent = {
			Records: [
				{
					messageId: "msg-1",
					body: JSON.stringify({ handlerId, record }),
				},
			],
		};

		const result = await router.processDeferred(sqsEvent, {
			reportBatchItemFailures: true,
		});

		expect(result.batchItemFailures).toHaveLength(0);
	});

	test("processDeferred returns partial failures correctly", async () => {
		const sqsClient = new SQSClient({ region: "us-east-1" });
		const router = new StreamRouter({
			deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			sqsClient: createSqsClientAdapter(sqsClient),
		});

		let callCount = 0;
		const handler = jest.fn().mockImplementation(() => {
			callCount++;
			// Fail on second call only
			if (callCount === 2) {
				throw new Error("Handler failed");
			}
		});
		const isAny = (record: unknown): record is Record<string, unknown> =>
			typeof record === "object" && record !== null;

		router.insert(isAny, handler).defer();

		const handlerId = router.handlers[0].id;

		const record1 = createStreamRecord(
			"INSERT",
			{ pk: "item#1", sk: "data" },
			{ pk: "item#1", sk: "data" },
		);
		const record2 = createStreamRecord(
			"INSERT",
			{ pk: "item#2", sk: "data" },
			{ pk: "item#2", sk: "data" },
		);
		const record3 = createStreamRecord(
			"INSERT",
			{ pk: "item#3", sk: "data" },
			{ pk: "item#3", sk: "data" },
		);

		const sqsEvent = {
			Records: [
				{
					messageId: "msg-1",
					body: JSON.stringify({ handlerId, record: record1 }),
				},
				{
					messageId: "msg-2",
					body: JSON.stringify({ handlerId, record: record2 }),
				},
				{
					messageId: "msg-3",
					body: JSON.stringify({ handlerId, record: record3 }),
				},
			],
		};

		const result = await router.processDeferred(sqsEvent, {
			reportBatchItemFailures: true,
		});

		// Only the second message should fail
		expect(result.batchItemFailures).toHaveLength(1);
		expect(result.batchItemFailures[0].itemIdentifier).toBe("msg-2");
	});
});
