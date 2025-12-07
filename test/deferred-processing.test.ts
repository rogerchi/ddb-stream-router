/**
 * Tests for deferred processing with SQS
 */
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { mockClient } from "aws-sdk-client-mock";
import { StreamRouter } from "../src/stream-router";
import type { DeferredRecordMessage } from "../src/types";
import { createStreamEvent, createStreamRecord } from "./test-utils";

describe("Deferred Processing with SQS", () => {
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

	describe("Enqueueing to SQS", () => {
		test("deferred INSERT handler enqueues message to SQS", async () => {
			sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
	});

	describe("Processing from SQS", () => {
		test("processDeferred executes handler from SQS message", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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

	describe("Deferred Batch Processing", () => {
		test("processDeferred collects records for batch handlers", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			const handler = jest.fn();
			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("USER#");

			router.insert(isUser, handler, { batch: true }).defer();

			const handlerId = router.handlers[0].id;

			const record1 = createStreamRecord(
				"INSERT",
				{ pk: "USER#1", sk: "profile" },
				{ pk: "USER#1", sk: "profile", name: "Alice" },
			);
			const record2 = createStreamRecord(
				"INSERT",
				{ pk: "USER#2", sk: "profile" },
				{ pk: "USER#2", sk: "profile", name: "Bob" },
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

			const result = await router.processDeferred(sqsEvent);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			// Handler should be called once with both records
			expect(handler).toHaveBeenCalledTimes(1);
			const batchRecords = handler.mock.calls[0][0];
			expect(batchRecords).toHaveLength(2);
			expect(batchRecords[0].newImage).toHaveProperty("name", "Alice");
			expect(batchRecords[1].newImage).toHaveProperty("name", "Bob");
		});

		test("processDeferred groups batch records by batchKey", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			const handler = jest.fn();
			const isAudit = (
				record: unknown,
			): record is { pk: string; userId: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("AUDIT#");

			router
				.insert(isAudit, handler, { batch: true, batchKey: "userId" })
				.defer();

			const handlerId = router.handlers[0].id;

			const record1 = createStreamRecord(
				"INSERT",
				{ pk: "AUDIT#1", sk: "log" },
				{ pk: "AUDIT#1", sk: "log", userId: "user-a", action: "login" },
			);
			const record2 = createStreamRecord(
				"INSERT",
				{ pk: "AUDIT#2", sk: "log" },
				{ pk: "AUDIT#2", sk: "log", userId: "user-b", action: "view" },
			);
			const record3 = createStreamRecord(
				"INSERT",
				{ pk: "AUDIT#3", sk: "log" },
				{ pk: "AUDIT#3", sk: "log", userId: "user-a", action: "logout" },
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

			const result = await router.processDeferred(sqsEvent);

			expect(result.processed).toBe(3);
			expect(result.succeeded).toBe(3);
			// Handler should be called twice: once for user-a (2 records), once for user-b (1 record)
			expect(handler).toHaveBeenCalledTimes(2);

			const calls = handler.mock.calls.map((call) => call[0]);
			const userABatch = calls.find((batch) => batch.length === 2);
			const userBBatch = calls.find((batch) => batch.length === 1);

			expect(userABatch).toBeDefined();
			expect(userBBatch).toBeDefined();
		});

		test("processDeferred reports all message IDs on batch failure", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			const handler = jest.fn().mockImplementation(() => {
				throw new Error("Batch handler failed");
			});
			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" && record !== null && "pk" in record;

			router.insert(isUser, handler, { batch: true }).defer();

			const handlerId = router.handlers[0].id;

			const record1 = createStreamRecord(
				"INSERT",
				{ pk: "USER#1", sk: "profile" },
				{ pk: "USER#1", sk: "profile" },
			);
			const record2 = createStreamRecord(
				"INSERT",
				{ pk: "USER#2", sk: "profile" },
				{ pk: "USER#2", sk: "profile" },
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

			// Both messages should be reported as failed since they're in the same batch
			expect(result.batchItemFailures).toHaveLength(2);
			expect(result.batchItemFailures).toContainEqual({
				itemIdentifier: "msg-1",
			});
			expect(result.batchItemFailures).toContainEqual({
				itemIdentifier: "msg-2",
			});
		});

		test("processDeferred handles mixed batch and non-batch handlers", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			const batchHandler = jest.fn();
			const nonBatchHandler = jest.fn();

			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("USER#");

			const isOrder = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("ORDER#");

			router.insert(isUser, batchHandler, { batch: true }).defer();
			router.insert(isOrder, nonBatchHandler).defer();

			const userHandlerId = router.handlers[0].id;
			const orderHandlerId = router.handlers[1].id;

			const userRecord1 = createStreamRecord(
				"INSERT",
				{ pk: "USER#1", sk: "profile" },
				{ pk: "USER#1", sk: "profile" },
			);
			const userRecord2 = createStreamRecord(
				"INSERT",
				{ pk: "USER#2", sk: "profile" },
				{ pk: "USER#2", sk: "profile" },
			);
			const orderRecord = createStreamRecord(
				"INSERT",
				{ pk: "ORDER#1", sk: "details" },
				{ pk: "ORDER#1", sk: "details" },
			);

			const sqsEvent = {
				Records: [
					{
						messageId: "msg-1",
						body: JSON.stringify({
							handlerId: userHandlerId,
							record: userRecord1,
						}),
					},
					{
						messageId: "msg-2",
						body: JSON.stringify({
							handlerId: orderHandlerId,
							record: orderRecord,
						}),
					},
					{
						messageId: "msg-3",
						body: JSON.stringify({
							handlerId: userHandlerId,
							record: userRecord2,
						}),
					},
				],
			};

			const result = await router.processDeferred(sqsEvent);

			expect(result.processed).toBe(3);
			expect(result.succeeded).toBe(3);

			// Batch handler called once with 2 records
			expect(batchHandler).toHaveBeenCalledTimes(1);
			expect(batchHandler.mock.calls[0][0]).toHaveLength(2);

			// Non-batch handler called once (immediately)
			expect(nonBatchHandler).toHaveBeenCalledTimes(1);
		});
	});
});
