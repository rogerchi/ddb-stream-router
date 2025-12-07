/**
 * Tests for deferred processing with SQS
 */
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { mockClient } from "aws-sdk-client-mock";
import { createSQSClient } from "../src/sqs-adapter";
import { StreamRouter } from "../src/stream-router";
import type { DeferredRecordMessage } from "../src/types";
import { createStreamEvent, createStreamRecord } from "./test-utils";

describe("Deferred Processing with SQS", () => {
	const sqsMock = mockClient(SQSClient);

	beforeEach(() => {
		sqsMock.reset();
	});

	// Use the createSQSClient helper to wrap the AWS SDK client
	function createSqsClientAdapter(client: SQSClient) {
		return createSQSClient(client, SendMessageCommand);
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

			router.onInsert(isUser, handler).defer("deferred-handler-1");

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

			router.onModify(isAny, handler).defer("deferred-handler-2");

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

			router.onRemove(isAny, handler).defer("deferred-handler-3");

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
			router.onInsert(isAny, handler).defer("deferred-handler-24", {
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

			router
				.onInsert(isAny, handler)
				.defer("deferred-handler-25", { delaySeconds: 60 });

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

			router.onInsert(isUser, handler1).defer("deferred-handler-4");
			router.onInsert(hasName, handler2).defer("deferred-handler-5");

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

			router.onInsert(isUser, deferredHandler).defer("deferred-handler-6");
			router.onInsert(hasName, immediateHandler); // Not deferred

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
		test("processDeferred fails when handler ID does not exist", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			// Register a handler but use a different ID in the message
			const handler = jest.fn();
			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" && record !== null;

			router.onInsert(isUser, handler).defer("deferred-handler-7");

			const originalRecord = createStreamRecord(
				"INSERT",
				{ pk: "user#1", sk: "profile" },
				{ pk: "user#1", sk: "profile", name: "Test User" },
			);

			// Use a non-existent handler ID
			const sqsEvent = {
				Records: [
					{
						messageId: "msg-1",
						body: JSON.stringify({
							handlerId: "non_existent_handler_id",
							record: originalRecord,
						} as DeferredRecordMessage),
					},
				],
			};

			const result = await router.processDeferred(sqsEvent);

			expect(result.processed).toBe(1);
			expect(result.failed).toBe(1);
			expect(result.succeeded).toBe(0);
			expect(result.errors[0].error.message).toBe(
				"Deferred handler not found: non_existent_handler_id",
			);
			expect(handler).not.toHaveBeenCalled();
		});

		test("processDeferred reports missing handler in batchItemFailures", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });
			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			const handler = jest.fn();
			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" && record !== null;

			router.onInsert(isUser, handler).defer("deferred-handler-8");

			const originalRecord = createStreamRecord(
				"INSERT",
				{ pk: "user#1", sk: "profile" },
				{ pk: "user#1", sk: "profile" },
			);

			const sqsEvent = {
				Records: [
					{
						messageId: "msg-missing-handler",
						body: JSON.stringify({
							handlerId: "non_existent_handler_id",
							record: originalRecord,
						}),
					},
				],
			};

			const result = await router.processDeferred(sqsEvent, {
				reportBatchItemFailures: true,
			});

			expect(result.batchItemFailures).toHaveLength(1);
			expect(result.batchItemFailures[0].itemIdentifier).toBe(
				"msg-missing-handler",
			);
		});

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

			router.onInsert(isUser, handler).defer("deferred-handler-9");

			// Get the handler ID from the router
			const handlerId = router.handlers[0].deferOptions?.id;

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

			router.onInsert(isAny, handler).defer("deferred-handler-10");

			const handlerId = router.handlers[0].deferOptions?.id;

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

			router.onInsert(isAny, handler).defer("deferred-handler-11");

			const handlerId = router.handlers[0].deferOptions?.id;

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

			router.onInsert(isAny, handler).defer("deferred-handler-12");

			const handlerId = router.handlers[0].deferOptions?.id;

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

			router
				.onInsert(isUser, handler, { batch: true })
				.defer("deferred-handler-13");

			const handlerId = router.handlers[0].deferOptions?.id;

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
				.onInsert(isAudit, handler, { batch: true, batchKey: "userId" })
				.defer("deferred-handler-14");

			const handlerId = router.handlers[0].deferOptions?.id;

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

			router
				.onInsert(isUser, handler, { batch: true })
				.defer("deferred-handler-15");

			const handlerId = router.handlers[0].deferOptions?.id;

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

			router
				.onInsert(isUser, batchHandler, { batch: true })
				.defer("deferred-handler-16");
			router.onInsert(isOrder, nonBatchHandler).defer("deferred-handler-17");

			const userHandlerId = router.handlers[0].deferOptions?.id;
			const orderHandlerId = router.handlers[1].deferOptions?.id;

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

	describe("Explicit Handler IDs", () => {
		test("explicit handler IDs work across router instances", async () => {
			// This test simulates Lambda cold starts where a new router instance is created
			// Using the same explicit ID ensures the handler can be found

			const sqsClient = new SQSClient({ region: "us-east-1" });

			// Define handlers outside the router to simulate module-level definitions
			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("user#");

			const userHandler = async (
				newImage: { pk: string },
				_ctx: unknown,
			): Promise<void> => {
				console.log("Processing user:", newImage.pk);
			};

			// Use the same explicit ID in both router instances
			const sharedDeferredId = "user-insert-handler";

			// Create first router instance (simulating first Lambda invocation)
			const router1 = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});
			router1.onInsert(isUser, userHandler).defer(sharedDeferredId);
			const handlerId1 = router1.handlers[0].deferOptions?.id;

			// Create second router instance (simulating cold start)
			const router2 = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});
			router2.onInsert(isUser, userHandler).defer(sharedDeferredId);
			const handlerId2 = router2.handlers[0].deferOptions?.id;

			// Handler IDs should be identical (same explicit ID)
			expect(handlerId1).toBe(handlerId2);
			expect(handlerId1).toBe(sharedDeferredId);
		});

		test("different handlers get different explicit IDs", async () => {
			const sqsClient = new SQSClient({ region: "us-east-1" });

			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("user#");

			const isOrder = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("order#");

			const handler1 = async (
				newImage: { pk: string },
				_ctx: unknown,
			): Promise<void> => {
				console.log("Handler 1:", newImage.pk);
			};

			const handler2 = async (
				newImage: { pk: string },
				_ctx: unknown,
			): Promise<void> => {
				console.log("Handler 2:", newImage.pk);
			};

			const router = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});

			router.onInsert(isUser, handler1).defer("user-handler");
			router.onInsert(isOrder, handler2).defer("order-handler");

			const handlerId1 = router.handlers[0].deferOptions?.id;
			const handlerId2 = router.handlers[1].deferOptions?.id;

			// Handler IDs should be different
			expect(handlerId1).not.toBe(handlerId2);
			expect(handlerId1).toBe("user-handler");
			expect(handlerId2).toBe("order-handler");
		});

		test("processDeferred works with same explicit ID across router instances", async () => {
			sqsMock.on(SendMessageCommand).resolves({ MessageId: "test-message-id" });

			const sqsClient = new SQSClient({ region: "us-east-1" });

			// Define handlers outside the router
			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("user#");

			// Use the same explicit ID in both router instances
			const sharedDeferredId = "user-insert-handler";

			const userHandler = jest.fn();

			// First router instance - enqueues to SQS
			const streamRouter = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});
			streamRouter.onInsert(isUser, userHandler).defer(sharedDeferredId);

			const record = createStreamRecord(
				"INSERT",
				{ pk: "user#1", sk: "profile" },
				{ pk: "user#1", sk: "profile", name: "Test User" },
			);
			const streamEvent = createStreamEvent([record]);

			await streamRouter.process(streamEvent);

			// Get the handler ID from the SQS message
			const calls = sqsMock.commandCalls(SendMessageCommand);
			const messageBody: DeferredRecordMessage = JSON.parse(
				calls[0].args[0].input.MessageBody as string,
			);
			const enqueuedHandlerId = messageBody.handlerId;

			// Verify the enqueued ID matches our explicit ID
			expect(enqueuedHandlerId).toBe(sharedDeferredId);

			// Second router instance - processes from SQS (simulating cold start)
			const sqsRouter = new StreamRouter({
				deferQueue:
					"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: createSqsClientAdapter(sqsClient),
			});
			const sqsHandler = jest.fn();
			sqsRouter.onInsert(isUser, sqsHandler).defer(sharedDeferredId);

			// Verify the handler IDs match
			expect(sqsRouter.handlers[0].deferOptions?.id).toBe(enqueuedHandlerId);

			// Process the deferred record
			const sqsEvent = {
				Records: [
					{
						messageId: "msg-1",
						body: JSON.stringify(messageBody),
					},
				],
			};

			const result = await sqsRouter.processDeferred(sqsEvent);

			expect(result.processed).toBe(1);
			expect(result.succeeded).toBe(1);
			expect(sqsHandler).toHaveBeenCalledTimes(1);
		});
	});
});
