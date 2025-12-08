/**
 * Integration tests for batch processing
 */
import { describe, expect, test, vi } from "vitest";

import { StreamRouter } from "../src/stream-router";
import { createStreamEvent, createStreamRecord } from "./test-utils";

describe("Batch Processing", () => {
	describe("Basic Batch Mode", () => {
		test("batch handler receives array of all matching INSERT records", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isUser = (
				record: unknown,
			): record is { pk: string; name: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("USER#");

			router.onInsert(isUser, handler, { batch: true });

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "USER#1", sk: "profile" },
					{ pk: "USER#1", sk: "profile", name: "Alice" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "USER#2", sk: "profile" },
					{ pk: "USER#2", sk: "profile", name: "Bob" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "USER#3", sk: "profile" },
					{ pk: "USER#3", sk: "profile", name: "Charlie" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const batchRecords = handler.mock.calls[0][0];
			expect(batchRecords).toHaveLength(3);
			expect(batchRecords[0].newImage).toHaveProperty("name", "Alice");
			expect(batchRecords[1].newImage).toHaveProperty("name", "Bob");
			expect(batchRecords[2].newImage).toHaveProperty("name", "Charlie");
		});

		test("batch handler receives array of all matching MODIFY records with oldImage and newImage", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isOrder = (
				record: unknown,
			): record is { pk: string; status: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("ORDER#");

			router.onModify(isOrder, handler, { batch: true });

			const records = [
				createStreamRecord(
					"MODIFY",
					{ pk: "ORDER#1", sk: "details" },
					{ pk: "ORDER#1", sk: "details", status: "shipped" },
					{ pk: "ORDER#1", sk: "details", status: "pending" },
				),
				createStreamRecord(
					"MODIFY",
					{ pk: "ORDER#2", sk: "details" },
					{ pk: "ORDER#2", sk: "details", status: "delivered" },
					{ pk: "ORDER#2", sk: "details", status: "shipped" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const batchRecords = handler.mock.calls[0][0];
			expect(batchRecords).toHaveLength(2);

			// First record
			expect(batchRecords[0].oldImage).toHaveProperty("status", "pending");
			expect(batchRecords[0].newImage).toHaveProperty("status", "shipped");

			// Second record
			expect(batchRecords[1].oldImage).toHaveProperty("status", "shipped");
			expect(batchRecords[1].newImage).toHaveProperty("status", "delivered");
		});

		test("batch handler receives array of all matching REMOVE records with oldImage", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isProduct = (
				record: unknown,
			): record is { pk: string; name: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("PRODUCT#");

			router.onRemove(isProduct, handler, { batch: true });

			const records = [
				createStreamRecord(
					"REMOVE",
					{ pk: "PRODUCT#1", sk: "info" },
					undefined,
					{ pk: "PRODUCT#1", sk: "info", name: "Widget" },
				),
				createStreamRecord(
					"REMOVE",
					{ pk: "PRODUCT#2", sk: "info" },
					undefined,
					{ pk: "PRODUCT#2", sk: "info", name: "Gadget" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const batchRecords = handler.mock.calls[0][0];
			expect(batchRecords).toHaveLength(2);
			expect(batchRecords[0].oldImage).toHaveProperty("name", "Widget");
			expect(batchRecords[1].oldImage).toHaveProperty("name", "Gadget");
		});
	});

	describe("Batch Key Grouping", () => {
		test("batchKey string groups records by attribute value", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isAuditLog = (
				record: unknown,
			): record is { pk: string; userId: string; action: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("AUDIT#");

			router.onInsert(isAuditLog, handler, { batch: true, batchKey: "userId" });

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "AUDIT#1", sk: "log" },
					{ pk: "AUDIT#1", sk: "log", userId: "user-a", action: "login" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "AUDIT#2", sk: "log" },
					{ pk: "AUDIT#2", sk: "log", userId: "user-b", action: "view" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "AUDIT#3", sk: "log" },
					{ pk: "AUDIT#3", sk: "log", userId: "user-a", action: "logout" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			// Should be called twice: once for user-a (2 records), once for user-b (1 record)
			expect(handler).toHaveBeenCalledTimes(2);

			const calls = handler.mock.calls.map((call) => call[0]);
			const userABatch = calls.find((batch) => batch.length === 2);
			const userBBatch = calls.find((batch) => batch.length === 1);

			expect(userABatch).toBeDefined();
			expect(userBBatch).toBeDefined();
			expect(userABatch[0].newImage.userId).toBe("user-a");
			expect(userBBatch[0].newImage.userId).toBe("user-b");
		});

		test("PrimaryKeyConfig partitionKey groups by partition key", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isItem = (
				record: unknown,
			): record is { pk: string; sk: string; data: string } =>
				typeof record === "object" && record !== null && "pk" in record;

			router.onInsert(isItem, handler, {
				batch: true,
				batchKey: { partitionKey: "pk" },
			});

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "TENANT#1", sk: "item#a" },
					{ pk: "TENANT#1", sk: "item#a", data: "1a" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "TENANT#1", sk: "item#b" },
					{ pk: "TENANT#1", sk: "item#b", data: "1b" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "TENANT#2", sk: "item#a" },
					{ pk: "TENANT#2", sk: "item#a", data: "2a" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			// Should be called twice: once for TENANT#1 (2 records), once for TENANT#2 (1 record)
			expect(handler).toHaveBeenCalledTimes(2);

			const calls = handler.mock.calls.map((call) => call[0]);
			const tenant1Batch = calls.find((batch) => batch.length === 2);
			const tenant2Batch = calls.find((batch) => batch.length === 1);

			expect(tenant1Batch).toBeDefined();
			expect(tenant2Batch).toBeDefined();
		});

		test("PrimaryKeyConfig with sortKey groups by composite key", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isItem = (
				record: unknown,
			): record is { pk: string; sk: string; version: number } =>
				typeof record === "object" && record !== null && "pk" in record;

			router.onInsert(isItem, handler, {
				batch: true,
				batchKey: { partitionKey: "pk", sortKey: "sk" },
			});

			const records = [
				// Same pk+sk (same item, multiple versions)
				createStreamRecord(
					"INSERT",
					{ pk: "DOC#1", sk: "v1" },
					{ pk: "DOC#1", sk: "v1", version: 1 },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "DOC#1", sk: "v1" },
					{ pk: "DOC#1", sk: "v1", version: 2 },
				),
				// Different sk (different item)
				createStreamRecord(
					"INSERT",
					{ pk: "DOC#1", sk: "v2" },
					{ pk: "DOC#1", sk: "v2", version: 1 },
				),
				// Different pk (different partition)
				createStreamRecord(
					"INSERT",
					{ pk: "DOC#2", sk: "v1" },
					{ pk: "DOC#2", sk: "v1", version: 1 },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			// Should be called 3 times:
			// - DOC#1#v1 (2 records)
			// - DOC#1#v2 (1 record)
			// - DOC#2#v1 (1 record)
			expect(handler).toHaveBeenCalledTimes(3);

			const calls = handler.mock.calls.map((call) => call[0]);
			const twoRecordBatch = calls.find((batch) => batch.length === 2);
			const oneRecordBatches = calls.filter((batch) => batch.length === 1);

			expect(twoRecordBatch).toBeDefined();
			expect(oneRecordBatches).toHaveLength(2);
		});

		test("batchKey function allows custom grouping logic", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const isEvent = (
				record: unknown,
			): record is { pk: string; timestamp: string } =>
				typeof record === "object" && record !== null && "timestamp" in record;

			// Group by date (extract date from timestamp)
			router.onInsert(isEvent, handler, {
				batch: true,
				batchKey: (record) => {
					const ts = (record as { timestamp: string }).timestamp;
					return ts.split("T")[0]; // Extract date part
				},
			});

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "EVENT#1", sk: "e1" },
					{ pk: "EVENT#1", sk: "e1", timestamp: "2024-01-15T10:00:00Z" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "EVENT#2", sk: "e2" },
					{ pk: "EVENT#2", sk: "e2", timestamp: "2024-01-15T14:00:00Z" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "EVENT#3", sk: "e3" },
					{ pk: "EVENT#3", sk: "e3", timestamp: "2024-01-16T09:00:00Z" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			// Should be called twice: once for 2024-01-15 (2 records), once for 2024-01-16 (1 record)
			expect(handler).toHaveBeenCalledTimes(2);

			const calls = handler.mock.calls.map((call) => call[0]);
			const jan15Batch = calls.find((batch) => batch.length === 2);
			const jan16Batch = calls.find((batch) => batch.length === 1);

			expect(jan15Batch).toBeDefined();
			expect(jan16Batch).toBeDefined();
		});
	});

	describe("Batch Handler Errors", () => {
		test("batch handler error is captured in processing result", async () => {
			const router = new StreamRouter();
			const handler = vi.fn().mockImplementation(() => {
				throw new Error("Batch processing failed");
			});

			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("USER#");

			router.onInsert(isUser, handler, { batch: true });

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "USER#1", sk: "profile" },
					{ pk: "USER#1", sk: "profile", name: "Alice" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "USER#2", sk: "profile" },
					{ pk: "USER#2", sk: "profile", name: "Bob" },
				),
			];
			const event = createStreamEvent(records);

			const result = await router.process(event);

			// Records are processed successfully (collected for batch)
			// but batch handler execution fails
			expect(result.succeeded).toBe(2);
			expect(result.failed).toBe(1);
			expect(result.errors).toHaveLength(1);
			expect(result.errors[0].error.message).toBe("Batch processing failed");
			expect(result.errors[0].recordId).toMatch(/^batch_/);
		});

		test("batch handler error with non-Error thrown is wrapped", async () => {
			const router = new StreamRouter();
			const handler = vi.fn().mockImplementation(() => {
				throw "String error"; // Non-Error thrown
			});

			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" && record !== null;

			router.onInsert(isUser, handler, { batch: true });

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "USER#1", sk: "profile" },
					{ pk: "USER#1", sk: "profile" },
				),
			];
			const event = createStreamEvent(records);

			const result = await router.process(event);

			expect(result.failed).toBe(1);
			expect(result.errors[0].error.message).toBe("String error");
		});

		test("multiple batch handlers with one failing", async () => {
			const router = new StreamRouter();
			const successHandler = vi.fn();
			const failHandler = vi.fn().mockImplementation(() => {
				throw new Error("Handler 2 failed");
			});

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

			router.onInsert(isUser, successHandler, { batch: true });
			router.onInsert(isOrder, failHandler, { batch: true });

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "USER#1", sk: "profile" },
					{ pk: "USER#1", sk: "profile" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "ORDER#1", sk: "details" },
					{ pk: "ORDER#1", sk: "details" },
				),
			];
			const event = createStreamEvent(records);

			const result = await router.process(event);

			// Both records processed successfully
			expect(result.succeeded).toBe(2);
			// One batch handler failed
			expect(result.failed).toBe(1);
			expect(result.errors[0].error.message).toBe("Handler 2 failed");

			// Success handler was called
			expect(successHandler).toHaveBeenCalledTimes(1);
			// Fail handler was called (and threw)
			expect(failHandler).toHaveBeenCalledTimes(1);
		});
	});

	describe("Mixed Batch and Non-Batch Handlers", () => {
		test("batch and non-batch handlers can coexist", async () => {
			const router = new StreamRouter();
			const batchHandler = vi.fn();
			const immediateHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string } =>
				typeof record === "object" &&
				record !== null &&
				"pk" in record &&
				(record as { pk: string }).pk.startsWith("USER#");

			// Batch handler
			router.onInsert(isUser, batchHandler, { batch: true });

			// Non-batch handler (same discriminator)
			router.onInsert(isUser, immediateHandler);

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "USER#1", sk: "profile" },
					{ pk: "USER#1", sk: "profile", name: "Alice" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "USER#2", sk: "profile" },
					{ pk: "USER#2", sk: "profile", name: "Bob" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			// Batch handler called once with all records
			expect(batchHandler).toHaveBeenCalledTimes(1);
			expect(batchHandler.mock.calls[0][0]).toHaveLength(2);

			// Immediate handler called twice (once per record)
			expect(immediateHandler).toHaveBeenCalledTimes(2);
		});

		test("batch handlers only collect matching records", async () => {
			const router = new StreamRouter();
			const userHandler = vi.fn();
			const orderHandler = vi.fn();

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

			router.onInsert(isUser, userHandler, { batch: true });
			router.onInsert(isOrder, orderHandler, { batch: true });

			const records = [
				createStreamRecord(
					"INSERT",
					{ pk: "USER#1", sk: "profile" },
					{ pk: "USER#1", sk: "profile" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "ORDER#1", sk: "details" },
					{ pk: "ORDER#1", sk: "details" },
				),
				createStreamRecord(
					"INSERT",
					{ pk: "USER#2", sk: "profile" },
					{ pk: "USER#2", sk: "profile" },
				),
			];
			const event = createStreamEvent(records);

			await router.process(event);

			// User handler gets 2 records
			expect(userHandler).toHaveBeenCalledTimes(1);
			expect(userHandler.mock.calls[0][0]).toHaveLength(2);

			// Order handler gets 1 record
			expect(orderHandler).toHaveBeenCalledTimes(1);
			expect(orderHandler.mock.calls[0][0]).toHaveLength(1);
		});
	});
});
