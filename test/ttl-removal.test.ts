/**
 * Tests for TTL removal event handling
 */

import type { DynamoDBRecord } from "aws-lambda";
import { describe, expect, test, vi } from "vitest";
import { StreamRouter } from "../src/stream-router";
import { createStreamEvent, createStreamRecord } from "./test-utils";

/**
 * Helper to create a TTL removal record
 */
function createTTLRemovalRecord(
	keys: Record<string, unknown>,
	oldImage?: Record<string, unknown>,
): DynamoDBRecord {
	const record = createStreamRecord("REMOVE", keys, undefined, oldImage);
	// Add userIdentity to indicate TTL removal
	return {
		...record,
		userIdentity: {
			type: "Service",
			principalId: "dynamodb.amazonaws.com",
		},
	} as DynamoDBRecord;
}

/**
 * Helper to create a regular user-initiated removal record
 */
function createUserRemovalRecord(
	keys: Record<string, unknown>,
	oldImage?: Record<string, unknown>,
): DynamoDBRecord {
	return createStreamRecord("REMOVE", keys, undefined, oldImage);
}

describe("TTL Removal Events", () => {
	describe("onTTLRemove handler", () => {
		test("handles TTL removal events", async () => {
			const router = new StreamRouter();
			const ttlHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			router.onTTLRemove(isUser, ttlHandler);

			const oldItem = { pk: "user#1", sk: "session", ttl: 1234567890 };
			const record = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				oldItem,
			);
			const event = createStreamEvent([record]);

			const result = await router.process(event);

			expect(result.processed).toBe(1);
			expect(result.succeeded).toBe(1);
			expect(result.failed).toBe(0);
			expect(ttlHandler).toHaveBeenCalledTimes(1);
			expect(ttlHandler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "user#1", sk: "session" }),
				expect.any(Object),
			);
		});

		test("does not handle regular REMOVE events", async () => {
			const router = new StreamRouter();
			const ttlHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			router.onTTLRemove(isUser, ttlHandler);

			const oldItem = { pk: "user#1", sk: "profile" };
			const record = createUserRemovalRecord(
				{ pk: "user#1", sk: "profile" },
				oldItem,
			);
			const event = createStreamEvent([record]);

			const result = await router.process(event);

			expect(result.processed).toBe(1);
			expect(result.succeeded).toBe(1);
			expect(ttlHandler).not.toHaveBeenCalled();
		});

		test("supports batch processing for TTL removals", async () => {
			const router = new StreamRouter();
			const batchHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			router.onTTLRemove(isUser, batchHandler, { batch: true });

			const records = [
				createTTLRemovalRecord(
					{ pk: "user#1", sk: "session" },
					{ pk: "user#1", sk: "session", ttl: 1234567890 },
				),
				createTTLRemovalRecord(
					{ pk: "user#2", sk: "session" },
					{ pk: "user#2", sk: "session", ttl: 1234567891 },
				),
			];
			const event = createStreamEvent(records);

			const result = await router.process(event);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			expect(batchHandler).toHaveBeenCalledTimes(1);
			expect(batchHandler).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						oldImage: expect.objectContaining({ pk: "user#1" }),
					}),
					expect.objectContaining({
						oldImage: expect.objectContaining({ pk: "user#2" }),
					}),
				]),
			);
		});

		test("supports parsers for TTL removal events", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			const sessionParser = {
				parse: (data: unknown) => data as { pk: string; sk: string; ttl: number },
				safeParse: (data: unknown) => {
					if (
						typeof data === "object" &&
						data !== null &&
						"pk" in data &&
						"sk" in data &&
						"ttl" in data
					) {
						return {
							success: true as const,
							data: data as { pk: string; sk: string; ttl: number },
						};
					}
					return { success: false as const, error: new Error("Invalid data") };
				},
			};

			router.onTTLRemove(sessionParser, handler);

			const oldItem = { pk: "user#1", sk: "session", ttl: 1234567890 };
			const record = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				oldItem,
			);
			const event = createStreamEvent([record]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			expect(handler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "user#1", ttl: 1234567890 }),
				expect.any(Object),
			);
		});
	});

	describe("onRemove with excludeTTL option", () => {
		test("excludeTTL: true excludes TTL removals", async () => {
			const router = new StreamRouter();
			const removeHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			router.onRemove(isUser, removeHandler, { excludeTTL: true });

			// Create a TTL removal event
			const ttlRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session", ttl: 1234567890 },
			);

			// Create a user removal event
			const userRecord = createUserRemovalRecord(
				{ pk: "user#2", sk: "profile" },
				{ pk: "user#2", sk: "profile" },
			);

			const event = createStreamEvent([ttlRecord, userRecord]);

			const result = await router.process(event);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			// Handler should only be called for user removal, not TTL removal
			expect(removeHandler).toHaveBeenCalledTimes(1);
			expect(removeHandler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "user#2", sk: "profile" }),
				expect.any(Object),
			);
		});

		test("excludeTTL: false (default) includes TTL removals", async () => {
			const router = new StreamRouter();
			const removeHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			// Default behavior - excludeTTL is false
			router.onRemove(isUser, removeHandler);

			// Create a TTL removal event
			const ttlRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session", ttl: 1234567890 },
			);

			// Create a user removal event
			const userRecord = createUserRemovalRecord(
				{ pk: "user#2", sk: "profile" },
				{ pk: "user#2", sk: "profile" },
			);

			const event = createStreamEvent([ttlRecord, userRecord]);

			const result = await router.process(event);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			// Handler should be called for both TTL and user removals
			expect(removeHandler).toHaveBeenCalledTimes(2);
		});

		test("excludeTTL: false explicitly includes TTL removals", async () => {
			const router = new StreamRouter();
			const removeHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			router.onRemove(isUser, removeHandler, { excludeTTL: false });

			const ttlRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session", ttl: 1234567890 },
			);

			const event = createStreamEvent([ttlRecord]);

			const result = await router.process(event);

			expect(result.processed).toBe(1);
			expect(result.succeeded).toBe(1);
			expect(removeHandler).toHaveBeenCalledTimes(1);
		});
	});

	describe("Combined TTL and regular removal handlers", () => {
		test("separate handlers for TTL and user removals", async () => {
			const router = new StreamRouter();
			const ttlHandler = vi.fn();
			const removeHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			// Register handler for TTL removals only
			router.onTTLRemove(isUser, ttlHandler);

			// Register handler for user removals only
			router.onRemove(isUser, removeHandler, { excludeTTL: true });

			const ttlRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session", ttl: 1234567890 },
			);

			const userRecord = createUserRemovalRecord(
				{ pk: "user#2", sk: "profile" },
				{ pk: "user#2", sk: "profile" },
			);

			const event = createStreamEvent([ttlRecord, userRecord]);

			const result = await router.process(event);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			expect(ttlHandler).toHaveBeenCalledTimes(1);
			expect(ttlHandler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "user#1", sk: "session" }),
				expect.any(Object),
			);
			expect(removeHandler).toHaveBeenCalledTimes(1);
			expect(removeHandler).toHaveBeenCalledWith(
				expect.objectContaining({ pk: "user#2", sk: "profile" }),
				expect.any(Object),
			);
		});

		test("different matchers for TTL and user removals", async () => {
			const router = new StreamRouter();
			const sessionTTLHandler = vi.fn();
			const profileRemoveHandler = vi.fn();

			const isSession = (
				record: unknown,
			): record is { pk: string; sk: string } =>
				typeof record === "object" &&
				record !== null &&
				"sk" in record &&
				(record as { sk: unknown }).sk === "session";

			const isProfile = (
				record: unknown,
			): record is { pk: string; sk: string } =>
				typeof record === "object" &&
				record !== null &&
				"sk" in record &&
				(record as { sk: unknown }).sk === "profile";

			router.onTTLRemove(isSession, sessionTTLHandler);
			router.onRemove(isProfile, profileRemoveHandler, { excludeTTL: true });

			const sessionTTLRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session", ttl: 1234567890 },
			);

			const profileRemoveRecord = createUserRemovalRecord(
				{ pk: "user#2", sk: "profile" },
				{ pk: "user#2", sk: "profile" },
			);

			const event = createStreamEvent([sessionTTLRecord, profileRemoveRecord]);

			const result = await router.process(event);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			expect(sessionTTLHandler).toHaveBeenCalledTimes(1);
			expect(profileRemoveHandler).toHaveBeenCalledTimes(1);
		});
	});

	describe("TTL detection", () => {
		test("isTTLRemoval correctly identifies TTL removals", () => {
			const router = new StreamRouter();

			const ttlRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session" },
			);

			expect(router.isTTLRemoval(ttlRecord)).toBe(true);
		});

		test("isTTLRemoval correctly identifies regular removals", () => {
			const router = new StreamRouter();

			const userRecord = createUserRemovalRecord(
				{ pk: "user#1", sk: "profile" },
				{ pk: "user#1", sk: "profile" },
			);

			expect(router.isTTLRemoval(userRecord)).toBe(false);
		});

		test("isTTLRemoval handles missing userIdentity", () => {
			const router = new StreamRouter();

			const record = createStreamRecord(
				"REMOVE",
				{ pk: "user#1", sk: "profile" },
				undefined,
				{ pk: "user#1", sk: "profile" },
			);

			expect(router.isTTLRemoval(record)).toBe(false);
		});
	});

	describe("Method chaining with onTTLRemove", () => {
		test("can chain after onTTLRemove", async () => {
			const router = new StreamRouter();
			const ttlHandler = vi.fn();
			const insertHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record && typeof (record as { pk: unknown }).pk === "string";

			router.onTTLRemove(isUser, ttlHandler).onInsert(isUser, insertHandler);

			const ttlRecord = createTTLRemovalRecord(
				{ pk: "user#1", sk: "session" },
				{ pk: "user#1", sk: "session", ttl: 1234567890 },
			);

			const insertRecord = createStreamRecord(
				"INSERT",
				{ pk: "user#2", sk: "profile" },
				{ pk: "user#2", sk: "profile", name: "New User" },
			);

			const event = createStreamEvent([ttlRecord, insertRecord]);

			const result = await router.process(event);

			expect(result.processed).toBe(2);
			expect(result.succeeded).toBe(2);
			expect(ttlHandler).toHaveBeenCalledTimes(1);
			expect(insertHandler).toHaveBeenCalledTimes(1);
		});

		test("can chain defer() after onTTLRemove", () => {
			const router = new StreamRouter({
				deferQueue: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				sqsClient: {
					sendMessage: vi.fn().mockResolvedValue({}),
				},
			});
			const ttlHandler = vi.fn();

			const isUser = (record: unknown): record is { pk: string; sk: string } => typeof record === "object" && record !== null && "pk" in record;

			// Should not throw
			expect(() => {
				router.onTTLRemove(isUser, ttlHandler).defer("ttl-handler");
			}).not.toThrow();
		});
	});
});
