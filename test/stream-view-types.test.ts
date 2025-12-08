/**
 * Tests for different StreamViewType configurations
 *
 * DynamoDB streams can be configured with different view types:
 * - KEYS_ONLY: Only key attributes
 * - NEW_IMAGE: Only the new item state
 * - OLD_IMAGE: Only the old item state
 * - NEW_AND_OLD_IMAGES: Both old and new states (default)
 */

import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { describe, expect, it } from "vitest";
import { StreamRouter } from "../src";

// Helper to create records with specific stream view type data
function createRecord(
	eventName: "INSERT" | "MODIFY" | "REMOVE",
	options: {
		keys?: Record<string, unknown>;
		newImage?: Record<string, unknown>;
		oldImage?: Record<string, unknown>;
		streamViewType?: string;
	},
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
		eventID: `event_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
		eventName,
		eventSource: "aws:dynamodb",
		awsRegion: "us-east-1",
		eventSourceARN:
			"arn:aws:dynamodb:us-east-1:123456789012:table/test/stream/2024-01-01",
		dynamodb: {
			Keys: toAttributeValue(options.keys),
			NewImage: toAttributeValue(options.newImage),
			OldImage: toAttributeValue(options.oldImage),
			SequenceNumber: `seq_${Date.now()}`,
			StreamViewType: options.streamViewType ?? "NEW_AND_OLD_IMAGES",
		},
	} as DynamoDBRecord;
}

function createEvent(records: DynamoDBRecord[]): DynamoDBStreamEvent {
	return { Records: records };
}

describe("StreamViewType: KEYS_ONLY", () => {
	it("should provide only keys to INSERT handler", async () => {
		const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onInsert(discriminator, (keys, ctx) => {
			receivedData.push({ keys, ctx: ctx.eventName });
		});

		const record = createRecord("INSERT", {
			keys: { pk: "USER#123", sk: "PROFILE" },
			newImage: { pk: "USER#123", sk: "PROFILE", name: "John" },
			streamViewType: "KEYS_ONLY",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			keys: { pk: "USER#123", sk: "PROFILE" },
			ctx: "INSERT",
		});
	});

	it("should provide only keys to MODIFY handler", async () => {
		const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onModify(discriminator, (keys, ctx) => {
			receivedData.push({ keys, ctx: ctx.eventName });
		});

		const record = createRecord("MODIFY", {
			keys: { pk: "USER#123", sk: "PROFILE" },
			oldImage: { pk: "USER#123", sk: "PROFILE", name: "John" },
			newImage: { pk: "USER#123", sk: "PROFILE", name: "Jane" },
			streamViewType: "KEYS_ONLY",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			keys: { pk: "USER#123", sk: "PROFILE" },
			ctx: "MODIFY",
		});
	});

	it("should provide only keys to REMOVE handler", async () => {
		const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onRemove(discriminator, (keys, ctx) => {
			receivedData.push({ keys, ctx: ctx.eventName });
		});

		const record = createRecord("REMOVE", {
			keys: { pk: "USER#123", sk: "PROFILE" },
			oldImage: { pk: "USER#123", sk: "PROFILE", name: "John" },
			streamViewType: "KEYS_ONLY",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			keys: { pk: "USER#123", sk: "PROFILE" },
			ctx: "REMOVE",
		});
	});

	it("should work with batch handlers in KEYS_ONLY mode", async () => {
		const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
		const receivedBatch: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onInsert(
			discriminator,
			(records) => {
				receivedBatch.push(...records);
			},
			{ batch: true },
		);

		const records = [
			createRecord("INSERT", {
				keys: { pk: "USER#1" },
				streamViewType: "KEYS_ONLY",
			}),
			createRecord("INSERT", {
				keys: { pk: "USER#2" },
				streamViewType: "KEYS_ONLY",
			}),
		];

		await router.process(createEvent(records));

		expect(receivedBatch).toHaveLength(2);
		expect(receivedBatch[0]).toHaveProperty("keys");
		expect(receivedBatch[0]).toHaveProperty("ctx");
	});
});

describe("StreamViewType: NEW_IMAGE", () => {
	it("should provide newImage to INSERT handler", async () => {
		const router = new StreamRouter({ streamViewType: "NEW_IMAGE" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onInsert(discriminator, (newImage, ctx) => {
			receivedData.push({ newImage, ctx: ctx.eventName });
		});

		const record = createRecord("INSERT", {
			keys: { pk: "USER#123" },
			newImage: { pk: "USER#123", name: "John", email: "john@example.com" },
			streamViewType: "NEW_IMAGE",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			newImage: { pk: "USER#123", name: "John", email: "john@example.com" },
			ctx: "INSERT",
		});
	});

	it("should provide undefined oldImage and newImage to MODIFY handler", async () => {
		const router = new StreamRouter({ streamViewType: "NEW_IMAGE" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onModify(discriminator, (oldImage, newImage, ctx) => {
			receivedData.push({ oldImage, newImage, ctx: ctx.eventName });
		});

		const record = createRecord("MODIFY", {
			keys: { pk: "USER#123" },
			newImage: { pk: "USER#123", name: "Jane" },
			streamViewType: "NEW_IMAGE",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			oldImage: undefined,
			newImage: { pk: "USER#123", name: "Jane" },
			ctx: "MODIFY",
		});
	});

	it("should provide undefined to REMOVE handler (no old image available)", async () => {
		const router = new StreamRouter({ streamViewType: "NEW_IMAGE" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onRemove(discriminator, (oldImage, ctx) => {
			receivedData.push({ oldImage, ctx: ctx.eventName });
		});

		const record = createRecord("REMOVE", {
			keys: { pk: "USER#123" },
			streamViewType: "NEW_IMAGE",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			oldImage: undefined,
			ctx: "REMOVE",
		});
	});
});

describe("StreamViewType: OLD_IMAGE", () => {
	it("should provide undefined to INSERT handler (no new image in OLD_IMAGE mode)", async () => {
		const router = new StreamRouter({ streamViewType: "OLD_IMAGE" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onInsert(discriminator, (newImage, ctx) => {
			receivedData.push({ newImage, ctx: ctx.eventName });
		});

		const record = createRecord("INSERT", {
			keys: { pk: "USER#123" },
			streamViewType: "OLD_IMAGE",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			newImage: undefined,
			ctx: "INSERT",
		});
	});

	it("should provide oldImage and undefined newImage to MODIFY handler", async () => {
		const router = new StreamRouter({ streamViewType: "OLD_IMAGE" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onModify(discriminator, (oldImage, newImage, ctx) => {
			receivedData.push({ oldImage, newImage, ctx: ctx.eventName });
		});

		const record = createRecord("MODIFY", {
			keys: { pk: "USER#123" },
			oldImage: { pk: "USER#123", name: "John" },
			streamViewType: "OLD_IMAGE",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			oldImage: { pk: "USER#123", name: "John" },
			newImage: undefined,
			ctx: "MODIFY",
		});
	});

	it("should provide oldImage to REMOVE handler", async () => {
		const router = new StreamRouter({ streamViewType: "OLD_IMAGE" });
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onRemove(discriminator, (oldImage, ctx) => {
			receivedData.push({ oldImage, ctx: ctx.eventName });
		});

		const record = createRecord("REMOVE", {
			keys: { pk: "USER#123" },
			oldImage: { pk: "USER#123", name: "John", email: "john@example.com" },
			streamViewType: "OLD_IMAGE",
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			oldImage: { pk: "USER#123", name: "John", email: "john@example.com" },
			ctx: "REMOVE",
		});
	});
});

describe("StreamViewType: NEW_AND_OLD_IMAGES (default)", () => {
	it("should provide newImage to INSERT handler", async () => {
		const router = new StreamRouter(); // Default is NEW_AND_OLD_IMAGES
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onInsert(discriminator, (newImage, ctx) => {
			receivedData.push({ newImage, ctx: ctx.eventName });
		});

		const record = createRecord("INSERT", {
			keys: { pk: "USER#123" },
			newImage: { pk: "USER#123", name: "John" },
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			newImage: { pk: "USER#123", name: "John" },
			ctx: "INSERT",
		});
	});

	it("should provide both oldImage and newImage to MODIFY handler", async () => {
		const router = new StreamRouter();
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onModify(discriminator, (oldImage, newImage, ctx) => {
			receivedData.push({ oldImage, newImage, ctx: ctx.eventName });
		});

		const record = createRecord("MODIFY", {
			keys: { pk: "USER#123" },
			oldImage: { pk: "USER#123", name: "John" },
			newImage: { pk: "USER#123", name: "Jane" },
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			oldImage: { pk: "USER#123", name: "John" },
			newImage: { pk: "USER#123", name: "Jane" },
			ctx: "MODIFY",
		});
	});

	it("should provide oldImage to REMOVE handler", async () => {
		const router = new StreamRouter();
		const receivedData: unknown[] = [];

		const discriminator = (
			_record: unknown,
		): _record is Record<string, unknown> => true;

		router.onRemove(discriminator, (oldImage, ctx) => {
			receivedData.push({ oldImage, ctx: ctx.eventName });
		});

		const record = createRecord("REMOVE", {
			keys: { pk: "USER#123" },
			oldImage: { pk: "USER#123", name: "John" },
		});

		await router.process(createEvent([record]));

		expect(receivedData).toHaveLength(1);
		expect(receivedData[0]).toMatchObject({
			oldImage: { pk: "USER#123", name: "John" },
			ctx: "REMOVE",
		});
	});
});

describe("Invalid StreamViewType", () => {
	it("should throw ConfigurationError for invalid stream view type", () => {
		expect(() => {
			// @ts-expect-error Testing invalid input
			new StreamRouter({ streamViewType: "INVALID_TYPE" });
		}).toThrow("Invalid streamViewType");
	});
});
