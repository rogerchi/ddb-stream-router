/**
 * Tests for basic event processing (INSERT, MODIFY, REMOVE)
 */
import { StreamRouter } from "../src/stream-router";
import { createStreamEvent, createStreamRecord } from "./test-utils";

describe("Event Processing", () => {
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

			router.onInsert(isUser, handler);

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

			router.onInsert(userParser, handler);

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

			router.onModify(isUser, handler, {
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

			router.onModify(isAny, handler);

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

			router.onRemove(isUser, handler);

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

			router.onInsert(isUser, handler1);
			router.onInsert(isAdmin, handler2);

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

			router.onInsert(isAny, insertHandler);
			router.onModify(isAny, modifyHandler);
			router.onRemove(isAny, removeHandler);

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

	describe("Stream View Types", () => {
		test("KEYS_ONLY stream view type", async () => {
			const router = new StreamRouter({ streamViewType: "KEYS_ONLY" });
			const handler = jest.fn();

			const isAny = (_record: unknown): _record is Record<string, unknown> =>
				true;

			router.onInsert(isAny, handler);

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

			router.onInsert(isAny, handler);

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

			router.onRemove(isAny, handler);

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

			router.onModify(isAny, handler);

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
});
