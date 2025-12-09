/**
 * Tests for validationTarget option
 */

import { describe, expect, it, vi } from "vitest";
import { StreamRouter } from "../src/stream-router.js";
import { createMockStreamEvent } from "./test-utils.js";

interface User {
	id: string;
	name: string;
	email: string;
}

interface AdminUser extends User {
	role: "admin";
	permissions: string[];
}

// Type guards
function isUser(record: unknown): record is User {
	return (
		typeof record === "object" &&
		record !== null &&
		"id" in record &&
		"name" in record &&
		"email" in record
	);
}

function isAdminUser(record: unknown): record is AdminUser {
	return (
		isUser(record) &&
		"role" in record &&
		(record as AdminUser).role === "admin" &&
		"permissions" in record &&
		Array.isArray((record as AdminUser).permissions)
	);
}

describe("ValidationTarget Option", () => {
	describe("MODIFY events with validationTarget", () => {
		it("should validate against newImage by default", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler);

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: { id: { S: "1" }, invalidField: { S: "old" } }, // Not a User
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						}, // Valid User
					},
				},
			]);

			await router.process(event);

			// Handler should execute because newImage matches
			expect(handler).toHaveBeenCalledTimes(1);
		});

		it('should validate against newImage when validationTarget is "newImage"', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, { validationTarget: "newImage" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: { id: { S: "1" }, invalidField: { S: "old" } }, // Not a User
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						}, // Valid User
					},
				},
			]);

			await router.process(event);

			// Handler should execute because newImage matches
			expect(handler).toHaveBeenCalledTimes(1);
		});

		it('should validate against oldImage when validationTarget is "oldImage"', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, { validationTarget: "oldImage" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "Jane" },
							email: { S: "jane@example.com" },
						}, // Valid User
						NewImage: { id: { S: "1" }, invalidField: { S: "new" } }, // Not a User
					},
				},
			]);

			await router.process(event);

			// Handler should execute because oldImage matches
			expect(handler).toHaveBeenCalledTimes(1);
		});

		it('should validate both images when validationTarget is "both"', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, { validationTarget: "both" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "Jane" },
							email: { S: "jane@example.com" },
						}, // Valid User
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						}, // Valid User
					},
				},
			]);

			await router.process(event);

			// Handler should execute because both images match
			expect(handler).toHaveBeenCalledTimes(1);
		});

		it('should NOT execute when validationTarget is "both" and only newImage matches', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, { validationTarget: "both" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: { id: { S: "1" }, invalidField: { S: "old" } }, // Not a User
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						}, // Valid User
					},
				},
			]);

			await router.process(event);

			// Handler should NOT execute because oldImage doesn't match
			expect(handler).not.toHaveBeenCalled();
		});

		it('should NOT execute when validationTarget is "both" and only oldImage matches', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, { validationTarget: "both" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "Jane" },
							email: { S: "jane@example.com" },
						}, // Valid User
						NewImage: { id: { S: "1" }, invalidField: { S: "new" } }, // Not a User
					},
				},
			]);

			await router.process(event);

			// Handler should NOT execute because newImage doesn't match
			expect(handler).not.toHaveBeenCalled();
		});

		it('should NOT execute when validationTarget is "both" and neither image matches', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, { validationTarget: "both" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: { id: { S: "1" }, invalidField: { S: "old" } }, // Not a User
						NewImage: { id: { S: "1" }, invalidField: { S: "new" } }, // Not a User
					},
				},
			]);

			await router.process(event);

			// Handler should NOT execute because neither image matches
			expect(handler).not.toHaveBeenCalled();
		});
	});

	describe("INSERT events with validationTarget", () => {
		it("should validate newImage for INSERT events (default)", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onInsert(isUser, handler);

			const event = createMockStreamEvent([
				{
					eventName: "INSERT",
					dynamodb: {
						Keys: { id: { S: "1" } },
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						},
					},
				},
			]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should validate newImage for INSERT events even with oldImage validation target", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			// validationTarget: "oldImage" is ignored for INSERT (no oldImage exists)
			router.onInsert(isUser, handler, { validationTarget: "oldImage" });

			const event = createMockStreamEvent([
				{
					eventName: "INSERT",
					dynamodb: {
						Keys: { id: { S: "1" } },
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						},
					},
				},
			]);

			await router.process(event);

			// Handler should execute because INSERT only has newImage
			expect(handler).toHaveBeenCalledTimes(1);
		});
	});

	describe("REMOVE events with validationTarget", () => {
		it("should validate oldImage for REMOVE events (default behavior falls back to oldImage)", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onRemove(isUser, handler);

			const event = createMockStreamEvent([
				{
					eventName: "REMOVE",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						},
					},
				},
			]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it('should validate oldImage for REMOVE events with validationTarget "oldImage"', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onRemove(isUser, handler, { validationTarget: "oldImage" });

			const event = createMockStreamEvent([
				{
					eventName: "REMOVE",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						},
					},
				},
			]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});
	});

	describe("Batch processing with validationTarget", () => {
		it('should collect only records where both images match when validationTarget is "both"', async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			router.onModify(isUser, handler, {
				batch: true,
				validationTarget: "both",
			});

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "Jane" },
							email: { S: "jane@example.com" },
						},
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						},
					},
				},
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "2" } },
						OldImage: { id: { S: "2" }, invalidField: { S: "old" } }, // Invalid
						NewImage: {
							id: { S: "2" },
							name: { S: "Bob" },
							email: { S: "bob@example.com" },
						},
					},
				},
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "3" } },
						OldImage: {
							id: { S: "3" },
							name: { S: "Alice" },
							email: { S: "alice@example.com" },
						},
						NewImage: {
							id: { S: "3" },
							name: { S: "Alicia" },
							email: { S: "alicia@example.com" },
						},
					},
				},
			]);

			await router.process(event);

			// Handler should be called once with 2 records (record 2 excluded)
			expect(handler).toHaveBeenCalledTimes(1);
			expect(handler).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						oldImage: expect.objectContaining({ id: "1" }),
						newImage: expect.objectContaining({ id: "1" }),
					}),
					expect.objectContaining({
						oldImage: expect.objectContaining({ id: "3" }),
						newImage: expect.objectContaining({ id: "3" }),
					}),
				]),
			);
		});
	});

	describe("Multiple handlers with different validationTargets", () => {
		it("should execute appropriate handlers based on their validationTarget", async () => {
			const router = new StreamRouter();
			const handlerNewImage = vi.fn();
			const handlerOldImage = vi.fn();
			const handlerBoth = vi.fn();

			router.onModify(isUser, handlerNewImage, {
				validationTarget: "newImage",
			});
			router.onModify(isUser, handlerOldImage, {
				validationTarget: "oldImage",
			});
			router.onModify(isUser, handlerBoth, { validationTarget: "both" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: { id: { S: "1" }, invalidField: { S: "old" } }, // Not a User
						NewImage: {
							id: { S: "1" },
							name: { S: "John" },
							email: { S: "john@example.com" },
						}, // Valid User
					},
				},
			]);

			await router.process(event);

			// Only handlerNewImage should execute
			expect(handlerNewImage).toHaveBeenCalledTimes(1);
			expect(handlerOldImage).not.toHaveBeenCalled();
			expect(handlerBoth).not.toHaveBeenCalled();
		});
	});

	describe("Complex type validation scenarios", () => {
		it("should handle type narrowing with validationTarget both", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			// Only match if both old and new are AdminUser
			router.onModify(isAdminUser, handler, { validationTarget: "both" });

			const event = createMockStreamEvent([
				{
					eventName: "MODIFY",
					dynamodb: {
						Keys: { id: { S: "1" } },
						OldImage: {
							id: { S: "1" },
							name: { S: "Admin" },
							email: { S: "admin@example.com" },
							role: { S: "admin" },
							permissions: { L: [{ S: "read" }, { S: "write" }] },
						},
						NewImage: {
							id: { S: "1" },
							name: { S: "Super Admin" },
							email: { S: "superadmin@example.com" },
							role: { S: "admin" },
							permissions: { L: [{ S: "read" }, { S: "write" }, { S: "delete" }] },
						},
					},
				},
			]);

			await router.process(event);

			expect(handler).toHaveBeenCalledTimes(1);
			const call = handler.mock.calls[0];
			expect(call[0]).toMatchObject({
				id: "1",
				role: "admin",
				permissions: expect.any(Array),
			});
			expect(call[1]).toMatchObject({
				id: "1",
				role: "admin",
				permissions: expect.any(Array),
			});
		});
	});
});
