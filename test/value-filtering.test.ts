/**
 * Tests for value-based filtering in onModify handlers
 * Tests the new oldFieldValue, newFieldValue, and field_cleared functionality
 */
import { describe, expect, it, vi } from "vitest";
import { StreamRouter } from "../src/stream-router";
import { createModifyEvent } from "./test-utils";

describe("Value Filtering in onModify", () => {
	describe("newFieldValue filtering", () => {
		it("should trigger handler when field is updated TO the specified value", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Only trigger when status changes TO "active"
			router.onModify(isUser, handler, {
				attribute: "status",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should NOT trigger when field is updated to a different value", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Only trigger when status changes TO "active"
			router.onModify(isUser, handler, {
				attribute: "status",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "suspended" },
			);

			await router.streamHandler(event);

			expect(handler).not.toHaveBeenCalled();
		});

		it("should work with nested attributes", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				preferences: {
					theme: string;
				};
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			router.onModify(isUser, handler, {
				attribute: "preferences.theme",
				newFieldValue: "dark",
			});

			const event = createModifyEvent(
				{ id: "user1", preferences: { theme: "light" } },
				{ id: "user1", preferences: { theme: "dark" } },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should work with complex object values", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface Document {
				id: string;
				metadata: Record<string, unknown>;
			}

			const isDocument = (r: unknown): r is Document =>
				typeof r === "object" && r !== null && "id" in r;

			router.onModify(isDocument, handler, {
				attribute: "metadata",
				newFieldValue: { version: 2, author: "alice" },
			});

			const event = createModifyEvent(
				{ id: "doc1", metadata: { version: 1, author: "bob" } },
				{ id: "doc1", metadata: { version: 2, author: "alice" } },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});
	});

	describe("oldFieldValue filtering", () => {
		it("should trigger handler when field is updated FROM the specified value", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Only trigger when status changes FROM "pending"
			router.onModify(isUser, handler, {
				attribute: "status",
				oldFieldValue: "pending",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should NOT trigger when field is updated from a different value", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Only trigger when status changes FROM "pending"
			router.onModify(isUser, handler, {
				attribute: "status",
				oldFieldValue: "pending",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "active" },
				{ id: "user1", status: "suspended" },
			);

			await router.streamHandler(event);

			expect(handler).not.toHaveBeenCalled();
		});
	});

	describe("oldFieldValue and newFieldValue combined", () => {
		it("should trigger only when field changes FROM specific value TO another specific value", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Only trigger when status changes FROM "pending" TO "active"
			router.onModify(isUser, handler, {
				attribute: "status",
				oldFieldValue: "pending",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should NOT trigger when old value matches but new value does not", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			router.onModify(isUser, handler, {
				attribute: "status",
				oldFieldValue: "pending",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "suspended" }, // Different than expected
			);

			await router.streamHandler(event);

			expect(handler).not.toHaveBeenCalled();
		});

		it("should NOT trigger when new value matches but old value does not", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			router.onModify(isUser, handler, {
				attribute: "status",
				oldFieldValue: "pending",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "suspended" }, // Different than expected
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).not.toHaveBeenCalled();
		});
	});

	describe("value filtering with changeType", () => {
		it("should work with changeType filter", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			router.onModify(isUser, handler, {
				attribute: "status",
				changeType: "changed_attribute",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should NOT trigger if changeType does not match", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
				email?: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Expect new_attribute but status is changed_attribute
			router.onModify(isUser, handler, {
				attribute: "status",
				changeType: "new_attribute",
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).not.toHaveBeenCalled();
		});
	});

	describe("value filtering without attribute specified", () => {
		it("should NOT trigger when value filters are specified without attribute", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Invalid configuration: value filter without attribute
			router.onModify(isUser, handler, {
				newFieldValue: "active",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			// Should not trigger because attribute is required for value filtering
			expect(handler).not.toHaveBeenCalled();
		});
	});

	describe("backward compatibility", () => {
		it("should work without value filters (existing behavior)", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// Traditional filtering without value filters
			router.onModify(isUser, handler, {
				attribute: "status",
				changeType: "changed_attribute",
			});

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});

		it("should trigger on any modify when no filters specified", async () => {
			const router = new StreamRouter();
			const handler = vi.fn();

			interface User {
				id: string;
				status: string;
			}

			const isUser = (r: unknown): r is User =>
				typeof r === "object" && r !== null && "id" in r;

			// No filters at all
			router.onModify(isUser, handler);

			const event = createModifyEvent(
				{ id: "user1", status: "pending" },
				{ id: "user1", status: "active" },
			);

			await router.streamHandler(event);

			expect(handler).toHaveBeenCalledTimes(1);
		});
	});
});

describe("field_cleared change type", () => {
	it("should detect when a field is cleared (non-null to null)", async () => {
		const router = new StreamRouter();
		const handler = vi.fn();

		interface User {
			id: string;
			email: string | null;
		}

		const isUser = (r: unknown): r is User =>
			typeof r === "object" && r !== null && "id" in r;

		router.onModify(isUser, handler, {
			attribute: "email",
			changeType: "field_cleared",
		});

		const event = createModifyEvent(
			{ id: "user1", email: "test@example.com" },
			{ id: "user1", email: null },
		);

		await router.streamHandler(event);

		expect(handler).toHaveBeenCalledTimes(1);
	});

	it("should NOT trigger field_cleared when field changes from null to non-null", async () => {
		const router = new StreamRouter();
		const handler = vi.fn();

		interface User {
			id: string;
			email: string | null;
		}

		const isUser = (r: unknown): r is User =>
			typeof r === "object" && r !== null && "id" in r;

		router.onModify(isUser, handler, {
			attribute: "email",
			changeType: "field_cleared",
		});

		const event = createModifyEvent(
			{ id: "user1", email: null },
			{ id: "user1", email: "test@example.com" },
		);

		await router.streamHandler(event);

		expect(handler).not.toHaveBeenCalled();
	});

	it("should NOT trigger field_cleared when field changes from one non-null value to another", async () => {
		const router = new StreamRouter();
		const handler = vi.fn();

		interface User {
			id: string;
			email: string | null;
		}

		const isUser = (r: unknown): r is User =>
			typeof r === "object" && r !== null && "id" in r;

		router.onModify(isUser, handler, {
			attribute: "email",
			changeType: "field_cleared",
		});

		const event = createModifyEvent(
			{ id: "user1", email: "old@example.com" },
			{ id: "user1", email: "new@example.com" },
		);

		await router.streamHandler(event);

		expect(handler).not.toHaveBeenCalled();
	});

	it("should work with nested attributes", async () => {
		const router = new StreamRouter();
		const handler = vi.fn();

		interface User {
			id: string;
			profile: {
				bio: string | null;
			};
		}

		const isUser = (r: unknown): r is User =>
			typeof r === "object" && r !== null && "id" in r;

		router.onModify(isUser, handler, {
			attribute: "profile.bio",
			changeType: "field_cleared",
		});

		const event = createModifyEvent(
			{ id: "user1", profile: { bio: "My bio" } },
			{ id: "user1", profile: { bio: null } },
		);

		await router.streamHandler(event);

		expect(handler).toHaveBeenCalledTimes(1);
	});

	it("should work with field_cleared in array of change types", async () => {
		const router = new StreamRouter();
		const handler = vi.fn();

		interface User {
			id: string;
			email: string | null;
		}

		const isUser = (r: unknown): r is User =>
			typeof r === "object" && r !== null && "id" in r;

		// Trigger on either field_cleared or changed_attribute
		router.onModify(isUser, handler, {
			attribute: "email",
			changeType: ["field_cleared", "changed_attribute"],
		});

		// Test field_cleared
		const event1 = createModifyEvent(
			{ id: "user1", email: "test@example.com" },
			{ id: "user1", email: null },
		);

		await router.streamHandler(event1);
		expect(handler).toHaveBeenCalledTimes(1);

		// Test changed_attribute
		const event2 = createModifyEvent(
			{ id: "user2", email: "old@example.com" },
			{ id: "user2", email: "new@example.com" },
		);

		await router.streamHandler(event2);
		expect(handler).toHaveBeenCalledTimes(2);
	});

	it("should combine field_cleared with value filters", async () => {
		const router = new StreamRouter();
		const handler = vi.fn();

		interface User {
			id: string;
			email: string | null;
		}

		const isUser = (r: unknown): r is User =>
			typeof r === "object" && r !== null && "id" in r;

		// Only trigger when email is cleared from a specific value
		router.onModify(isUser, handler, {
			attribute: "email",
			changeType: "field_cleared",
			oldFieldValue: "test@example.com",
		});

		// Should trigger - correct old value
		const event1 = createModifyEvent(
			{ id: "user1", email: "test@example.com" },
			{ id: "user1", email: null },
		);

		await router.streamHandler(event1);
		expect(handler).toHaveBeenCalledTimes(1);

		// Should NOT trigger - different old value
		const event2 = createModifyEvent(
			{ id: "user2", email: "other@example.com" },
			{ id: "user2", email: null },
		);

		await router.streamHandler(event2);
		expect(handler).toHaveBeenCalledTimes(1); // Still 1
	});
});
