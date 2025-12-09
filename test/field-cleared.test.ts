/**
 * Tests for field_cleared change type
 */
import { describe, it, expect } from "vitest";
import { diffAttributes } from "../src/attribute-diff";

describe("field_cleared change type", () => {
	it("should detect when a field is cleared (non-null to null)", () => {
		const oldImage = {
			id: "user1",
			email: "test@example.com",
			status: "active",
		};

		const newImage = {
			id: "user1",
			email: null,
			status: "active",
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const emailChange = result.changes.find((c) => c.attribute === "email");
		expect(emailChange).toBeDefined();
		expect(emailChange?.changeType).toBe("field_cleared");
		expect(emailChange?.oldValue).toBe("test@example.com");
		expect(emailChange?.newValue).toBe(null);
	});

	it("should NOT detect field_cleared when field changes from null to non-null", () => {
		const oldImage = {
			id: "user1",
			email: null,
			status: "active",
		};

		const newImage = {
			id: "user1",
			email: "test@example.com",
			status: "active",
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const emailChange = result.changes.find((c) => c.attribute === "email");
		expect(emailChange).toBeDefined();
		// Should be changed_attribute, not field_cleared
		expect(emailChange?.changeType).toBe("changed_attribute");
	});

	it("should NOT detect field_cleared when field changes from one non-null value to another", () => {
		const oldImage = {
			id: "user1",
			email: "old@example.com",
			status: "active",
		};

		const newImage = {
			id: "user1",
			email: "new@example.com",
			status: "active",
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const emailChange = result.changes.find((c) => c.attribute === "email");
		expect(emailChange).toBeDefined();
		// Should be changed_attribute, not field_cleared
		expect(emailChange?.changeType).toBe("changed_attribute");
	});

	it("should detect field_cleared for nested attributes", () => {
		const oldImage = {
			id: "user1",
			profile: {
				bio: "My biography",
				avatar: "url",
			},
		};

		const newImage = {
			id: "user1",
			profile: {
				bio: null,
				avatar: "url",
			},
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const bioChange = result.changes.find((c) => c.attribute === "profile.bio");
		expect(bioChange).toBeDefined();
		expect(bioChange?.changeType).toBe("field_cleared");
		expect(bioChange?.oldValue).toBe("My biography");
		expect(bioChange?.newValue).toBe(null);
	});

	it("should handle undefined to null transition as changed_attribute", () => {
		const oldImage = {
			id: "user1",
		};

		const newImage = {
			id: "user1",
			email: null,
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const emailChange = result.changes.find((c) => c.attribute === "email");
		expect(emailChange).toBeDefined();
		// New attribute with null value should be new_attribute
		expect(emailChange?.changeType).toBe("new_attribute");
	});
});
