/**
 * Tests for DynamoDB Set attribute change detection
 *
 * DynamoDB has three Set types:
 * - SS (String Set) - becomes Set<string> after unmarshalling
 * - NS (Number Set) - becomes Set<number> after unmarshalling
 * - BS (Binary Set) - becomes Set<Uint8Array> after unmarshalling
 */
import { describe, expect, it } from "vitest";
import { diffAttributes, hasAttributeChange } from "../src/attribute-diff";

describe("String Set (SS) change detection", () => {
	it("should detect new items added to string set", () => {
		const oldImage = {
			tags: new Set(["tag1", "tag2"]),
		};
		const newImage = {
			tags: new Set(["tag1", "tag2", "tag3"]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(hasAttributeChange(result, "tags", "new_item_in_collection")).toBe(
			true,
		);
	});

	it("should detect items removed from string set", () => {
		const oldImage = {
			tags: new Set(["tag1", "tag2", "tag3"]),
		};
		const newImage = {
			tags: new Set(["tag1", "tag2"]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(
			hasAttributeChange(result, "tags", "remove_item_from_collection"),
		).toBe(true);
	});

	it("should detect items both added and removed (changed)", () => {
		const oldImage = {
			tags: new Set(["tag1", "tag2"]),
		};
		const newImage = {
			tags: new Set(["tag1", "tag3"]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(
			hasAttributeChange(result, "tags", "changed_item_in_collection"),
		).toBe(true);
	});

	it("should not detect changes when sets are equal", () => {
		const oldImage = {
			tags: new Set(["tag1", "tag2"]),
		};
		const newImage = {
			tags: new Set(["tag2", "tag1"]), // Same items, different order
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(false);
		expect(hasAttributeChange(result, "tags")).toBe(false);
	});

	it("should detect new string set attribute", () => {
		const oldImage = {
			name: "John",
		};
		const newImage = {
			name: "John",
			tags: new Set(["tag1", "tag2"]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(hasAttributeChange(result, "tags", "new_attribute")).toBe(true);
	});

	it("should detect removed string set attribute", () => {
		const oldImage = {
			name: "John",
			tags: new Set(["tag1", "tag2"]),
		};
		const newImage = {
			name: "John",
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(hasAttributeChange(result, "tags", "remove_attribute")).toBe(true);
	});
});

describe("Number Set (NS) change detection", () => {
	it("should detect new items added to number set", () => {
		const oldImage = {
			scores: new Set([100, 200]),
		};
		const newImage = {
			scores: new Set([100, 200, 300]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(hasAttributeChange(result, "scores", "new_item_in_collection")).toBe(
			true,
		);
	});

	it("should detect items removed from number set", () => {
		const oldImage = {
			scores: new Set([100, 200, 300]),
		};
		const newImage = {
			scores: new Set([100, 200]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(
			hasAttributeChange(result, "scores", "remove_item_from_collection"),
		).toBe(true);
	});

	it("should not detect changes when number sets are equal", () => {
		const oldImage = {
			scores: new Set([100, 200, 300]),
		};
		const newImage = {
			scores: new Set([300, 100, 200]), // Same items, different order
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(false);
	});
});

describe("Set with nested attributes", () => {
	it("should detect set changes in nested objects", () => {
		const oldImage = {
			user: {
				permissions: new Set(["read", "write"]),
			},
		};
		const newImage = {
			user: {
				permissions: new Set(["read", "write", "admin"]),
			},
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		// Parent path should show change
		expect(hasAttributeChange(result, "user")).toBe(true);
		// Nested path should show collection change
		expect(
			hasAttributeChange(result, "user.permissions", "new_item_in_collection"),
		).toBe(true);
	});
});

describe("Mixed collection types", () => {
	it("should handle Set changing to Array as changed_attribute", () => {
		const oldImage = {
			items: new Set(["a", "b"]),
		};
		const newImage = {
			items: ["a", "b"], // Changed from Set to Array
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		// Type change should be detected as changed_attribute
		expect(hasAttributeChange(result, "items", "changed_attribute")).toBe(true);
	});

	it("should handle Array changing to Set as changed_attribute", () => {
		const oldImage = {
			items: ["a", "b"],
		};
		const newImage = {
			items: new Set(["a", "b"]), // Changed from Array to Set
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(hasAttributeChange(result, "items", "changed_attribute")).toBe(true);
	});
});

describe("Empty sets", () => {
	it("should detect items added to empty set", () => {
		const oldImage = {
			tags: new Set<string>(),
		};
		const newImage = {
			tags: new Set(["tag1"]),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(hasAttributeChange(result, "tags", "new_item_in_collection")).toBe(
			true,
		);
	});

	it("should detect all items removed (set becomes empty)", () => {
		const oldImage = {
			tags: new Set(["tag1", "tag2"]),
		};
		const newImage = {
			tags: new Set<string>(),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(
			hasAttributeChange(result, "tags", "remove_item_from_collection"),
		).toBe(true);
	});

	it("should not detect changes between two empty sets", () => {
		const oldImage = {
			tags: new Set<string>(),
		};
		const newImage = {
			tags: new Set<string>(),
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(false);
	});
});
