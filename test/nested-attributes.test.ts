/**
 * Tests for nested attribute change detection with dot notation
 */
import { describe, expect, it } from "vitest";
import {
	diffAttributes,
	getNestedValue,
	hasAttributeChange,
} from "../src/attribute-diff";

describe("getNestedValue", () => {
	it("should get top-level value", () => {
		const obj = { name: "John", age: 30 };
		expect(getNestedValue(obj, "name")).toBe("John");
		expect(getNestedValue(obj, "age")).toBe(30);
	});

	it("should get nested value with dot notation", () => {
		const obj = {
			user: {
				profile: {
					name: "John",
					settings: {
						theme: "dark",
					},
				},
			},
		};
		expect(getNestedValue(obj, "user.profile.name")).toBe("John");
		expect(getNestedValue(obj, "user.profile.settings.theme")).toBe("dark");
	});

	it("should return undefined for non-existent paths", () => {
		const obj = { user: { name: "John" } };
		expect(getNestedValue(obj, "user.email")).toBeUndefined();
		expect(getNestedValue(obj, "nonexistent")).toBeUndefined();
		expect(getNestedValue(obj, "user.profile.name")).toBeUndefined();
	});

	it("should handle undefined/null objects", () => {
		expect(getNestedValue(undefined, "name")).toBeUndefined();
		expect(
			getNestedValue({ user: null } as Record<string, unknown>, "user.name"),
		).toBeUndefined();
	});
});

describe("diffAttributes with nested paths", () => {
	it("should detect changes to nested attributes", () => {
		const oldImage = {
			id: "123",
			preferences: {
				theme: "light",
				notifications: true,
			},
		};
		const newImage = {
			id: "123",
			preferences: {
				theme: "dark",
				notifications: true,
			},
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		// Should have change for parent "preferences" and nested "preferences.theme"
		expect(result.changes.some((c) => c.attribute === "preferences")).toBe(
			true,
		);
		expect(
			result.changes.some((c) => c.attribute === "preferences.theme"),
		).toBe(true);
	});

	it("should detect new nested attributes", () => {
		const oldImage = {
			preferences: {
				theme: "light",
			},
		};
		const newImage = {
			preferences: {
				theme: "light",
				notifications: true,
			},
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const newAttr = result.changes.find(
			(c) =>
				c.attribute === "preferences.notifications" &&
				c.changeType === "new_attribute",
		);
		expect(newAttr).toBeDefined();
		expect(newAttr?.newValue).toBe(true);
	});

	it("should detect removed nested attributes", () => {
		const oldImage = {
			preferences: {
				theme: "light",
				notifications: true,
			},
		};
		const newImage = {
			preferences: {
				theme: "light",
			},
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		const removedAttr = result.changes.find(
			(c) =>
				c.attribute === "preferences.notifications" &&
				c.changeType === "remove_attribute",
		);
		expect(removedAttr).toBeDefined();
		expect(removedAttr?.oldValue).toBe(true);
	});

	it("should detect deeply nested changes", () => {
		const oldImage = {
			user: {
				profile: {
					settings: {
						theme: "light",
					},
				},
			},
		};
		const newImage = {
			user: {
				profile: {
					settings: {
						theme: "dark",
					},
				},
			},
		};

		const result = diffAttributes(oldImage, newImage);

		expect(result.hasChanges).toBe(true);
		expect(
			result.changes.some((c) => c.attribute === "user.profile.settings.theme"),
		).toBe(true);
	});
});

describe("hasAttributeChange with nested paths", () => {
	it("should match exact nested path", () => {
		const oldImage = {
			preferences: { theme: "light", notifications: true },
		};
		const newImage = {
			preferences: { theme: "dark", notifications: true },
		};

		const result = diffAttributes(oldImage, newImage);

		expect(hasAttributeChange(result, "preferences.theme")).toBe(true);
		expect(hasAttributeChange(result, "preferences.notifications")).toBe(false);
	});

	it("should match parent path when child changes", () => {
		const oldImage = {
			preferences: { theme: "light" },
		};
		const newImage = {
			preferences: { theme: "dark" },
		};

		const result = diffAttributes(oldImage, newImage);

		// Watching "preferences" should catch changes to "preferences.theme"
		expect(hasAttributeChange(result, "preferences")).toBe(true);
		expect(hasAttributeChange(result, "preferences.theme")).toBe(true);
	});

	it("should match with change type filter on nested path", () => {
		const oldImage = {
			preferences: { theme: "light" },
		};
		const newImage = {
			preferences: { theme: "dark" },
		};

		const result = diffAttributes(oldImage, newImage);

		expect(
			hasAttributeChange(result, "preferences.theme", "changed_attribute"),
		).toBe(true);
		expect(
			hasAttributeChange(result, "preferences.theme", "new_attribute"),
		).toBe(false);
	});

	it("should detect new nested attribute with change type", () => {
		const oldImage = {
			preferences: { theme: "light" },
		};
		const newImage = {
			preferences: { theme: "light", notifications: true },
		};

		const result = diffAttributes(oldImage, newImage);

		expect(
			hasAttributeChange(result, "preferences.notifications", "new_attribute"),
		).toBe(true);
		expect(hasAttributeChange(result, "preferences", "new_attribute")).toBe(
			true,
		); // Parent catches child
	});

	it("should not match child path when only parent changes type", () => {
		const oldImage = {
			preferences: "simple-string",
		};
		const newImage = {
			preferences: { theme: "dark" },
		};

		const result = diffAttributes(oldImage, newImage);

		// The parent "preferences" changed from string to object
		expect(hasAttributeChange(result, "preferences", "changed_attribute")).toBe(
			true,
		);
	});

	it("should handle multiple change types with nested paths", () => {
		const oldImage = {
			settings: {
				theme: "light",
				oldSetting: "value",
			},
		};
		const newImage = {
			settings: {
				theme: "dark",
				newSetting: "value",
			},
		};

		const result = diffAttributes(oldImage, newImage);

		// Multiple changes under settings
		expect(
			hasAttributeChange(result, "settings", [
				"changed_attribute",
				"new_attribute",
			]),
		).toBe(true);
		expect(
			hasAttributeChange(result, "settings.theme", "changed_attribute"),
		).toBe(true);
		expect(
			hasAttributeChange(result, "settings.newSetting", "new_attribute"),
		).toBe(true);
		expect(
			hasAttributeChange(result, "settings.oldSetting", "remove_attribute"),
		).toBe(true);
	});
});

describe("Integration: nested attribute filtering in router context", () => {
	it("should work with realistic user preferences scenario", () => {
		const oldImage = {
			pk: "USER#123",
			sk: "PROFILE",
			name: "John",
			preferences: {
				theme: "light",
				notifications: {
					email: true,
					push: false,
				},
				language: "en",
			},
		};
		const newImage = {
			pk: "USER#123",
			sk: "PROFILE",
			name: "John",
			preferences: {
				theme: "dark",
				notifications: {
					email: true,
					push: true,
				},
				language: "en",
			},
		};

		const result = diffAttributes(oldImage, newImage);

		// Theme changed
		expect(
			hasAttributeChange(result, "preferences.theme", "changed_attribute"),
		).toBe(true);

		// Push notification changed
		expect(
			hasAttributeChange(
				result,
				"preferences.notifications.push",
				"changed_attribute",
			),
		).toBe(true);

		// Email notification did not change
		expect(hasAttributeChange(result, "preferences.notifications.email")).toBe(
			false,
		);

		// Language did not change
		expect(hasAttributeChange(result, "preferences.language")).toBe(false);

		// Watching parent catches all nested changes
		expect(hasAttributeChange(result, "preferences")).toBe(true);
		expect(hasAttributeChange(result, "preferences.notifications")).toBe(true);
	});
});
