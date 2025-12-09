import type {
	AttributeChangeType,
	AttributeDiff,
	DiffResult,
} from "./types.js";

/**
 * Gets a nested value from an object using dot notation path.
 * @param obj - The object to traverse
 * @param path - Dot notation path (e.g., "preferences.theme")
 * @returns The value at the path, or undefined if not found
 */
export function getNestedValue(
	obj: Record<string, unknown> | undefined,
	path: string,
): unknown {
	if (!obj) return undefined;

	const parts = path.split(".");
	let current: unknown = obj;

	for (const part of parts) {
		if (current === null || current === undefined) return undefined;
		if (typeof current !== "object") return undefined;
		current = (current as Record<string, unknown>)[part];
	}

	return current;
}

/**
 * Checks if a value is a JavaScript Set (DynamoDB SS, NS, BS become Set after unmarshalling).
 */
function isSet(value: unknown): value is Set<unknown> {
	return value instanceof Set;
}

/**
 * Checks if a value is a Map/Object (not an array).
 */
function isMap(value: unknown): boolean {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Deep equality check for comparing attribute values.
 * Handles primitives, arrays, objects, and Sets.
 */
export function deepEqual(a: unknown, b: unknown): boolean {
	if (a === b) return true;
	if (a === null || b === null) return a === b;
	if (typeof a !== typeof b) return false;

	// Handle Sets (DynamoDB SS, NS, BS)
	if (isSet(a) && isSet(b)) {
		if (a.size !== b.size) return false;
		// Compare by converting to sorted JSON strings
		const aItems = [...a].map((v) => JSON.stringify(v)).sort();
		const bItems = [...b].map((v) => JSON.stringify(v)).sort();
		return aItems.every((item, index) => item === bItems[index]);
	}

	if (Array.isArray(a) && Array.isArray(b)) {
		if (a.length !== b.length) return false;
		return a.every((item, index) => deepEqual(item, b[index]));
	}

	if (typeof a === "object" && typeof b === "object") {
		const aObj = a as Record<string, unknown>;
		const bObj = b as Record<string, unknown>;
		const aKeys = Object.keys(aObj);
		const bKeys = Object.keys(bObj);

		if (aKeys.length !== bKeys.length) return false;
		return aKeys.every((key) => deepEqual(aObj[key], bObj[key]));
	}

	return false;
}

/**
 * Detects collection-level changes between old and new values.
 * Returns the specific type of collection change if detected.
 *
 * Handles:
 * - Arrays (DynamoDB Lists)
 * - Sets (DynamoDB String Sets, Number Sets, Binary Sets)
 */
function detectCollectionChange(
	oldValue: unknown,
	newValue: unknown,
): AttributeChangeType | null {
	// Handle Set changes (DynamoDB SS, NS, BS)
	if (isSet(oldValue) && isSet(newValue)) {
		const oldSet = oldValue as Set<unknown>;
		const newSet = newValue as Set<unknown>;

		// Convert to comparable strings for complex values
		const oldItems = new Set([...oldSet].map((v) => JSON.stringify(v)));
		const newItems = new Set([...newSet].map((v) => JSON.stringify(v)));

		let hasNewItems = false;
		let hasRemovedItems = false;

		// Check for new items
		for (const item of newItems) {
			if (!oldItems.has(item)) {
				hasNewItems = true;
				break;
			}
		}

		// Check for removed items
		for (const item of oldItems) {
			if (!newItems.has(item)) {
				hasRemovedItems = true;
				break;
			}
		}

		// Sets don't have "changed" items - items are either present or not
		if (hasNewItems && !hasRemovedItems) return "new_item_in_collection";
		if (hasRemovedItems && !hasNewItems) return "remove_item_from_collection";
		if (hasNewItems && hasRemovedItems) return "changed_item_in_collection";

		return null;
	}

	// Handle array/list changes
	if (Array.isArray(oldValue) && Array.isArray(newValue)) {
		// Use a counting approach to handle duplicates correctly
		const oldCounts = new Map<string, number>();
		const newCounts = new Map<string, number>();

		for (const item of oldValue) {
			const key = JSON.stringify(item);
			oldCounts.set(key, (oldCounts.get(key) ?? 0) + 1);
		}

		for (const item of newValue) {
			const key = JSON.stringify(item);
			newCounts.set(key, (newCounts.get(key) ?? 0) + 1);
		}

		let hasNewItems = false;
		let hasRemovedItems = false;

		// Check for new items (items with higher count in new or not in old)
		for (const [key, newCount] of newCounts) {
			const oldCount = oldCounts.get(key) ?? 0;
			if (newCount > oldCount) {
				hasNewItems = true;
				break;
			}
		}

		// Check for removed items (items with higher count in old or not in new)
		for (const [key, oldCount] of oldCounts) {
			const newCount = newCounts.get(key) ?? 0;
			if (oldCount > newCount) {
				hasRemovedItems = true;
				break;
			}
		}

		// Check for changed items at same indices (modification in place)
		const minLength = Math.min(oldValue.length, newValue.length);
		let hasChangedItems = false;
		for (let i = 0; i < minLength; i++) {
			if (!deepEqual(oldValue[i], newValue[i])) {
				const oldStr = JSON.stringify(oldValue[i]);
				const newStr = JSON.stringify(newValue[i]);
				// If both items exist in both arrays (just different positions), it's reordering
				const oldInNew =
					newCounts.has(oldStr) && (newCounts.get(oldStr) ?? 0) > 0;
				const newInOld =
					oldCounts.has(newStr) && (oldCounts.get(newStr) ?? 0) > 0;
				if (!oldInNew || !newInOld) {
					hasChangedItems = true;
					break;
				}
			}
		}

		if (hasChangedItems) return "changed_item_in_collection";
		if (hasNewItems && !hasRemovedItems) return "new_item_in_collection";
		if (hasRemovedItems && !hasNewItems) return "remove_item_from_collection";
		if (hasNewItems && hasRemovedItems) return "changed_item_in_collection";

		return null;
	}

	return null;
}

/**
 * Recursively compares objects and generates changes with nested paths.
 *
 * @param oldObj - The old object
 * @param newObj - The new object
 * @param prefix - Current path prefix for nested attributes
 * @param changes - Array to collect changes
 */
function diffAttributesRecursive(
	oldObj: Record<string, unknown> | undefined,
	newObj: Record<string, unknown> | undefined,
	prefix: string,
	changes: AttributeDiff[],
): void {
	const oldKeys = new Set(oldObj ? Object.keys(oldObj) : []);
	const newKeys = new Set(newObj ? Object.keys(newObj) : []);
	const allKeys = new Set([...oldKeys, ...newKeys]);

	for (const key of allKeys) {
		const path = prefix ? `${prefix}.${key}` : key;
		const oldValue = oldObj?.[key];
		const newValue = newObj?.[key];
		const existsInOld = oldKeys.has(key);
		const existsInNew = newKeys.has(key);

		// New attribute added
		if (!existsInOld && existsInNew) {
			changes.push({
				attribute: path,
				changeType: "new_attribute",
				newValue,
			});
			continue;
		}

		// Attribute removed
		if (existsInOld && !existsInNew) {
			changes.push({
				attribute: path,
				changeType: "remove_attribute",
				oldValue,
			});
			continue;
		}

		// Both exist - check for changes
		if (existsInOld && existsInNew) {
			if (deepEqual(oldValue, newValue)) {
				continue; // No change
			}

			// Check for field_cleared: attribute went from non-null to null
			if (oldValue !== null && oldValue !== undefined && newValue === null) {
				changes.push({
					attribute: path,
					changeType: "field_cleared",
					oldValue,
					newValue,
				});
				continue;
			}

			// Check for collection-level changes first (arrays and Sets)
			if (Array.isArray(oldValue) && Array.isArray(newValue)) {
				const collectionChange = detectCollectionChange(oldValue, newValue);
				if (collectionChange) {
					changes.push({
						attribute: path,
						changeType: collectionChange,
						oldValue,
						newValue,
					});
				}
				continue;
			}

			// Handle DynamoDB Sets (SS, NS, BS become JavaScript Set after unmarshalling)
			if (isSet(oldValue) && isSet(newValue)) {
				const collectionChange = detectCollectionChange(oldValue, newValue);
				if (collectionChange) {
					changes.push({
						attribute: path,
						changeType: collectionChange,
						oldValue,
						newValue,
					});
				}
				continue;
			}

			// For nested objects (Maps), recurse to get granular changes
			if (isMap(oldValue) && isMap(newValue)) {
				// Add a changed_attribute for the parent path
				changes.push({
					attribute: path,
					changeType: "changed_attribute",
					oldValue,
					newValue,
				});

				// Also recurse to get nested changes
				diffAttributesRecursive(
					oldValue as Record<string, unknown>,
					newValue as Record<string, unknown>,
					path,
					changes,
				);
				continue;
			}

			// Scalar attribute changed or type changed
			changes.push({
				attribute: path,
				changeType: "changed_attribute",
				oldValue,
				newValue,
			});
		}
	}
}

/**
 * Compares oldImage and newImage to detect attribute-level changes.
 * Returns a DiffResult containing all detected changes, including nested paths.
 *
 * Nested attributes are represented with dot notation (e.g., "preferences.theme").
 * Both the parent path and nested paths are included in the changes.
 *
 * @param oldImage - The old image from the DynamoDB stream record
 * @param newImage - The new image from the DynamoDB stream record
 * @returns DiffResult with list of AttributeDiff objects
 */
export function diffAttributes(
	oldImage: Record<string, unknown> | undefined,
	newImage: Record<string, unknown> | undefined,
): DiffResult {
	const changes: AttributeDiff[] = [];
	diffAttributesRecursive(oldImage, newImage, "", changes);

	return {
		hasChanges: changes.length > 0,
		changes,
	};
}

/**
 * Checks if a specific attribute has a specific type of change.
 * Supports dot notation for nested attributes (e.g., "preferences.theme").
 *
 * Matching behavior:
 * - Exact match: "preferences.theme" matches changes to "preferences.theme"
 * - Parent match: "preferences" matches changes to "preferences" OR any nested path like "preferences.theme"
 * - Child match: "preferences.theme" does NOT match changes to just "preferences"
 *
 * @param diffResult - The result from diffAttributes
 * @param attribute - The attribute name or path to check (supports dot notation)
 * @param changeTypes - The change type(s) to match (OR logic if array)
 * @returns true if the attribute has any of the specified change types
 */
export function hasAttributeChange(
	diffResult: DiffResult,
	attribute: string,
	changeTypes?: AttributeChangeType | AttributeChangeType[],
): boolean {
	// Find changes that match the attribute path
	// A change matches if:
	// 1. Exact match: change.attribute === attribute
	// 2. Nested match: change.attribute starts with attribute + "." (watching parent catches child changes)
	const attributeChanges = diffResult.changes.filter((c) => {
		// Exact match
		if (c.attribute === attribute) return true;
		// Nested match - if watching "preferences", match "preferences.theme"
		if (c.attribute.startsWith(`${attribute}.`)) return true;
		return false;
	});

	if (attributeChanges.length === 0) {
		return false;
	}

	if (!changeTypes) {
		return true; // Any change to the attribute or its children
	}

	const typesArray = Array.isArray(changeTypes) ? changeTypes : [changeTypes];
	return attributeChanges.some((c) => typesArray.includes(c.changeType));
}
