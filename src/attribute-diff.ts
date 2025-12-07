import type { AttributeChangeType, AttributeDiff, DiffResult } from "./types";

/**
 * Checks if a value is a collection type (List, Map, or Set-like array).
 */
function isCollection(value: unknown): boolean {
	return (
		Array.isArray(value) ||
		(typeof value === "object" && value !== null && !isSet(value))
	);
}

/**
 * Checks if a value represents a DynamoDB Set (SS, NS, BS become arrays after unmarshalling).
 * Since unmarshalled sets are just arrays, we treat arrays as potential collections.
 */
function isSet(_value: unknown): boolean {
	// After unmarshalling, DynamoDB Sets become regular arrays
	// We can't distinguish them from Lists, so we treat both as collections
	return false;
}

/**
 * Checks if a value is a Map/Object (not an array).
 */
function isMap(value: unknown): boolean {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Deep equality check for comparing attribute values.
 */
function deepEqual(a: unknown, b: unknown): boolean {
	if (a === b) return true;
	if (a === null || b === null) return a === b;
	if (typeof a !== typeof b) return false;

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
 */
function detectCollectionChange(
	oldValue: unknown,
	newValue: unknown,
): AttributeChangeType | null {
	// Both must be collections for collection-level change detection
	if (!isCollection(oldValue) && !isCollection(newValue)) {
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

	// Handle map/object changes
	if (isMap(oldValue) && isMap(newValue)) {
		const oldObj = oldValue as Record<string, unknown>;
		const newObj = newValue as Record<string, unknown>;
		const oldKeys = new Set(Object.keys(oldObj));
		const newKeys = new Set(Object.keys(newObj));

		const addedKeys = [...newKeys].filter((k) => !oldKeys.has(k));
		const removedKeys = [...oldKeys].filter((k) => !newKeys.has(k));
		const commonKeys = [...oldKeys].filter((k) => newKeys.has(k));

		const hasChangedItems = commonKeys.some(
			(key) => !deepEqual(oldObj[key], newObj[key]),
		);

		if (hasChangedItems) return "changed_item_in_collection";
		if (addedKeys.length > 0 && removedKeys.length === 0)
			return "new_item_in_collection";
		if (removedKeys.length > 0 && addedKeys.length === 0)
			return "remove_item_from_collection";
		if (addedKeys.length > 0 && removedKeys.length > 0)
			return "changed_item_in_collection";

		return null;
	}

	return null;
}

/**
 * Compares oldImage and newImage to detect attribute-level changes.
 * Returns a DiffResult containing all detected changes.
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

	const oldKeys = new Set(oldImage ? Object.keys(oldImage) : []);
	const newKeys = new Set(newImage ? Object.keys(newImage) : []);
	const allKeys = new Set([...oldKeys, ...newKeys]);

	for (const key of allKeys) {
		const oldValue = oldImage?.[key];
		const newValue = newImage?.[key];
		const existsInOld = oldKeys.has(key);
		const existsInNew = newKeys.has(key);

		// New attribute added
		if (!existsInOld && existsInNew) {
			changes.push({
				attribute: key,
				changeType: "new_attribute",
				newValue,
			});
			continue;
		}

		// Attribute removed
		if (existsInOld && !existsInNew) {
			changes.push({
				attribute: key,
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

			// Check for collection-level changes first
			const collectionChange = detectCollectionChange(oldValue, newValue);
			if (collectionChange) {
				changes.push({
					attribute: key,
					changeType: collectionChange,
					oldValue,
					newValue,
				});
			} else {
				// Scalar attribute changed
				changes.push({
					attribute: key,
					changeType: "changed_attribute",
					oldValue,
					newValue,
				});
			}
		}
	}

	return {
		hasChanges: changes.length > 0,
		changes,
	};
}

/**
 * Checks if a specific attribute has a specific type of change.
 *
 * @param diffResult - The result from diffAttributes
 * @param attribute - The attribute name to check
 * @param changeTypes - The change type(s) to match (OR logic if array)
 * @returns true if the attribute has any of the specified change types
 */
export function hasAttributeChange(
	diffResult: DiffResult,
	attribute: string,
	changeTypes?: AttributeChangeType | AttributeChangeType[],
): boolean {
	const attributeChanges = diffResult.changes.filter(
		(c) => c.attribute === attribute,
	);

	if (attributeChanges.length === 0) {
		return false;
	}

	if (!changeTypes) {
		return true; // Any change to the attribute
	}

	const typesArray = Array.isArray(changeTypes) ? changeTypes : [changeTypes];
	return attributeChanges.some((c) => typesArray.includes(c.changeType));
}
