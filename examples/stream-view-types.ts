/**
 * Stream View Types Example
 *
 * DynamoDB streams can be configured with different view types that determine
 * what data is available in stream records. This example shows how to configure
 * the router for each view type and what data handlers receive.
 *
 * Stream View Types:
 * - KEYS_ONLY: Only primary key attributes (pk, sk)
 * - NEW_IMAGE: Only the new item state after the change
 * - OLD_IMAGE: Only the old item state before the change
 * - NEW_AND_OLD_IMAGES: Both old and new states (default)
 */
import type { DynamoDBStreamHandler } from "aws-lambda";
import { StreamRouter } from "../src";

// Entity type
interface User {
	pk: string;
	sk: string;
	name: string;
	email: string;
}

const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("USER#");

// ============================================================================
// KEYS_ONLY Mode
// ============================================================================
// Use when you only need to know WHICH item changed, not the data itself.
// Useful for triggering cache invalidation or external lookups.

const keysOnlyRouter = new StreamRouter({ streamViewType: "KEYS_ONLY" });

keysOnlyRouter
	.onInsert(
		// Discriminator receives keys only - match on key pattern
		(record): record is Record<string, unknown> =>
			typeof record === "object" &&
			record !== null &&
			"pk" in record &&
			String((record as { pk: string }).pk).startsWith("USER#"),
		(keys, ctx) => {
			// Handler receives only the key attributes
			console.log(`[KEYS_ONLY] New item created with keys:`, keys);
			// Typical use: invalidate cache, trigger external lookup
			// await invalidateCache(keys.pk, keys.sk);
		},
	)
	.onModify(
		(record): record is Record<string, unknown> => true,
		(keys, ctx) => {
			// MODIFY handler also receives only keys
			console.log(`[KEYS_ONLY] Item modified:`, keys);
		},
	)
	.onRemove(
		(record): record is Record<string, unknown> => true,
		(keys, ctx) => {
			// REMOVE handler receives only keys
			console.log(`[KEYS_ONLY] Item deleted:`, keys);
		},
	);

// ============================================================================
// NEW_IMAGE Mode
// ============================================================================
// Use when you only need the final state after changes.
// Good for syncing to external systems or search indexes.

const newImageRouter = new StreamRouter({ streamViewType: "NEW_IMAGE" });

newImageRouter
	.onInsert(isUser, (newUser, ctx) => {
		// INSERT: receives the new item
		console.log(`[NEW_IMAGE] User created:`, newUser.name, newUser.email);
		// Sync to search index, external system, etc.
	})
	.onModify(isUser, (oldImage, newUser, ctx) => {
		// MODIFY: oldImage is undefined, only newImage available
		// Note: oldImage parameter exists for API consistency but is always undefined
		console.log(`[NEW_IMAGE] User updated to:`, newUser.name);
		console.log(`[NEW_IMAGE] Old state not available:`, oldImage); // undefined
		// Update search index with new state
	})
	.onRemove(
		(record): record is Record<string, unknown> => true,
		(oldImage, ctx) => {
			// REMOVE: oldImage is undefined (no data available)
			console.log(`[NEW_IMAGE] Item removed, no data available:`, oldImage);
			// Can only know something was deleted, not what
		},
	);

// ============================================================================
// OLD_IMAGE Mode
// ============================================================================
// Use when you need to know what was there BEFORE the change.
// Good for audit logs, compliance, or undo functionality.

const oldImageRouter = new StreamRouter({ streamViewType: "OLD_IMAGE" });

oldImageRouter
	.onInsert(
		(record): record is Record<string, unknown> => true,
		(newImage, ctx) => {
			// INSERT: newImage is undefined (item didn't exist before)
			console.log(`[OLD_IMAGE] New item created, no previous state:`, newImage);
		},
	)
	.onModify(isUser, (oldUser, newImage, ctx) => {
		// MODIFY: only oldImage available, newImage is undefined
		console.log(`[OLD_IMAGE] User was:`, oldUser.name, oldUser.email);
		console.log(`[OLD_IMAGE] New state not available:`, newImage); // undefined
		// Log what the item WAS before modification
	})
	.onRemove(isUser, (deletedUser, ctx) => {
		// REMOVE: receives the old item that was deleted
		console.log(`[OLD_IMAGE] User deleted:`, deletedUser.name, deletedUser.email);
		// Archive deleted data, audit log, etc.
	});

// ============================================================================
// NEW_AND_OLD_IMAGES Mode (Default)
// ============================================================================
// Use when you need to compare before/after states.
// Best for change detection, diff calculations, audit trails.

const fullRouter = new StreamRouter(); // Default: NEW_AND_OLD_IMAGES

fullRouter
	.onInsert(isUser, (newUser, ctx) => {
		// INSERT: receives the new item
		console.log(`[FULL] User created:`, newUser.name);
	})
	.onModify(isUser, (oldUser, newUser, ctx) => {
		// MODIFY: receives BOTH old and new images
		console.log(`[FULL] User changed from:`, oldUser.name, `to:`, newUser.name);

		// Can detect specific changes
		if (oldUser.email !== newUser.email) {
			console.log(`[FULL] Email changed: ${oldUser.email} -> ${newUser.email}`);
		}
	})
	.onRemove(isUser, (deletedUser, ctx) => {
		// REMOVE: receives the deleted item
		console.log(`[FULL] User deleted:`, deletedUser.name);
	});

// ============================================================================
// Lambda Handlers
// ============================================================================

export const keysOnlyHandler: DynamoDBStreamHandler = async (event) => {
	return keysOnlyRouter.process(event, { reportBatchItemFailures: true });
};

export const newImageHandler: DynamoDBStreamHandler = async (event) => {
	return newImageRouter.process(event, { reportBatchItemFailures: true });
};

export const oldImageHandler: DynamoDBStreamHandler = async (event) => {
	return oldImageRouter.process(event, { reportBatchItemFailures: true });
};

export const fullHandler: DynamoDBStreamHandler = async (event) => {
	return fullRouter.process(event, { reportBatchItemFailures: true });
};
