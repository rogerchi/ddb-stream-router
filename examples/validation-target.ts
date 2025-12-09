/**
 * Example: Using validationTarget option to control which image(s) are validated
 *
 * The validationTarget option allows you to specify which DynamoDB image(s)
 * should be validated against the discriminator or parser:
 *
 * - "newImage" (default): Validate only the new image
 * - "oldImage": Validate only the old image
 * - "both": Validate both images (both must match)
 *
 * This is particularly useful for MODIFY events where you want to ensure
 * both the old and new states match your schema/type, or when you want
 * to validate against the old image instead of the new one.
 */

import { StreamRouter } from "../src/stream-router.js";

interface User {
	id: string;
	name: string;
	email: string;
	role: "admin" | "user";
}

// Type guard discriminator
function isUser(record: unknown): record is User {
	return (
		typeof record === "object" &&
		record !== null &&
		"id" in record &&
		"name" in record &&
		"email" in record &&
		"role" in record
	);
}

const router = new StreamRouter();

// Example 1: Default behavior - validate against newImage
router.onModify(isUser, (oldImage, newImage, ctx) => {
	console.log("MODIFY with default validation (newImage)");
	console.log("Old:", oldImage);
	console.log("New:", newImage);
	// Handler only executes if newImage matches User type
});

// Example 2: Validate against oldImage instead
router.onModify(
	isUser,
	(oldImage, newImage, ctx) => {
		console.log("MODIFY validating oldImage");
		console.log("Old:", oldImage);
		console.log("New:", newImage);
		// Handler only executes if oldImage matches User type
	},
	{ validationTarget: "oldImage" },
);

// Example 3: Validate both images - both must match
router.onModify(
	isUser,
	(oldImage, newImage, ctx) => {
		console.log("MODIFY validating both images");
		console.log("Old:", oldImage);
		console.log("New:", newImage);
		// Handler only executes if BOTH oldImage and newImage match User type
		// This is useful when you want to ensure type consistency across the change
	},
	{ validationTarget: "both" },
);

// Example 4: validationTarget also works with parsers (like Zod)
// Commented out as it requires Zod as a dependency:
/*
import { z } from "zod";

const UserSchema = z.object({
	id: z.string(),
	name: z.string(),
	email: z.string().email(),
	role: z.enum(["admin", "user"]),
});

router.onModify(
	UserSchema,
	(oldImage, newImage, ctx) => {
		console.log("MODIFY with Zod schema - validating both");
		console.log("Old:", oldImage); // Fully typed and validated
		console.log("New:", newImage); // Fully typed and validated
		// Both images are parsed and validated by Zod
	},
	{ validationTarget: "both" },
);
*/

// Example 5: validationTarget with INSERT events
// INSERT only has newImage, so "oldImage" and "both" behave like "newImage"
router.onInsert(
	isUser,
	(newImage, ctx) => {
		console.log("INSERT - validationTarget ignored (no oldImage)");
		console.log("New:", newImage);
	},
	{ validationTarget: "newImage" }, // This is the only meaningful option for INSERT
);

// Example 6: validationTarget with REMOVE events
// REMOVE only has oldImage, so "newImage" and "both" behave like "oldImage"
router.onRemove(
	isUser,
	(oldImage, ctx) => {
		console.log("REMOVE - validating oldImage");
		console.log("Old:", oldImage);
	},
	{ validationTarget: "oldImage" }, // For REMOVE, this validates the removed record
);

// Example 7: Use case - Validate that both old and new states have required structure
// This ensures migrations or schema changes don't break existing records
interface StrictUser {
	id: string;
	name: string;
	email: string;
	role: "admin" | "user";
	// New field that should exist in both old and new
	createdAt: string;
}

function isStrictUser(record: unknown): record is StrictUser {
	return (
		isUser(record) &&
		"createdAt" in (record as Record<string, unknown>) &&
		typeof (record as StrictUser).createdAt === "string"
	);
}

router.onModify(
	isStrictUser,
	(oldImage, newImage, ctx) => {
		console.log("Both old and new states have createdAt field");
		console.log("Old createdAt:", oldImage.createdAt);
		console.log("New createdAt:", newImage.createdAt);
		// This handler only executes if both images have the createdAt field
		// Useful for validating migrations
	},
	{ validationTarget: "both" },
);

// Example 8: Batch processing with validationTarget
router.onModify(
	isUser,
	(records) => {
		console.log("Batch MODIFY with 'both' validation");
		console.log(`Processing ${records.length} records`);
		for (const record of records) {
			console.log(
				`User ${record.oldImage.id}: ${record.oldImage.name} -> ${record.newImage.name}`,
			);
		}
		// All records in the batch have both images validated
	},
	{
		batch: true,
		validationTarget: "both",
	},
);

export const handler = router.streamHandler;
