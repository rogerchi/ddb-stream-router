/**
 * Global Tables example with same-region filtering
 *
 * This example shows how to handle DynamoDB Global Tables
 * where streams are replicated across regions.
 */
import type { DynamoDBStreamEvent } from "aws-lambda";
import { StreamRouter } from "../src";

// Entity type
interface User {
	pk: string;
	sk: string;
	name: string;
	region: string;
}

const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("USER#");

// Create router with sameRegionOnly enabled
// This prevents duplicate processing in Global Tables setups
const router = new StreamRouter({
	sameRegionOnly: true, // Only process records from the same region as this Lambda
});

router
	.onInsert(isUser, async (newUser) => {
		// This handler only runs for records that originated in this region
		console.log(`New user in ${process.env.AWS_REGION}: ${newUser.name}`);

		// Safe to perform region-specific operations
		await notifyLocalServices(newUser);
	})
	.onModify(isUser, async (_oldUser, newUser) => {
		console.log(`User updated in ${process.env.AWS_REGION}: ${newUser.name}`);
	});

// Lambda handler
export async function handler(event: DynamoDBStreamEvent) {
	const result = await router.process(event);

	// Records from other regions are automatically skipped
	// They count as "succeeded" but handlers are not invoked
	console.log(
		`Processed ${result.processed} records ` +
			`(some may have been skipped due to region filtering)`,
	);
}

// Placeholder function
async function notifyLocalServices(user: User) {
	console.log(`Notifying local services about user: ${user.name}`);
}
