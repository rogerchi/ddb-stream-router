/**
 * Basic usage example for DynamoDB Stream Router
 *
 * This example shows how to set up a simple router with handlers
 * for INSERT, MODIFY, and REMOVE events.
 */
import { StreamRouter } from "../src";

// Define your entity types
interface User {
	pk: string;
	sk: string;
	name: string;
	email: string;
}

interface Order {
	pk: string;
	sk: string;
	orderId: string;
	status: string;
	total: number;
}

// Create discriminator functions (type guards)
const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("USER#");

const isOrder = (record: unknown): record is Order =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("ORDER#");

// Create the router
const router = new StreamRouter();

// Register handlers for different event types and entity types
router
	.onInsert(isUser, async (newUser, ctx) => {
		console.log(`New user created: ${newUser.name}`, ctx.eventID);
		// Send welcome email, update analytics, etc.
	})
	.onInsert(isOrder, async (newOrder, ctx) => {
		console.log(`New order placed: ${newOrder.orderId}`, ctx.eventID);
		// Process payment, send confirmation, etc.
	})
	.onModify(isUser, async (oldUser, newUser, ctx) => {
		console.log(`User updated: ${oldUser.name} -> ${newUser.name}`);
		// Sync to external systems, audit log, etc.
	})
	.onModify(
		isOrder,
		async (oldOrder, newOrder, ctx) => {
			console.log(`Order status changed: ${oldOrder.status} -> ${newOrder.status}`);
			// Send status update notification
		},
		{ attribute: "status", changeType: "changed_attribute" },
	)
	.onRemove(isUser, async (deletedUser, ctx) => {
		console.log(`User deleted: ${deletedUser.name}`);
		// Clean up related data, GDPR compliance, etc.
	});

// Lambda handler - simplified export with built-in batch failure support
export const handler = router.streamHandler;
