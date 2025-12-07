/**
 * Example using Zod for schema validation
 *
 * This example shows how to use Zod schemas as matchers,
 * which provides runtime validation and type-safe parsed data.
 */
import type { DynamoDBStreamHandler } from "aws-lambda";
import { z } from "zod";
import { StreamRouter } from "../src";

// Define Zod schemas for your entities
const UserSchema = z.object({
	pk: z.string().startsWith("USER#"),
	sk: z.string(),
	name: z.string(),
	email: z.string().email(),
	createdAt: z.string().datetime(),
});

const OrderSchema = z.object({
	pk: z.string().startsWith("ORDER#"),
	sk: z.string(),
	orderId: z.string().uuid(),
	status: z.enum(["pending", "processing", "shipped", "delivered", "cancelled"]),
	total: z.number().positive(),
	items: z.array(
		z.object({
			productId: z.string(),
			quantity: z.number().int().positive(),
			price: z.number().positive(),
		}),
	),
});

// Infer TypeScript types from schemas
type User = z.infer<typeof UserSchema>;
type Order = z.infer<typeof OrderSchema>;

// Create the router
const router = new StreamRouter();

// Register handlers using Zod schemas as matchers
// The handler receives fully validated and typed data
router
	.onInsert(UserSchema, async (newUser: User, ctx) => {
		// newUser is guaranteed to match the schema
		console.log(`Valid user created: ${newUser.email}`);
	})
	.onInsert(OrderSchema, async (newOrder: Order, ctx) => {
		// newOrder is guaranteed to have valid items array
		const itemCount = newOrder.items.reduce((sum, item) => sum + item.quantity, 0);
		console.log(`Order ${newOrder.orderId} placed with ${itemCount} items`);
	})
	.onModify(OrderSchema, async (oldOrder: Order, newOrder: Order, ctx) => {
		if (oldOrder.status !== newOrder.status) {
			console.log(`Order ${newOrder.orderId}: ${oldOrder.status} -> ${newOrder.status}`);
		}
	});

// Lambda handler
export const handler: DynamoDBStreamHandler = async (event) => {
	// Records that don't match any schema are silently skipped
	const result = await router.process(event);
	console.log(`Processed: ${result.succeeded}, Skipped/Failed: ${result.failed}`);
};
