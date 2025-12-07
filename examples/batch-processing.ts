/**
 * Batch processing example
 *
 * This example shows how to collect multiple records and process
 * them together, useful for bulk operations or aggregations.
 *
 * Note: Batch processing collects all matching records from a single
 * Lambda invocation and passes them to the handler as an array.
 */
import type { DynamoDBStreamEvent } from "aws-lambda";
import { StreamRouter } from "../src";

// Entity types
interface InventoryChange {
	pk: string; // e.g., "INV#warehouse-1"
	sk: string; // e.g., "PRODUCT#sku-123"
	productId: string;
	warehouseId: string;
	quantity: number;
}

interface AuditLog {
	pk: string;
	sk: string;
	action: string;
	userId: string;
	timestamp: string;
}

const isInventoryChange = (record: unknown): record is InventoryChange =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("INV#");

const isAuditLog = (record: unknown): record is AuditLog =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("AUDIT#");

const router = new StreamRouter();

// Batch all inventory changes together (no grouping key)
// All matching records in the batch are passed to the handler at once
router.insert(
	isInventoryChange,
	async (records) => {
		console.log(`Processing ${records.length} inventory changes in batch`);

		// Aggregate changes by product
		const changesByProduct = new Map<string, number>();
		for (const { newImage } of records) {
			const current = changesByProduct.get(newImage.productId) ?? 0;
			changesByProduct.set(newImage.productId, current + newImage.quantity);
		}

		// Bulk update inventory system
		await bulkUpdateInventory(changesByProduct);
	},
	{ batch: true },
);

// Batch audit logs by user ID (simple string attribute)
// Records are grouped by the userId attribute before handler is called
router.insert(
	isAuditLog,
	async (records) => {
		const userId = records[0].newImage.userId;
		console.log(`Processing ${records.length} audit logs for user ${userId}`);

		// Send batched notification per user
		await sendUserActivitySummary(
			userId,
			records.map((r) => r.newImage),
		);
	},
	{ batch: true, batchKey: "userId" },
);

// Group by primary key (partition key + sort key)
// This groups all changes to the same DynamoDB item together
router.modify(
	isInventoryChange,
	async (records) => {
		// All records in this batch are for the same pk+sk (same item)
		const { pk, sk } = records[0].newImage;
		console.log(`Processing ${records.length} updates for item ${pk}/${sk}`);

		// Process all changes to this specific item
		for (const { oldImage, newImage } of records) {
			console.log(`  Quantity: ${oldImage.quantity} -> ${newImage.quantity}`);
		}
	},
	{
		batch: true,
		batchKey: { pk: "pk", sk: "sk" }, // Group by composite primary key
	},
);

// Group by partition key only (all items with same pk)
// Useful when you want to process all items in a partition together
router.remove(
	isInventoryChange,
	async (records) => {
		const warehouseId = records[0].oldImage.warehouseId;
		console.log(`Processing ${records.length} deletions for warehouse ${warehouseId}`);
	},
	{
		batch: true,
		batchKey: { pk: "pk" }, // Group by partition key only
	},
);

// Lambda handler
export async function handler(event: DynamoDBStreamEvent) {
	const result = await router.process(event);
	console.log(`Batch processed ${result.processed} records`);
}

// Placeholder functions
async function bulkUpdateInventory(changes: Map<string, number>) {
	console.log("Bulk updating inventory:", Object.fromEntries(changes));
}

async function sendUserActivitySummary(userId: string, logs: AuditLog[]) {
	console.log(`Sending activity summary to user ${userId}:`, logs.length, "events");
}
