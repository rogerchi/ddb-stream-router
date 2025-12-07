/**
 * Batch processing example
 *
 * This example shows how to collect multiple records and process
 * them together, useful for bulk operations or aggregations.
 */
import type { DynamoDBStreamHandler } from "aws-lambda";
import { StreamRouter, type HandlerContext } from "ddb-stream-router";

// Entity types
interface InventoryChange {
	pk: string;
	sk: string;
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

// Batch all inventory changes together
// Handler receives array of all matching records at once
router.insert(
	isInventoryChange,
	async (records: Array<{ newImage: InventoryChange; ctx: HandlerContext }>) => {
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

// Batch audit logs by user ID
// Records are grouped by the batchKey before handler is called
router.insert(
	isAuditLog,
	async (records: Array<{ newImage: AuditLog; ctx: HandlerContext }>) => {
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

// You can also use a function for complex batch keys
router.modify(
	isInventoryChange,
	async (records) => {
		const warehouseId = (records[0] as { newImage: InventoryChange }).newImage.warehouseId;
		console.log(`Processing inventory updates for warehouse ${warehouseId}`);
	},
	{
		batch: true,
		batchKey: (record: unknown) => (record as InventoryChange).warehouseId,
	},
);

// Lambda handler
export const handler: DynamoDBStreamHandler = async (event) => {
	const result = await router.process(event);
	console.log(`Batch processed ${result.processed} records`);
};

// Placeholder functions
async function bulkUpdateInventory(changes: Map<string, number>) {
	console.log("Bulk updating inventory:", Object.fromEntries(changes));
}

async function sendUserActivitySummary(userId: string, logs: AuditLog[]) {
	console.log(`Sending activity summary to user ${userId}:`, logs.length, "events");
}
