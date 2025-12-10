import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { z } from "zod";
import { StreamRouter, createSQSClient } from "../src/index.js";
import type { DynamoDBRecord } from "aws-lambda";

// Environment variables
const DEFER_QUEUE_URL = process.env.DEFER_QUEUE_URL!;
const VERIFICATION_QUEUE_URL = process.env.VERIFICATION_QUEUE_URL!;

// Extended verification message format for comprehensive testing
interface VerificationMessage {
	operationType: "INSERT" | "MODIFY" | "REMOVE" | "TTL_REMOVE";
	isDeferred: boolean;
	pk: string;
	sk: string;
	timestamp: number;
	eventId?: string;
	handlerType?: string;
	batchCount?: number;
	batchKey?: string;
	middlewareExecuted?: string[];
	validationTarget?: string;
	changeTypes?: string[];
	nestedPath?: string;
	parsedWithZod?: boolean;
	fieldCleared?: string;
}

// Extended test item interface
interface TestItem {
	pk: string;
	sk: string;
	data?: string;
	status?: string;
	count?: number;
	ttl?: number;
	preferences?: {
		theme?: string;
		notifications?: boolean;
	};
	email?: string | null;
	validatedField?: string;
	skipProcessing?: boolean;
}

// Type guards for different test item prefixes
const isTestItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("TEST#") && typeof r.sk === "string";
};

const isBatchItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("BATCH#") && typeof r.sk === "string";
};

const isMiddlewareItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("MW#") && typeof r.sk === "string";
};

const isTTLItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("TTL#") && typeof r.sk === "string";
};

const isNestedItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("NESTED#") && typeof r.sk === "string";
};

const isClearedItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("CLEARED#") && typeof r.sk === "string";
};

const isValTargetItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("VALTARGET#") && typeof r.sk === "string";
};

const hasValidatedField = (record: unknown): record is TestItem => {
	if (!isValTargetItem(record)) return false;
	return typeof (record as TestItem).validatedField === "string";
};

const isMultiChangeItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return typeof r.pk === "string" && r.pk.startsWith("MULTICHANGE#") && typeof r.sk === "string";
};

// Zod schema for validation testing
const ZodTestSchema = z.object({
	pk: z.string().startsWith("ZOD#"),
	sk: z.string(),
	requiredField: z.string(),
	numericField: z.number(),
});
type ZodTestItem = z.infer<typeof ZodTestSchema>;

// SQS client for sending verification messages
const sqsClient = new SQSClient({});

async function sendVerification(message: VerificationMessage): Promise<void> {
	await sqsClient.send(
		new SendMessageCommand({
			QueueUrl: VERIFICATION_QUEUE_URL,
			MessageBody: JSON.stringify(message),
			MessageGroupId: message.pk,
		}),
	);
}

// Track middleware execution
let middlewareLog: string[] = [];

// Create router
const router = new StreamRouter({
	deferQueue: DEFER_QUEUE_URL,
	sqsClient: createSQSClient(sqsClient, SendMessageCommand),
});


// ============================================================================
// MIDDLEWARE
// ============================================================================

router.use(async (_record: DynamoDBRecord, next: () => Promise<void>) => {
	middlewareLog.push("middleware-1");
	await next();
});

router.use(async (ddbRecord: DynamoDBRecord, next: () => Promise<void>) => {
	const newImage = ddbRecord.dynamodb?.NewImage;
	if (newImage?.skipProcessing?.BOOL === true) {
		return; // Skip handlers
	}
	middlewareLog.push("middleware-2");
	await next();
});

router.use(async (_record: DynamoDBRecord, next: () => Promise<void>) => {
	middlewareLog.push("middleware-3");
	await next();
});

// ============================================================================
// BASIC HANDLERS (TEST# prefix)
// ============================================================================

router.onInsert(isTestItem, async (newImage, ctx) => {
	await sendVerification({
		operationType: "INSERT",
		isDeferred: false,
		pk: newImage.pk,
		sk: newImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
	});
});

router.onModify(isTestItem, async (_oldImage, newImage, ctx) => {
	await sendVerification({
		operationType: "MODIFY",
		isDeferred: false,
		pk: newImage.pk,
		sk: newImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
		handlerType: "modify-all",
	});
});

router.onModify(
	isTestItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "modify-status-change",
		});
	},
	{ attribute: "status" },
);

router.onModify(
	isTestItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "modify-status-pending-to-active",
		});
	},
	{ attribute: "status", oldFieldValue: "pending", newFieldValue: "active" },
);

router.onModify(
	isTestItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "modify-status-to-completed",
		});
	},
	{ attribute: "status", newFieldValue: "completed" },
);

router.onRemove(isTestItem, async (oldImage, ctx) => {
	await sendVerification({
		operationType: "REMOVE",
		isDeferred: false,
		pk: oldImage.pk,
		sk: oldImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
	});
});

router
	.onInsert(isTestItem, async (newImage, ctx) => {
		await sendVerification({
			operationType: "INSERT",
			isDeferred: true,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
		});
	})
	.defer("deferred-insert-handler");

// ============================================================================
// BATCH HANDLERS (BATCH# prefix)
// ============================================================================

router.onInsert(
	isBatchItem,
	async (records) => {
		const firstRecord = records[0].newImage;
		await sendVerification({
			operationType: "INSERT",
			isDeferred: false,
			pk: firstRecord.pk,
			sk: firstRecord.sk,
			timestamp: Date.now(),
			handlerType: "batch-by-status",
			batchCount: records.length,
			batchKey: firstRecord.status ?? "undefined",
		});
	},
	{ batch: true, batchKey: "status" },
);

router.onModify(
	isBatchItem,
	async (records) => {
		const firstRecord = records[0].newImage;
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: firstRecord.pk,
			sk: firstRecord.sk,
			timestamp: Date.now(),
			handlerType: "batch-by-pk",
			batchCount: records.length,
			batchKey: `${firstRecord.pk}#${firstRecord.sk}`,
		});
	},
	{ batch: true, batchKey: { partitionKey: "pk", sortKey: "sk" } },
);


// ============================================================================
// MIDDLEWARE TEST HANDLERS (MW# prefix)
// ============================================================================

router.onInsert(isMiddlewareItem, async (newImage, ctx) => {
	await sendVerification({
		operationType: "INSERT",
		isDeferred: false,
		pk: newImage.pk,
		sk: newImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
		handlerType: "middleware-test",
		middlewareExecuted: [...middlewareLog],
	});
	middlewareLog = [];
});

// ============================================================================
// TTL REMOVAL HANDLERS (TTL# prefix)
// ============================================================================

router.onTTLRemove(isTTLItem, async (oldImage, ctx) => {
	await sendVerification({
		operationType: "TTL_REMOVE",
		isDeferred: false,
		pk: oldImage.pk,
		sk: oldImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
		handlerType: "ttl-remove",
	});
});

router.onRemove(
	isTTLItem,
	async (oldImage, ctx) => {
		await sendVerification({
			operationType: "REMOVE",
			isDeferred: false,
			pk: oldImage.pk,
			sk: oldImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "remove-exclude-ttl",
		});
	},
	{ excludeTTL: true },
);

// ============================================================================
// NESTED ATTRIBUTE HANDLERS (NESTED# prefix)
// ============================================================================

router.onModify(
	isNestedItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "nested-theme-change",
			nestedPath: "preferences.theme",
		});
	},
	{ attribute: "preferences.theme" },
);

router.onModify(
	isNestedItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "nested-notifications-change",
			nestedPath: "preferences.notifications",
		});
	},
	{ attribute: "preferences.notifications" },
);

router.onModify(
	isNestedItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "nested-preferences-any",
			nestedPath: "preferences",
		});
	},
	{ attribute: "preferences" },
);

// ============================================================================
// FIELD CLEARED HANDLERS (CLEARED# prefix)
// ============================================================================

router.onModify(
	isClearedItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "field-cleared-email",
			fieldCleared: "email",
		});
	},
	{ attribute: "email", changeType: "field_cleared" },
);

router.onModify(
	isClearedItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "email-changed",
		});
	},
	{ attribute: "email", changeType: "changed_attribute" },
);


// ============================================================================
// ZOD VALIDATION HANDLERS (ZOD# prefix)
// ============================================================================

router.onInsert(ZodTestSchema, async (newImage: ZodTestItem, ctx) => {
	await sendVerification({
		operationType: "INSERT",
		isDeferred: false,
		pk: newImage.pk,
		sk: newImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
		handlerType: "zod-validated",
		parsedWithZod: true,
	});
});

router.onModify(
	ZodTestSchema,
	async (_oldImage: ZodTestItem, newImage: ZodTestItem, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "zod-modify-validated",
			parsedWithZod: true,
		});
	},
);

// ============================================================================
// VALIDATION TARGET HANDLERS (VALTARGET# prefix)
// ============================================================================

router.onModify(
	hasValidatedField,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "validation-target-new",
			validationTarget: "newImage",
		});
	},
	{ validationTarget: "newImage" },
);

router.onModify(
	hasValidatedField,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "validation-target-old",
			validationTarget: "oldImage",
		});
	},
	{ validationTarget: "oldImage" },
);

router.onModify(
	hasValidatedField,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "validation-target-both",
			validationTarget: "both",
		});
	},
	{ validationTarget: "both" },
);

// ============================================================================
// MULTIPLE CHANGE TYPES HANDLERS (MULTICHANGE# prefix)
// ============================================================================

router.onModify(
	isMultiChangeItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "multi-change-type",
			changeTypes: ["field_cleared", "changed_attribute"],
		});
	},
	{ attribute: "email", changeType: ["field_cleared", "changed_attribute"] },
);

// ============================================================================
// EXPORT HANDLERS
// ============================================================================

export const streamHandler = router.streamHandler;
export const sqsHandler = router.sqsHandler;
