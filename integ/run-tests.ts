#!/usr/bin/env npx ts-node
import * as fs from "node:fs";
import * as path from "node:path";
import assert from "node:assert";
import {
	DynamoDBClient,
	PutItemCommand,
	UpdateItemCommand,
	DeleteItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
	SQSClient,
	ReceiveMessageCommand,
	DeleteMessageCommand,
} from "@aws-sdk/client-sqs";

// Configuration interface
interface CdkOutputs {
	IntegrationTestStack: {
		TableArn: string;
		TableName: string;
		DeferQueueUrl: string;
		VerificationQueueUrl: string;
		HandlerFunctionArn: string;
		SqsHandlerFunctionArn: string;
	};
}

// Verification message interface
interface VerificationMessage {
	operationType: "INSERT" | "MODIFY" | "REMOVE";
	isDeferred: boolean;
	pk: string;
	sk: string;
	timestamp: number;
	eventId?: string;
}

// Load CDK outputs
function loadConfig(): CdkOutputs {
	const outputsPath = path.join(__dirname, "..", ".cdk.outputs.integration.json");
	if (!fs.existsSync(outputsPath)) {
		throw new Error(
			`CDK outputs file not found at ${outputsPath}. Run 'npm run integ:deploy' first.`,
		);
	}
	const content = fs.readFileSync(outputsPath, "utf-8");
	return JSON.parse(content) as CdkOutputs;
}

// Poll verification queue for messages
async function pollVerificationQueue(
	sqsClient: SQSClient,
	queueUrl: string,
	expectedCount: number,
	timeoutMs = 60000,
): Promise<VerificationMessage[]> {
	const messages: VerificationMessage[] = [];
	const startTime = Date.now();
	const pollInterval = 2000;

	console.log(`  Polling for ${expectedCount} messages (timeout: ${timeoutMs / 1000}s)...`);

	while (messages.length < expectedCount && Date.now() - startTime < timeoutMs) {
		const response = await sqsClient.send(
			new ReceiveMessageCommand({
				QueueUrl: queueUrl,
				MaxNumberOfMessages: 10,
				WaitTimeSeconds: 5,
			}),
		);

		if (response.Messages) {
			for (const msg of response.Messages) {
				if (msg.Body && msg.ReceiptHandle) {
					const parsed = JSON.parse(msg.Body) as VerificationMessage;
					messages.push(parsed);
					console.log(
						`  Received: ${parsed.operationType} (deferred: ${parsed.isDeferred}) - ${parsed.pk}`,
					);

					// Delete message after reading
					await sqsClient.send(
						new DeleteMessageCommand({
							QueueUrl: queueUrl,
							ReceiptHandle: msg.ReceiptHandle,
						}),
					);
				}
			}
		}

		if (messages.length < expectedCount) {
			await new Promise((resolve) => setTimeout(resolve, pollInterval));
		}
	}

	return messages;
}

// Main test runner
async function runTests(): Promise<void> {
	console.log("\n🧪 DDB Stream Router Integration Tests\n");
	console.log("=".repeat(50));

	// Load configuration
	console.log("\n📋 Loading configuration...");
	const config = loadConfig();
	const { TableName, VerificationQueueUrl } = config.IntegrationTestStack;
	console.log(`  Table: ${TableName}`);
	console.log(`  Verification Queue: ${VerificationQueueUrl}`);

	// Initialize clients
	const ddbClient = new DynamoDBClient({});
	const sqsClient = new SQSClient({});

	// Test data
	const testPk = `TEST#${Date.now()}`;
	const testSk = "v0";

	let totalTests = 0;
	let passedTests = 0;

	try {
		// Test 1: INSERT operation
		console.log("\n📝 Test 1: INSERT operation");
		console.log("-".repeat(50));
		console.log(`  Creating item: pk=${testPk}, sk=${testSk}`);

		await ddbClient.send(
			new PutItemCommand({
				TableName,
				Item: {
					pk: { S: testPk },
					sk: { S: testSk },
					data: { S: "initial data" },
				},
			}),
		);

		// Wait for stream processing + deferred processing
		const insertMessages = await pollVerificationQueue(
			sqsClient,
			VerificationQueueUrl,
			2, // Expect immediate INSERT + deferred INSERT
			60000,
		);

		totalTests++;
		try {
			assert.strictEqual(
				insertMessages.length,
				2,
				`Expected 2 INSERT messages, got ${insertMessages.length}`,
			);

			const immediateInsert = insertMessages.find(
				(m) => m.operationType === "INSERT" && !m.isDeferred,
			);
			const deferredInsert = insertMessages.find(
				(m) => m.operationType === "INSERT" && m.isDeferred,
			);

			assert.ok(immediateInsert, "Missing immediate INSERT message");
			assert.ok(deferredInsert, "Missing deferred INSERT message");
			assert.strictEqual(immediateInsert.pk, testPk);
			assert.strictEqual(deferredInsert.pk, testPk);

			console.log("  ✅ INSERT test passed");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ INSERT test failed: ${(error as Error).message}`);
		}

		// Test 2: MODIFY operation
		console.log("\n📝 Test 2: MODIFY operation");
		console.log("-".repeat(50));
		console.log(`  Updating item: pk=${testPk}, sk=${testSk}`);

		await ddbClient.send(
			new UpdateItemCommand({
				TableName,
				Key: {
					pk: { S: testPk },
					sk: { S: testSk },
				},
				UpdateExpression: "SET #data = :data",
				ExpressionAttributeNames: { "#data": "data" },
				ExpressionAttributeValues: { ":data": { S: "updated data" } },
			}),
		);

		const modifyMessages = await pollVerificationQueue(
			sqsClient,
			VerificationQueueUrl,
			1, // Expect immediate MODIFY only
			30000,
		);

		totalTests++;
		try {
			assert.strictEqual(
				modifyMessages.length,
				1,
				`Expected 1 MODIFY message, got ${modifyMessages.length}`,
			);

			const modifyMsg = modifyMessages[0];
			assert.strictEqual(modifyMsg.operationType, "MODIFY");
			assert.strictEqual(modifyMsg.isDeferred, false);
			assert.strictEqual(modifyMsg.pk, testPk);

			console.log("  ✅ MODIFY test passed");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ MODIFY test failed: ${(error as Error).message}`);
		}

		// Test 3: REMOVE operation
		console.log("\n📝 Test 3: REMOVE operation");
		console.log("-".repeat(50));
		console.log(`  Deleting item: pk=${testPk}, sk=${testSk}`);

		await ddbClient.send(
			new DeleteItemCommand({
				TableName,
				Key: {
					pk: { S: testPk },
					sk: { S: testSk },
				},
			}),
		);

		const removeMessages = await pollVerificationQueue(
			sqsClient,
			VerificationQueueUrl,
			1, // Expect immediate REMOVE only
			30000,
		);

		totalTests++;
		try {
			assert.strictEqual(
				removeMessages.length,
				1,
				`Expected 1 REMOVE message, got ${removeMessages.length}`,
			);

			const removeMsg = removeMessages[0];
			assert.strictEqual(removeMsg.operationType, "REMOVE");
			assert.strictEqual(removeMsg.isDeferred, false);
			assert.strictEqual(removeMsg.pk, testPk);

			console.log("  ✅ REMOVE test passed");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ REMOVE test failed: ${(error as Error).message}`);
		}

		// Summary
		console.log("\n" + "=".repeat(50));
		console.log("📊 Test Summary");
		console.log("-".repeat(50));
		console.log(`  Total:  ${totalTests}`);
		console.log(`  Passed: ${passedTests}`);
		console.log(`  Failed: ${totalTests - passedTests}`);

		if (passedTests === totalTests) {
			console.log("\n✅ All integration tests passed!\n");
			process.exit(0);
		} else {
			console.log("\n❌ Some integration tests failed.\n");
			process.exit(1);
		}
	} catch (error) {
		console.error("\n❌ Test execution error:", error);
		process.exit(1);
	}
}

// Run tests
runTests();
