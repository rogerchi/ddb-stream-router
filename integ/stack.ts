#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambdaNodejs from "aws-cdk-lib/aws-lambda-nodejs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as path from "node:path";

class IntegrationTestStack extends cdk.Stack {
	constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
		super(scope, id, props);

		// DynamoDB table with stream enabled and TTL for testing
		const table = new dynamodb.Table(this, "TestTable", {
			partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
			sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
			stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
			removalPolicy: cdk.RemovalPolicy.DESTROY,
			billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
			timeToLiveAttribute: "ttl", // Enable TTL for TTL removal testing
		});

		// Standard SQS queue for deferred processing
		const deferQueue = new sqs.Queue(this, "DeferQueue", {
			visibilityTimeout: cdk.Duration.seconds(30),
			retentionPeriod: cdk.Duration.days(1),
		});

		// FIFO SQS queue for test verification
		const verificationQueue = new sqs.Queue(this, "VerificationQueue", {
			fifo: true,
			contentBasedDeduplication: true,
			visibilityTimeout: cdk.Duration.seconds(30),
			retentionPeriod: cdk.Duration.days(1),
		});

		// Lambda handler function
		const handler = new lambdaNodejs.NodejsFunction(this, "Handler", {
			runtime: lambda.Runtime.NODEJS_20_X,
			entry: path.join(__dirname, "handler.ts"),
			handler: "streamHandler",
			timeout: cdk.Duration.seconds(30),
			environment: {
				DEFER_QUEUE_URL: deferQueue.queueUrl,
				VERIFICATION_QUEUE_URL: verificationQueue.queueUrl,
			},
			bundling: {
				externalModules: [],
				minify: false,
				sourceMap: true,
			},
		});

		// Grant permissions
		table.grantStreamRead(handler);
		deferQueue.grantConsumeMessages(handler);
		deferQueue.grantSendMessages(handler);
		verificationQueue.grantSendMessages(handler);

		// Event source mapping: DynamoDB Stream -> Lambda
		handler.addEventSource(
			new lambdaEventSources.DynamoEventSource(table, {
				startingPosition: lambda.StartingPosition.TRIM_HORIZON,
				batchSize: 10,
				maxBatchingWindow: cdk.Duration.seconds(10), // Wait up to 10s to batch records
				retryAttempts: 2,
				reportBatchItemFailures: true,
			}),
		);

		// Event source mapping: Defer Queue -> Lambda (separate handler export)
		const sqsHandler = new lambdaNodejs.NodejsFunction(this, "SqsHandler", {
			runtime: lambda.Runtime.NODEJS_20_X,
			entry: path.join(__dirname, "handler.ts"),
			handler: "sqsHandler",
			timeout: cdk.Duration.seconds(30),
			environment: {
				DEFER_QUEUE_URL: deferQueue.queueUrl,
				VERIFICATION_QUEUE_URL: verificationQueue.queueUrl,
			},
			bundling: {
				externalModules: [],
				minify: false,
				sourceMap: true,
			},
		});

		deferQueue.grantConsumeMessages(sqsHandler);
		verificationQueue.grantSendMessages(sqsHandler);

		sqsHandler.addEventSource(
			new lambdaEventSources.SqsEventSource(deferQueue, {
				batchSize: 10,
				reportBatchItemFailures: true,
			}),
		);

		// Stack outputs
		new cdk.CfnOutput(this, "TableArn", { value: table.tableArn });
		new cdk.CfnOutput(this, "TableName", { value: table.tableName });
		new cdk.CfnOutput(this, "DeferQueueUrl", { value: deferQueue.queueUrl });
		new cdk.CfnOutput(this, "VerificationQueueUrl", {
			value: verificationQueue.queueUrl,
		});
		new cdk.CfnOutput(this, "HandlerFunctionArn", {
			value: handler.functionArn,
		});
		new cdk.CfnOutput(this, "SqsHandlerFunctionArn", {
			value: sqsHandler.functionArn,
		});
	}
}

const app = new cdk.App();
new IntegrationTestStack(app, "IntegrationTestStack");
