import { typescript } from "projen";

const project = new typescript.TypeScriptProject({
	defaultReleaseBranch: "main",
	name: "ddb-stream-router",
	projenrcTs: true,
	biome: true,
	deps: ["@aws-sdk/util-dynamodb"],
	devDeps: [
		"fast-check",
		"@types/aws-lambda",
		"@aws-sdk/client-sqs",
		"@aws-sdk/client-dynamodb",
		"aws-sdk-client-mock",
	],
	peerDeps: ["@aws-sdk/util-dynamodb", "@aws-sdk/client-sqs"],
});

project.synth();
