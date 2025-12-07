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
		"@shelf/jest-dynamodb",
		"@aws-sdk/client-dynamodb",
		"@aws-sdk/client-dynamodb-streams",
		"@aws-sdk/lib-dynamodb",
	],
	peerDeps: ["@aws-sdk/util-dynamodb"],
	jestOptions: {
		jestConfig: {
			preset: "@shelf/jest-dynamodb",
		},
	},
});

project.synth();
