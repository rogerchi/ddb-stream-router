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
		"zod",
	],
	peerDeps: ["@aws-sdk/util-dynamodb", "@aws-sdk/client-sqs"],
	tsconfig: {
		compilerOptions: {
			inlineSourceMap: false,
			inlineSources: false,
			sourceMap: true, // Generate separate .map files instead
		},
	},
});

// Exclude files from npm package
project.npmignore?.addPatterns("/.kiro/", "/examples/", "/biome.jsonc");

project.synth();
