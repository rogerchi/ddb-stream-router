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
		// CDK dependencies for integration tests
		"aws-cdk-lib",
		"aws-cdk",
		"esbuild",
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

// Add integration test scripts
project.addTask("integ:deploy", {
	description: "Deploy integration test infrastructure",
	cwd: "integ",
	exec: "npx cdk deploy --require-approval never --outputs-file ../.cdk.outputs.integration.json",
});

project.addTask("integ:test", {
	description: "Run integration tests",
	exec: "npx ts-node integ/run-tests.ts",
});

project.addTask("integ:destroy", {
	description: "Destroy integration test infrastructure",
	cwd: "integ",
	exec: "npx cdk destroy --force",
});

// Exclude files from npm package
project.npmignore?.addPatterns(
	"/.kiro/",
	"/examples/",
	"/biome.jsonc",
	"/integ/",
	"/.cdk.outputs.integration.json",
);

// Exclude CDK outputs from git
project.gitignore?.addPatterns(
	".cdk.outputs.integration.json",
	"integ/cdk.out/",
);

project.synth();
