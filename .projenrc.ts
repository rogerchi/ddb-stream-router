import { Vitest } from "@nikovirtala/projen-vitest";
import { typescript } from "projen";
import { NpmAccess } from "projen/lib/javascript";

const project = new typescript.TypeScriptProject({
	defaultReleaseBranch: "main",
	name: "@rogerchi/ddb-stream-router",
	authorEmail: "roger@rogerchi.com",
	authorName: "Roger Chi",
	releaseToNpm: true,
	repository: "github:rogerchi/ddb-stream-router",
	npmAccess: NpmAccess.PUBLIC,
	npmTrustedPublishing: true,
	description:
		"Express-like routing for DynamoDB Streams with type-safe handlers, validation, attribute filtering, batch processing, and SQS deferral",
	docgen: false,
	keywords: ["aws-sdk", "dynamodb"],
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
		"swc",
		"tsx",
		"@nikovirtala/projen-vitest",
	],
	jest: false,
	peerDeps: ["@aws-sdk/util-dynamodb", "@aws-sdk/client-sqs"],
	tsconfig: {
		compilerOptions: {
			inlineSourceMap: false,
			inlineSources: false,
			sourceMap: true, // Generate separate .map files instead
			module: "nodenext",
			target: "es2015",
			isolatedModules: true,
		},
	},
	entrypointTypes: "types/index.d.ts",
	minNodeVersion: "24",
	releaseBranches: {
		beta: {
			prerelease: "beta",
			majorVersion: 0,
			npmDistTag: "beta",
		},
	},
});

project.defaultTask?.reset(
	`tsc .projenrc.ts --noEmit  && tsx --tsconfig ${project.tsconfigDev.fileName} .projenrc.ts`,
);

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
	"/docs/",
);

// Exclude CDK outputs from git
project.gitignore?.addPatterns(
	".cdk.outputs.integration.json",
	"integ/cdk.out/",
	"cjs/",
	"esm/",
	"types/",
);

project.addFields({
	files: ["esm", "cjs"],
	main: "./cjs/index.js",
	exports: {
		".": {
			import: {
				types: "./esm/index.d.ts",
				default: "./esm/index.js",
			},
			require: {
				types: "./cjs/index.d.ts",
				default: "./cjs/index.js",
			},
		},
		"./*": {
			import: "./esm/*",
			require: "./cjs/*",
		},
	},
	type: "module",
});

project.compileTask.reset();
project.compileTask.exec("swc src -d esm -C module.type=es6");
project.compileTask.exec("swc src -d cjs -C module.type=commonjs");
project.compileTask.exec(
	"tsc --outDir esm --declaration --emitDeclarationOnly",
);
project.compileTask.exec(
	"tsc --outDir cjs --declaration --emitDeclarationOnly",
);
// write {"type": "commonjs"} to cjs/package.json
project.compileTask.exec('echo \'{"type": "commonjs"}\' > cjs/package.json');

new Vitest(project);

project.synth();
