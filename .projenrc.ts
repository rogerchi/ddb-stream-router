import { typescript } from "projen";

const project = new typescript.TypeScriptProject({
	defaultReleaseBranch: "main",
	name: "ddb-stream-router",
	projenrcTs: true,
	biome: true,
	deps: ["@aws-sdk/util-dynamodb"],
	devDeps: ["fast-check", "@types/aws-lambda"],
	peerDeps: ["@aws-sdk/util-dynamodb"],
});

project.synth();
