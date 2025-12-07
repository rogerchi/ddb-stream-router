import { typescript } from "projen";

const project = new typescript.TypeScriptProject({
	defaultReleaseBranch: "main",
	name: "ddb-stream-router",
	projenrcTs: true,
	biome: true,
});

project.synth();
