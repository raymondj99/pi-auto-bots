import assert from "node:assert/strict";
import { it } from "node:test";
import { prepareSpawnArguments } from "../src/spawn-prompt.ts";

it("misfiled systemPrompt briefs become task without changing authorization or assignment", () => {
	const input = {
		name: "Parser",
		role: "parser",
		taskId: "parse-task",
		agent: "worker",
		model: "openai-codex/gpt-5.6-sol",
		cwd: "/tmp/example",
		requiredInputs: [{ artifactId: "contract", version: 1, digest: "sha256:example" }],
		systemPrompt: "Implement parser.mjs and parser.test.mjs against the bound contract.",
	};
	const { systemPrompt, ...rest } = input;
	assert.deepEqual(prepareSpawnArguments(input), { ...rest, task: systemPrompt });
	assert.ok(!Object.hasOwn(input, "task"), "normalization does not mutate the recorded call");
	assert.equal(input.systemPrompt, systemPrompt);
});

it("explicit work briefs and separate role instructions remain distinct", () => {
	const input = {
		name: "Parser",
		task: "Implement parser",
		systemPrompt: "Parser role constraints",
	};
	assert.equal(prepareSpawnArguments(input), input);
	assert.deepEqual(
		prepareSpawnArguments({ name: "Parser", task: "  ", systemPrompt: "Implement parser" }),
		{ name: "Parser", task: "Implement parser" },
	);
});

it("assignment IDs cannot silently launch instructionless work and malformed types still reach validation", () => {
	assert.throws(
		() => prepareSpawnArguments({ name: "Parser", taskId: "parse-task" }),
		/task brief/,
	);
	assert.throws(
		() => prepareSpawnArguments({ name: "Parser", task: "", systemPrompt: " " }),
		/task brief/,
	);
	const malformed = {
		name: "Parser",
		task: 42,
		systemPrompt: "Do not conceal invalid typed arguments",
	};
	assert.equal(prepareSpawnArguments(malformed), malformed);
	assert.equal(prepareSpawnArguments(null), null);
});
