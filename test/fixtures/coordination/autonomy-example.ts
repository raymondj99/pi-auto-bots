import assert from "node:assert/strict";
import type { Step } from "./worked-example.ts";

export function* autonomyExample(role: string): Generator<Step, void, any> {
	let key = 0;
	function* call(name: string, args: any): Generator<Step, any, any> {
		const result = yield {
			name,
			args:
				name.startsWith("subagent_") && name !== "subagent_team"
					? { request: { ...args, idempotencyKey: `${role}-${++key}` } }
					: args,
		};
		return result?.event?.payload ?? result;
	}
	const tasks = (yield* call("subagent_team", { kind: "tasks" })).state.items;
	const own = tasks.find((t: any) => t.owner === role);
	assert.equal(own.state, "in_progress");
	assert.ok(own.requiredInputs.length > 0);
	if (role === "reviewer") {
		for (const task of tasks.filter((t: any) => t.owner !== role)) {
			assert.ok(task.verification.length > 0, "scoped reviewer sees submitted verification");
			const approved = yield* call("subagent_task", {
				action: "approve",
				taskId: task.id,
				expectedRevision: task.revision,
				reason: `${task.owner}: frozen fixture output independently checked.`,
				nextActions: [],
			});
			assert.equal(approved.state, "completed");
		}
	}
	yield* call("write", { path: `${role}-result.txt`, content: `${role} verified output\n` });
	const output = yield* call("subagent_artifact", {
		action: "publish",
		artifactId: `${role}-output`,
		sourcePath: `${role}-result.txt`,
		version: 1,
		reason: "Scripted real-runtime check",
	});
	yield* call("subagent_task", {
		action: "submit",
		taskId: own.id,
		expectedRevision: own.revision,
		outputs: [{ artifactId: output.artifactId, version: output.version, digest: output.digest }],
		verification: ["Fixture assertions passed"],
		nextActions: [],
	});
	// Intentionally no subagent_done: a bare autonomous run must close itself.
}
