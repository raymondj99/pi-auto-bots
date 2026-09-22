import assert from "node:assert/strict";
import { it } from "node:test";
import { buildCoordinationBootstrap, MAX_BOOTSTRAP_BYTES } from "../src/coordination/bootstrap.ts";
import { createProjection } from "../src/coordination/projection.ts";

it("maximum-team bootstraps are byte bounded and preserve identity and exact input digests", () => {
	const projection = createProjection();
	const actor = { roleId: "worker", runId: "run-worker", generation: 3 };
	projection.roles.worker = { ...actor, active: true, capabilities: ["worker"] };
	projection.artifacts.pack = [
		{
			artifactId: "pack",
			version: 7,
			digest: "a".repeat(64),
			bytes: 20,
			state: "frozen",
			producer: actor,
			consumers: [],
		},
	];
	for (let i = 0; i < 100; i++)
		projection.tasks[`task-${i}`] = {
			id: `task-${i}`,
			title: "Task",
			owner: "worker",
			state: "ready",
			revision: 1,
			dependsOn: [],
			blockers: [],
			requiredInputs: [{ artifactId: "pack", version: 7, digest: "a".repeat(64) }],
			outputs: [],
			requiredAcknowledgements: [],
			leases: [],
			reviewSatisfied: false,
			verification: [],
			nextActions: [],
			permits: [],
		};
	for (let i = 0; i < 32; i++)
		projection.channels[`channel-${i}`] = {
			id: `channel-${i}`,
			name: "shared",
			purpose: "🚀".repeat(500),
			members: [{ roleId: "worker", mode: "participate" }],
			revision: 1,
			delivery: "digest",
			status: "open",
		};
	const text = buildCoordinationBootstrap(projection, "worker");
	assert.ok(Buffer.byteLength(text) <= MAX_BOOTSTRAP_BYTES);
	assert.match(text, /roleId=worker; runId=run-worker; generation=3/);
	assert.ok(text.includes(`pack@7 sha256=${"a".repeat(64)}`));
	assert.match(text, /entries omitted/);
	assert.match(text, /subagent_team/);
});
