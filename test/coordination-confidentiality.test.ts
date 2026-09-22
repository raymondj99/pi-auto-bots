import assert from "node:assert/strict";
import { it } from "node:test";
import { redactProjection } from "../src/coordination/policy.ts";
import { createProjection, makeEnvelope } from "../src/coordination/projection.ts";
import { publicMutationResult } from "../src/coordination/results.ts";

it("mutation responses never expose the internal ledger, artifact paths or checkpoints", () => {
	const result = {
		event: {
			seq: 1,
			payload: { id: "artifact", path: "PRIVATE_PATH", checkpoint: "PRIVATE_CHECKPOINT" },
		},
		projection: { roles: { evaluator: "PRIVATE_ROLE" }, idempotency: { key: "PRIVATE_RESULT" } },
		replayed: true,
	};
	assert.deepEqual(publicMutationResult(result), {
		event: { seq: 1, payload: { id: "artifact" } },
		replayed: true,
	});
	assert.equal(result.event.payload.path, "PRIVATE_PATH");
});

it("role views redact private artifacts, raw audit payloads, checkpoints and unrelated tasks", () => {
	const projection = createProjection("session", "branch");
	const actor = { roleId: "evaluator", runId: "run", generation: 1 };
	projection.roles.worker = {
		roleId: "worker",
		capabilities: ["worker"],
		generation: 1,
		active: true,
	};
	projection.roles.evaluator = {
		roleId: "evaluator",
		capabilities: ["evaluator"],
		generation: 1,
		active: true,
		private: true,
		checkpoint: "CHECKPOINT_SECRET",
	};
	projection.artifacts.secret = [
		{
			artifactId: "secret",
			version: 1,
			digest: "hidden-digest",
			bytes: 5,
			path: "/EVALUATOR_PRIVATE_PATH",
			state: "frozen",
			producer: actor,
			privateTo: ["evaluator"],
			consumers: [],
		},
	];
	projection.tasks.hidden = {
		id: "hidden",
		title: "HIDDEN_TASK_TITLE",
		owner: "evaluator",
		state: "ready",
		revision: 1,
		dependsOn: [],
		blockers: [],
		requiredInputs: [],
		outputs: [],
		requiredAcknowledgements: [],
		leases: [],
		reviewSatisfied: false,
		verification: ["EVALUATOR_TRUTH"],
		nextActions: [],
		permits: [],
	};
	projection.audit = [
		makeEnvelope(projection, "artifact.sealed", actor, projection.artifacts.secret[0], "artifact"),
		makeEnvelope(projection, "task.created", actor, projection.tasks.hidden, "task"),
		makeEnvelope(
			projection,
			"checkpoint.saved",
			actor,
			{ roleId: "evaluator", checkpoint: "CHECKPOINT_SECRET" },
			"checkpoint",
		),
	];
	const worker = redactProjection(projection, "worker");
	assert.deepEqual(worker.tasks, {});
	assert.deepEqual(worker.artifacts, {});
	assert.deepEqual(worker.audit, []);
	assert.doesNotMatch(
		JSON.stringify(worker),
		/CHECKPOINT_SECRET|EVALUATOR_TRUTH|EVALUATOR_PRIVATE_PATH|HIDDEN_TASK_TITLE|hidden-digest/,
	);
	const evaluator = redactProjection(projection, "evaluator");
	assert.equal(evaluator.tasks.hidden.verification[0], "EVALUATOR_TRUTH");
	assert.doesNotMatch(JSON.stringify(evaluator), /CHECKPOINT_SECRET|EVALUATOR_PRIVATE_PATH/);
	assert.throws(() => redactProjection(projection, "unknown"), { code: "UNAUTHORIZED" });
	assert.equal(projection.roles.evaluator.checkpoint, "CHECKPOINT_SECRET");
	projection.artifacts.secret.push({
		...projection.artifacts.secret[0],
		version: 2,
		digest: "public-version",
		privateTo: undefined,
	});
	projection.signals.private = {
		id: "private",
		kind: "decision",
		state: "open",
		severity: "critical",
		summary: "EVALUATOR_TRUTH",
		revision: 1,
		targets: [{ roleId: "worker" }],
		artifactRefs: [{ artifactId: "secret", version: 1, digest: "hidden-digest" }],
		requiresAck: [],
		acknowledgements: [],
	};
	const mixedVersions = redactProjection(projection, "worker");
	assert.equal(mixedVersions.artifacts.secret.length, 1);
	assert.equal(mixedVersions.signals.private, undefined);
	assert.doesNotMatch(JSON.stringify(mixedVersions), /hidden-digest|EVALUATOR_TRUTH/);
	assert.equal(
		mixedVersions.audit.length,
		0,
		"a public successor must not reveal private predecessor audit data",
	);
});
