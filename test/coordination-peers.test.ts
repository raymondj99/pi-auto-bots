import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import { buildCoordinationBootstrap } from "../src/coordination/bootstrap.ts";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import type { ArtifactRef } from "../src/coordination/protocol.ts";

const actor = (roleId: string) => ({ roleId, runId: roleId, generation: 1 });
function fixture(maxCorrections = 2) {
	const dir = mkdtempSync(join(tmpdir(), "pi-peer-unit-"));
	const entries: any[] = [],
		wakes: any[] = [];
	const sink = {
		appendEntry(customType: string, data: any) {
			entries.push({ customType, data });
		},
	};
	const b = new CoordinationBroker("peers", "branch", sink, 1000, undefined, {
		workflowMode: "fast",
		artifactStoreDir: join(dir, "artifacts"),
	});
	let key = 0;
	const id = () => `k${++key}`;
	for (const roleId of ["coordinator", "a", "b", "reviewer"]) {
		b.admitRole({
			...(roleId === "coordinator" ? {} : { actor: actor("coordinator") }),
			roleId,
			capabilities:
				roleId === "coordinator"
					? ["coordinator", "reviewer"]
					: roleId === "reviewer"
						? ["worker", "reviewer"]
						: ["worker"],
			idempotencyKey: id(),
		});
		b.bindRole({ roleId, runId: roleId, idempotencyKey: id() });
	}
	const publish = (role: string, name: string): ArtifactRef => {
		const path = join(dir, name);
		writeFileSync(path, `${name}-${key}`);
		const a = b.sealArtifact({
			actor: actor(role),
			artifactId: name,
			sourcePath: path,
			freeze: true,
			idempotencyKey: id(),
		}).event.payload;
		return { artifactId: a.artifactId, version: a.version, digest: a.digest };
	};
	const contract = publish("coordinator", "contract");
	const channel = b.createChannel({
		actor: actor("coordinator"),
		name: "integration",
		purpose: "Direct peer work",
		members: ["coordinator", "a", "b", "reviewer"].map((roleId) => ({
			roleId,
			mode: "participate",
		})),
		review: {
			taskIds: ["a", "b"],
			reviewerRoleId: "reviewer",
			reviewTaskId: "review",
			maxCorrections,
		},
		idempotencyKey: id(),
	}).event.payload;
	b.setDeliverySink((d, s) => wakes.push({ d, s }));
	for (const role of ["reviewer", "a", "b"])
		b.assignDefaultTask({
			actor: actor("coordinator"),
			owner: role,
			title: role,
			...(role !== "reviewer" ? { id: role } : {}),
			requiredInputs: [contract],
			idempotencyKey: id(),
		});
	const submit = (role: string, taskId = role, output = publish(role, `${taskId}-output`)) =>
		b.transitionTask({
			actor: actor(role),
			taskId,
			expectedRevision: b.snapshot().tasks[taskId].revision,
			state: "submitted",
			outputs: [output],
			verification: ["Tests passed"],
			nextActions: [],
			idempotencyKey: id(),
		});
	const decide = (taskId: string, decision: "approved" | "changes_requested" = "approved") =>
		b.reviewTask({
			actor: actor("reviewer"),
			taskId,
			expectedRevision: b.snapshot().tasks[taskId].revision,
			decision,
			reason: `${taskId}: independently checked; ${decision}`,
			nextActions: [],
			idempotencyKey: id(),
		});
	return {
		b,
		entries,
		wakes,
		id,
		submit,
		decide,
		publish,
		contract,
		channel,
		cleanup: () => rmSync(dir, { recursive: true, force: true }),
	};
}

it("short briefs get role-specific coordination instructions and actionable correction errors", () => {
	const f = fixture();
	try {
		const producer = buildCoordinationBootstrap(f.b.snapshot(), "a", 0, "fast");
		const reviewer = buildCoordinationBootstrap(f.b.snapshot(), "reviewer", 0, "fast");
		assert.match(producer, /Your producer workflow/);
		assert.match(producer, /start with the current revision BEFORE/);
		assert.match(producer, /SAME artifactId/);
		assert.match(producer, /Peers in .*a, b, reviewer/);
		assert.doesNotMatch(producer, /Your reviewer workflow/);
		assert.match(reviewer, /Your reviewer workflow/);
		assert.doesNotMatch(reviewer, /Your producer workflow/);
		assert.throws(
			() =>
				f.b.supersedeArtifact({
					actor: actor("a"),
					artifactId: "output",
					previousVersion: 1,
					successorVersion: 2,
					policy: "cancel",
					idempotencyKey: f.id(),
				}),
			(error: any) => error.code === "UNAUTHORIZED" && error.message.includes("SAME artifactId"),
		);
		f.submit("a");
		f.submit("b");
		f.decide("a", "changes_requested");
		const task = f.b.snapshot().tasks.a;
		assert.throws(
			() =>
				f.b.transitionTask({
					actor: actor("a"),
					taskId: "a",
					expectedRevision: task.revision,
					state: "submitted",
					idempotencyKey: f.id(),
				}),
			(error: any) =>
				error.code === "INVALID_TRANSITION" && error.message.includes("subagent_task start"),
		);
		assert.equal(f.b.snapshot().tasks.a.revision, task.revision);
	} finally {
		f.cleanup();
	}
});

it("peer review is gated, peer channels wake directly, and no coordinator GO turn is required", () => {
	const f = fixture();
	try {
		assert.equal(f.b.snapshot().tasks.review.state, "draft");
		assert.equal(f.b.runDisposition({ actor: actor("reviewer") }).keepAlive, true);
		assert.throws(
			() =>
				f.b.transitionTask({
					actor: actor("reviewer"),
					taskId: "review",
					expectedRevision: 1,
					state: "in_progress",
					idempotencyKey: f.id(),
				}),
			{ code: "UNRESOLVED_DEPENDENCY" },
		);
		f.submit("a");
		assert.equal(f.wakes.length, 0);
		assert.throws(() => f.decide("a"), { code: "UNRESOLVED_DEPENDENCY" });
		f.b.sendChannel({
			actor: actor("reviewer"),
			channelId: f.channel.id,
			expectedRevision: 1,
			text: "a: does zero preserve the contract's identity rule?",
			idempotencyKey: f.id(),
		});
		assert.deepEqual(f.wakes.map((w) => w.d.targetRoleId).sort(), ["a", "b"]);
		f.wakes.length = 0;
		f.submit("b");
		assert.deepEqual(
			f.wakes.map((w) => w.d.targetRoleId),
			["reviewer"],
		);
		const report = f.b.snapshot().tasks.review;
		assert.equal(report.state, "in_progress");
		assert.equal(report.requiredInputs.length, 3);
		assert.equal(report.reviewSourceInputs?.length, 2);
		assert.ok(f.b.snapshot().artifacts["a-output"][0].consumers.includes("review"));
		assert.equal(f.b.runDisposition({ actor: actor("a") }).keepAlive, true);
	} finally {
		f.cleanup();
	}
});

it("corrections wake owners, renew frozen inputs and re-review without coordinator mutation or resumes", () => {
	const f = fixture();
	try {
		f.submit("a");
		f.submit("b");
		f.wakes.length = 0;
		f.decide("a", "changes_requested");
		assert.deepEqual(
			f.wakes.map((w) => w.d.targetRoleId),
			["a"],
		);
		assert.equal(f.b.snapshot().tasks.review.state, "blocked");
		assert.equal(f.b.runDisposition({ actor: actor("reviewer") }).phase, "waiting_for_submissions");
		f.b.transitionTask({
			actor: actor("a"),
			taskId: "a",
			expectedRevision: f.b.snapshot().tasks.a.revision,
			state: "in_progress",
			idempotencyKey: f.id(),
		});
		f.submit("a");
		const p = f.b.snapshot();
		assert.equal(p.tasks.review.state, "in_progress");
		assert.equal(
			p.tasks.review.reviewSourceInputs?.find((r) => r.artifactId === "a-output")?.version,
			2,
		);
		assert.equal(p.artifacts["a-output"][0].consumers.includes("review"), false);
		assert.equal(p.artifacts["a-output"][1].consumers.includes("review"), true);
		assert.equal(f.wakes.at(-1).d.targetRoleId, "reviewer");
		const restored = CoordinationBroker.restore(f.entries, "peers", "branch", undefined, 1000, {
			workflowMode: "fast",
		});
		const replayWakes: any[] = [];
		restored.setDeliverySink((d) => replayWakes.push(d));
		assert.equal(restored.snapshot().seq, p.seq, "replay does not append another activation");
		assert.equal(restored.snapshot().tasks.review.revision, p.tasks.review.revision);
	} finally {
		f.cleanup();
	}
});

it("report submission releases peers but cannot self-approve or auto-complete at detach", () => {
	const f = fixture();
	try {
		f.submit("a");
		f.submit("b");
		f.decide("a");
		f.decide("b");
		assert.equal(f.b.runDisposition({ actor: actor("a") }).keepAlive, true);
		const report = f.b.snapshot().tasks.review;
		assert.throws(
			() =>
				f.b.transitionTask({
					actor: actor("reviewer"),
					taskId: "review",
					expectedRevision: report.revision,
					state: "submitted",
					reviewSatisfied: true,
					idempotencyKey: f.id(),
				}),
			{ code: "UNAUTHORIZED" },
		);
		f.wakes.length = 0;
		f.submit("reviewer", "review");
		assert.deepEqual(f.wakes.map((w) => w.d.targetRoleId).sort(), ["a", "b"]);
		for (const role of ["a", "b", "reviewer"])
			assert.equal(f.b.runDisposition({ actor: actor(role) }).keepAlive, false);
		f.b.detachRole({
			actor: actor("coordinator"),
			roleId: "reviewer",
			runId: "reviewer",
			outcome: "finished",
			idempotencyKey: f.id(),
		});
		const task = f.b.snapshot().tasks.review;
		assert.equal(task.state, "review");
		assert.equal(task.reviewSatisfied, false);
		f.b.reviewTask({
			actor: actor("coordinator"),
			taskId: "review",
			expectedRevision: task.revision,
			decision: "approved",
			reason: "Independent aggregate verification",
			nextActions: [],
			idempotencyKey: f.id(),
		});
		assert.equal(f.b.snapshot().tasks.review.state, "completed");
		assert.equal(f.wakes.length, 2, "approval/detach do not release peers repeatedly");
	} finally {
		f.cleanup();
	}
});

it("late reviewer binding activates pending submissions, while stale runs remain fenced", () => {
	const f = fixture();
	try {
		f.b.detachRole({
			actor: actor("coordinator"),
			roleId: "reviewer",
			runId: "reviewer",
			outcome: "interrupted",
			idempotencyKey: f.id(),
		});
		f.submit("a");
		f.submit("b");
		assert.equal(f.wakes.length, 0);
		assert.notEqual(f.b.snapshot().tasks.review.state, "in_progress");
		f.b.bindRole({ roleId: "reviewer", runId: "reviewer-new", idempotencyKey: f.id() });
		assert.equal(f.b.snapshot().tasks.review.state, "in_progress");
		assert.equal(f.b.snapshot().tasks.review.activeRunId, "reviewer-new");
		assert.equal(f.wakes.at(-1).d.targetRoleId, "reviewer");
		assert.throws(() => f.b.runDisposition({ actor: actor("reviewer") }), {
			code: "STALE_GENERATION",
		});
	} finally {
		f.cleanup();
	}
});

it("peer submissions require frozen evidence and report submission waits for every approval", () => {
	const f = fixture();
	try {
		assert.throws(
			() =>
				f.b.transitionTask({
					actor: actor("a"),
					taskId: "a",
					expectedRevision: 1,
					state: "submitted",
					outputs: [],
					verification: [],
					idempotencyKey: f.id(),
				}),
			{ code: "INVALID_TRANSITION" },
		);
		f.submit("a");
		f.submit("b");
		f.decide("a");
		assert.throws(() => f.submit("reviewer", "review"), { code: "UNRESOLVED_DEPENDENCY" });
		assert.equal(f.b.runDisposition({ actor: actor("a") }).keepAlive, true);
	} finally {
		f.cleanup();
	}
});

it("peer owners stay in the channel and handoff cannot erase independent review", () => {
	const f = fixture();
	try {
		assert.throws(
			() =>
				f.b.updateChannel({
					actor: actor("coordinator"),
					channelId: f.channel.id,
					expectedRevision: 1,
					members: f.channel.members.filter((m: any) => m.roleId !== "a"),
					idempotencyKey: f.id(),
				}),
			{ code: "INVALID_TRANSITION" },
		);
		assert.throws(
			() =>
				f.b.handoffTask({
					actor: actor("a"),
					taskId: "a",
					expectedRevision: 1,
					owner: "reviewer",
					reason: "Transfer",
					nextActions: [],
					idempotencyKey: f.id(),
				}),
			{ code: "UNAUTHORIZED" },
		);
		f.b.admitRole({
			actor: actor("coordinator"),
			roleId: "observer",
			capabilities: ["worker"],
			idempotencyKey: f.id(),
		});
		f.b.bindRole({ roleId: "observer", runId: "observer", idempotencyKey: f.id() });
		f.b.updateChannel({
			actor: actor("coordinator"),
			channelId: f.channel.id,
			expectedRevision: 1,
			members: [...f.channel.members, { roleId: "observer", mode: "watch" }],
			idempotencyKey: f.id(),
		});
		f.b.sendChannel({
			actor: actor("reviewer"),
			channelId: f.channel.id,
			expectedRevision: 2,
			text: "a: clarify the boundary convention",
			idempotencyKey: f.id(),
		});
		assert.deepEqual(f.wakes.map((w) => w.d.targetRoleId).sort(), ["a", "b"]);
	} finally {
		f.cleanup();
	}
});

it("peer correction bound and channel cancellation terminate coordination loops explicitly", () => {
	const f = fixture(0);
	try {
		f.submit("a");
		f.submit("b");
		assert.throws(() => f.decide("a", "changes_requested"), { code: "BOUNDS_EXCEEDED" });
		f.wakes.length = 0;
		f.b.closeChannel({
			actor: actor("coordinator"),
			channelId: f.channel.id,
			expectedRevision: 1,
			idempotencyKey: f.id(),
		});
		assert.deepEqual(f.wakes.map((w) => w.d.targetRoleId).sort(), ["a", "b", "reviewer"]);
		assert.equal(f.b.runDisposition({ actor: actor("a") }).keepAlive, false);
	} finally {
		f.cleanup();
	}
});
