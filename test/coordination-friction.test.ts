import assert from "node:assert/strict";
import {
	chmodSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	realpathSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { it } from "node:test";
import { buildCoordinationBootstrap } from "../src/coordination/bootstrap.ts";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { formatRunTaskOutcome } from "../src/coordination/completion.ts";
import { buildOperationalView } from "../src/coordination/operational.ts";
import { registerCoordinationTools } from "../src/coordination/tools.ts";

const parent = { roleId: "coordinator", runId: "parent", generation: 1 };
const worker = { roleId: "worker", runId: "worker-1", generation: 1 };
function fixture(mode: "fast" | "strict" = "fast") {
	const dir = mkdtempSync(join(tmpdir(), "pi-friction-"));
	const entries: any[] = [];
	const b = new CoordinationBroker(
		"s",
		"b",
		{
			appendEntry(customType, data) {
				entries.push({ customType, data });
			},
		},
		1000,
		undefined,
		{ workflowMode: mode, artifactStoreDir: join(dir, "store") },
	);
	b.admitRole({ roleId: parent.roleId, capabilities: ["coordinator"], idempotencyKey: "admit" });
	b.bindRole({ roleId: parent.roleId, runId: parent.runId, idempotencyKey: "bind" });
	b.admitRole({
		actor: parent,
		roleId: worker.roleId,
		capabilities: ["worker"],
		idempotencyKey: "admit-worker",
	});
	b.bindRole({ roleId: worker.roleId, runId: worker.runId, idempotencyKey: "bind-worker" });
	const sourcePath = join(dir, "output.txt");
	writeFileSync(sourcePath, "tested output");
	return {
		b,
		dir,
		entries,
		sourcePath,
		cleanup: () => rmSync(dir, { recursive: true, force: true }),
	};
}
function submitted(b: CoordinationBroker, sourcePath: string, id = "task") {
	const output = b.sealArtifact({
		actor: worker,
		artifactId: id,
		sourcePath,
		freeze: true,
		idempotencyKey: `publish-${id}`,
	}).event.payload;
	const ref = { artifactId: output.artifactId, version: output.version, digest: output.digest };
	b.createTask({
		actor: parent,
		owner: worker.roleId,
		id,
		title: id,
		start: true,
		idempotencyKey: `create-${id}`,
	});
	b.transitionTask({
		actor: worker,
		taskId: id,
		state: "submitted",
		expectedRevision: 1,
		outputs: [ref],
		verification: ["Tests pass"],
		reviewSatisfied: false,
		nextActions: [],
		idempotencyKey: `submit-${id}`,
	});
	return ref;
}

it("publication needs no storage argument, resolves caller-relative paths and validates optional version/reason", async () => {
	const f = fixture();
	try {
		const tools = new Map<string, any>();
		registerCoordinationTools(
			{
				registerTool(t: any) {
					tools.set(t.name, t);
				},
			} as any,
			() => ({ broker: f.b, actor: worker }),
			() => true,
		);
		const params = {
			action: "publish",
			artifactId: "result",
			sourcePath: "output.txt",
			version: 1,
			reason: "Independent result",
			idempotencyKey: "publish",
		};
		const call = (args: any) =>
			tools.get("subagent_artifact").execute("call", args, undefined, undefined, { cwd: f.dir });
		const reply = await call(params);
		assert.equal(reply.details.event.payload.state, "frozen");
		assert.equal(reply.details.event.payload.publicationReason, params.reason);
		const artifact = f.b.snapshot().artifacts.result[0];
		assert.equal(dirname(artifact.path!), realpathSync(join(f.dir, "store")));
		assert.equal(readFileSync(artifact.path!, "utf8"), "tested output");
		assert.deepEqual(readdirSync(f.dir).sort(), ["output.txt", "store"]);
		assert.equal((await call(params)).details.replayed, true);
		await assert.rejects(
			call({ ...params, idempotencyKey: "wrong-version" }),
			/Publication allocates version 2/,
		);
		const next = await call({ ...params, version: undefined, idempotencyKey: "next" });
		assert.equal(next.details.event.payload.version, 2);
		const restored = CoordinationBroker.restore(f.entries, "s", "b");
		assert.equal(restored.snapshot().artifacts.result[0].publicationReason, params.reason);
	} finally {
		f.cleanup();
	}
});

it("exposes no free-text message tool and tells runs not to duplicate their final summary", () => {
	const names: string[] = [];
	const pi: any = {
		registerTool(t: any) {
			names.push(t.name);
		},
	};
	registerCoordinationTools(pi, (() => ({})) as any, () => true);
	assert.equal(names.includes("subagent_message"), false);
	assert.equal(names.includes("subagent_signal"), true);
	const f = fixture();
	try {
		const prompt = buildCoordinationBootstrap(f.b.snapshot(), worker.roleId, 0, "fast");
		assert.match(prompt, /do not .*duplicate final summaries/i);
		assert.match(prompt, /finish your final summary and exit/);
	} finally {
		f.cleanup();
	}
});

it("successful process exit moves submitted work to explicit pending review, never fabricates approval", () => {
	const f = fixture();
	try {
		submitted(f.b, f.sourcePath);
		const before = f.b.snapshot().seq;
		const params = {
			actor: parent,
			roleId: worker.roleId,
			runId: worker.runId,
			outcome: "finished" as const,
			idempotencyKey: "detach",
		};
		f.b.detachRole(params);
		assert.equal(f.b.snapshot().seq, before + 1, "detach and reconciliation are one event");
		const task = f.b.snapshot().tasks.task;
		assert.equal(task.state, "review");
		assert.equal(task.reviewSatisfied, false);
		assert.match(task.nextActions[0], /Await coordinator review/);
		const operational = buildOperationalView(f.b.snapshot(), parent.roleId);
		assert.equal(operational.tasks[0].pendingAction, "awaiting review");
		assert.equal(operational.tasks[0].ownerOnline, false);
		assert.equal(f.b.detachRole(params).replayed, true);
		const report = formatRunTaskOutcome(f.b.snapshot(), worker.roleId, worker.runId);
		assert.match(report, /awaiting coordinator review/);
		assert.match(report, /expectedRevision=3/);
		assert.equal(
			CoordinationBroker.restore(f.entries, "s", "b").snapshot().tasks.task.state,
			"review",
		);
		const approved = f.b.reviewTask({
			actor: parent,
			taskId: "task",
			expectedRevision: 3,
			decision: "approved",
			reason: "Independent verification",
			nextActions: [],
			idempotencyKey: "approve",
		});
		assert.equal(approved.event.payload.state, "completed");
		assert.equal(approved.event.payload.reviewSatisfied, true);
		assert.deepEqual(approved.event.payload.nextActions, []);
	} finally {
		f.cleanup();
	}
});

it("detach blocks unfinished work, preserves handoffs and cannot detach a replacement generation", () => {
	const f = fixture();
	try {
		f.b.createTask({
			actor: parent,
			owner: worker.roleId,
			id: "unfinished",
			title: "Work",
			start: true,
			idempotencyKey: "create",
		});
		f.b.createTask({
			actor: parent,
			owner: parent.roleId,
			id: "other",
			title: "Other owner",
			start: true,
			idempotencyKey: "other",
		});
		f.b.rebindRole({
			actor: parent,
			roleId: worker.roleId,
			runId: "worker-2",
			idempotencyKey: "rebind",
		});
		assert.throws(
			() =>
				f.b.detachRole({
					actor: parent,
					roleId: worker.roleId,
					runId: worker.runId,
					outcome: "finished",
					idempotencyKey: "late-watcher",
				}),
			{ code: "STALE_GENERATION" },
		);
		assert.equal(f.b.role(worker.roleId)?.active, true);
		f.b.detachRole({
			actor: parent,
			roleId: worker.roleId,
			runId: "worker-2",
			outcome: "failed",
			idempotencyKey: "detach",
		});
		assert.equal(f.b.snapshot().tasks.unfinished.state, "blocked");
		assert.equal(f.b.snapshot().tasks.other.state, "in_progress");
		assert.match(
			formatRunTaskOutcome(f.b.snapshot(), worker.roleId, "worker-2"),
			/Resume or hand off/,
		);
	} finally {
		f.cleanup();
	}
});

it("exit reconciliation uses bounded patches without duplicating evidence or dropping next actions", () => {
	const f = fixture();
	try {
		const nextActions = Array.from({ length: 100 }, (_, i) => `${i}: ${"x".repeat(900)}`);
		for (const id of ["large-a", "large-b"])
			f.b.createTask({
				actor: parent,
				owner: worker.roleId,
				id,
				title: id,
				nextActions,
				start: true,
				idempotencyKey: id,
			});
		const result = f.b.detachRole({
			actor: parent,
			roleId: worker.roleId,
			runId: worker.runId,
			outcome: "finished",
			idempotencyKey: "detach",
		});
		assert.ok(JSON.stringify(result.event.payload).length < 2000);
		assert.deepEqual(f.b.snapshot().tasks["large-a"].nextActions, nextActions);
		assert.equal(f.b.snapshot().tasks["large-b"].state, "blocked");
		assert.equal(
			CoordinationBroker.restore(f.entries, "s", "b").snapshot().tasks["large-b"].state,
			"blocked",
		);
	} finally {
		f.cleanup();
	}
});

it("fast approval still requires explicit next actions, reviewer authority, settled gates and current bytes", () => {
	const f = fixture();
	try {
		submitted(f.b, f.sourcePath);
		const approve = {
			actor: parent,
			taskId: "task",
			expectedRevision: 2,
			decision: "approved" as const,
			reason: "Reviewed",
			idempotencyKey: "approve",
		};
		assert.throws(() => f.b.reviewTask(approve), /explicit nextActions/);
		assert.throws(() => f.b.reviewTask({ ...approve, actor: worker, nextActions: [] }), {
			code: "UNAUTHORIZED",
		});
		const hold = f.b.emitSignal({
			actor: parent,
			kind: "hold",
			severity: "blocking",
			summary: "Hold integration",
			targets: [{ taskId: "task" }],
			idempotencyKey: "hold",
		}).event.payload;
		assert.throws(() => f.b.reviewTask({ ...approve, nextActions: [] }), {
			code: "UNRESOLVED_BLOCKER",
		});
		f.b.resolveSignal({
			actor: parent,
			signalId: hold.id,
			expectedRevision: 1,
			idempotencyKey: "release",
		});
		// A sealed-but-newer version invalidates completion even with review approval.
		f.b.sealArtifact({
			actor: worker,
			artifactId: "task",
			sourcePath: f.sourcePath,
			idempotencyKey: "new-version",
		});
		assert.throws(() => f.b.reviewTask({ ...approve, nextActions: [] }), {
			code: "INPUT_SUPERSEDED",
		});
		assert.equal(f.b.snapshot().tasks.task.state, "submitted");
		assert.equal(f.b.snapshot().tasks.task.reviewSatisfied, false);
	} finally {
		f.cleanup();
	}
});

it("detach completes only already-approved, byte-valid work and explains missing reviewer outputs", () => {
	const f = fixture();
	try {
		submitted(f.b, f.sourcePath, "valid");
		submitted(f.b, f.sourcePath, "invalid");
		writeFileSync(f.sourcePath, "different bytes");
		submitted(f.b, f.sourcePath, "tampered");
		const tampered = f.b.snapshot().artifacts.tampered[0].path!;
		chmodSync(tampered, 0o600);
		writeFileSync(tampered, "changed after sealing");
		for (const id of ["valid", "invalid", "tampered"])
			f.b.transitionTask({
				actor: parent,
				taskId: id,
				state: "review",
				expectedRevision: 2,
				reviewSatisfied: true,
				idempotencyKey: `review-${id}`,
			});
		// A new version makes the old reviewed output non-current without changing bytes.
		writeFileSync(f.sourcePath, "tested output");
		f.b.sealArtifact({
			actor: worker,
			artifactId: "invalid",
			sourcePath: f.sourcePath,
			freeze: true,
			idempotencyKey: "superseding-version",
		});
		f.b.createTask({
			actor: parent,
			owner: worker.roleId,
			id: "report",
			title: "Review report",
			start: true,
			idempotencyKey: "report",
		});
		f.b.transitionTask({
			actor: worker,
			taskId: "report",
			expectedRevision: 1,
			state: "submitted",
			verification: ["Reviewed files"],
			idempotencyKey: "report-submit",
		});
		f.b.detachRole({
			actor: parent,
			roleId: worker.roleId,
			runId: worker.runId,
			outcome: "finished",
			idempotencyKey: "detach",
		});
		assert.equal(f.b.snapshot().tasks.valid.state, "completed");
		assert.equal(f.b.snapshot().tasks.invalid.state, "review");
		assert.match(f.b.snapshot().tasks.invalid.nextActions[0], /Completion blocked/);
		assert.equal(f.b.snapshot().tasks.tampered.state, "review");
		assert.match(f.b.snapshot().tasks.tampered.nextActions[0], /Completion blocked/);
		assert.equal(f.b.snapshot().tasks.report.reviewSatisfied, false);
		assert.match(
			formatRunTaskOutcome(f.b.snapshot(), worker.roleId, worker.runId),
			/Attach current frozen outputs/,
		);
	} finally {
		f.cleanup();
	}
});
