import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { buildCoordinationBootstrap } from "../src/coordination/bootstrap.ts";
import { CoordinationBroker, hasLiveCoordinationRuns } from "../src/coordination/broker.ts";
import {
	loadOptionalCoordinationConfig,
	parseCoordinationConfig,
} from "../src/coordination/config.ts";
import { CoordinationEventLog } from "../src/coordination/event-log.ts";
import {
	computeCharacterizationMetrics,
	computeReplayMetrics,
	loadCharacterizationFixture,
} from "../src/coordination/metrics.ts";
import { buildOperationalView } from "../src/coordination/operational.ts";
import { registerCoordinationTools } from "../src/coordination/tools.ts";
import { TransportBroker, TransportClient } from "../src/transport.ts";
import { runCharacterizationReplay } from "./fixtures/coordination/replay.ts";

const coordinator = { roleId: "coordinator", runId: "coordinator", generation: 1 };
const worker = { roleId: "worker", runId: "run-worker-1", generation: 1 };
const reviewer = { roleId: "reviewer", runId: "run-reviewer-1", generation: 1 };

function broker() {
	const b = new CoordinationBroker("s", "b");
	b.admitRole({
		roleId: "coordinator",
		capabilities: ["coordinator", "worker", "reviewer", "evaluator"],
		idempotencyKey: "admit:coord",
	});
	b.bindRole({
		roleId: "coordinator",
		runId: "coordinator",
		generation: 1,
		capabilities: ["coordinator", "worker", "reviewer", "evaluator"],
		idempotencyKey: "bind:coord",
	});
	b.admitRole({
		actor: coordinator,
		roleId: "worker",
		capabilities: ["worker"],
		idempotencyKey: "admit:worker",
	});
	b.bindRole({
		roleId: "worker",
		runId: "run-worker-1",
		generation: 1,
		capabilities: ["worker"],
		idempotencyKey: "bind:worker",
	});
	b.admitRole({
		actor: coordinator,
		roleId: "reviewer",
		capabilities: ["reviewer"],
		idempotencyKey: "admit:reviewer",
	});
	b.bindRole({
		roleId: "reviewer",
		runId: "run-reviewer-1",
		generation: 1,
		capabilities: ["reviewer"],
		idempotencyKey: "bind:reviewer",
	});
	return b;
}

describe("coordination characterization and config", () => {
	it("committed fixture captures prose-coordination failure metrics and typed target outcomes", () => {
		const fixture = loadCharacterizationFixture("test/fixtures/coordination/reviewed-session.json");
		const metrics = computeCharacterizationMetrics(fixture);
		assert.equal(
			metrics.invalidProtectedDispatches,
			fixture.expectedV1Metrics.invalidProtectedDispatches,
		);
		assert.equal(metrics.fakeChannelPrefixes, fixture.expectedV1Metrics.fakeChannelPrefixes);
		const replay = runCharacterizationReplay(fixture);
		const current = computeReplayMetrics(replay.observations);
		assert.equal(
			replay.observations.filter((event) => event.kind === "dispatch" && event.valid).length,
			1,
			"Valid protected work must actually dispatch",
		);
		assert.deepEqual(
			replay.rejected.map((event) => event.time),
			[2, 6, 9, 16, 18],
		);
		assert.equal(replay.projection.budgets["gpu-calls"].used, 1);
		assert.equal(replay.projection.budgets["gpu-calls"].dispatchedUnknown, 1);
		assert.equal(replay.projection.channels[replay.reviewChannel].revision, 2);
		assert.equal(current.invalidProtectedDispatches, 0);
		assert.equal(current.fakeChannelPrefixes, 0);
		assert.ok(current.parentRelays <= metrics.parentRelays * 0.3);
		assert.ok(current.wakeCount <= metrics.wakeCount * 0.4);
		assert.equal(current.channelRecreationsNeededForMembership, 0);
		assert.equal(fixture.expectedOutcomes.membershipChangeRequiresRecreate, false);
	});
	it("applies the workflow-mode environment override with no config file present", () => {
		// config.json is gitignored, so it is absent in CI and in a fresh clone. The
		// override used to apply only on the file-reading path, which silently left a
		// run in fast mode when it asked for strict.
		const previous = process.env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE;
		const missing = join(mkdtempSync(join(tmpdir(), "coord-config-")), "absent.json");
		try {
			process.env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE = "strict";
			const config = loadOptionalCoordinationConfig(missing);
			assert.equal(config.workflowMode, "strict");
			assert.ok(config.protectedOperations.includes("shell"), "strict protects shell");

			delete process.env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE;
			assert.equal(loadOptionalCoordinationConfig(missing).workflowMode, "fast");
		} finally {
			if (previous === undefined) delete process.env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE;
			else process.env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE = previous;
		}
	});

	it("parses the coordination schema strictly and rejects unknown settings", () => {
		const parsed = parseCoordinationConfig({
			coordination: {
				maxEvents: 500,
				maxDigestLines: 10,
				acknowledgementDeadlineMs: 2000,
				protectedOperations: ["gpu"],
				defaultDelivery: "urgent",
			},
		});
		assert.equal(parsed.maxEvents, 500);
		assert.deepEqual(parsed.protectedOperations, ["gpu"]);
		assert.equal(parsed.defaultDelivery, "urgent");
		assert.throws(
			() => parseCoordinationConfig({ coordination: { protocolVersion: 2 } }),
			/Unknown/,
		);
		assert.throws(() => parseCoordinationConfig({ coordination: { unknown: true } }), /Unknown/);
		assert.throws(() => parseCoordinationConfig({ coordination: { maxEvents: 5 } }), /maxEvents/);
	});
});

describe("coordination reducers, gates, artifacts, permits", () => {
	it("enforces idempotency, role generations, task start gates, and completion evidence", () => {
		const b = broker();
		const first = b.createTask({
			actor: coordinator,
			id: "task-protected",
			owner: "worker",
			title: "Protected task",
			idempotencyKey: "task:1",
		});
		const replay = b.createTask({
			actor: coordinator,
			id: "task-protected",
			owner: "worker",
			title: "Protected task",
			idempotencyKey: "task:1",
		});
		assert.equal(replay.replayed, true);
		assert.throws(
			() =>
				b.createTask({
					actor: coordinator,
					owner: "worker",
					title: "Different",
					idempotencyKey: "task:1",
				}),
			/Idempotency/,
		);
		const task: any = first.event.payload;
		b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: 1,
			state: "ready",
			idempotencyKey: "ready:no-gates",
		});
		assert.equal(b.snapshot().tasks[task.id].state, "ready");
		assert.throws(
			() =>
				b.transitionTask({
					actor: worker,
					taskId: task.id,
					expectedRevision: 2,
					state: "in_progress",
					idempotencyKey: "start:blocked",
				}),
			(error: any) =>
				error.code === "LEASE_REQUIRED" &&
				error.current.revision === 2 &&
				error.current.state === "ready",
		);
		assert.throws(
			() =>
				b.transitionTask({
					actor: worker,
					taskId: task.id,
					expectedRevision: 2,
					state: "completed",
					idempotencyKey: "complete:no-evidence",
				}),
			/Cannot transition|Completion requires/,
		);
		b.rebindRole({
			actor: coordinator,
			roleId: "worker",
			runId: "run-worker-2",
			idempotencyKey: "rebind",
		});
		assert.throws(
			() =>
				b.emitSignal({
					actor: worker,
					kind: "attention",
					severity: "info",
					summary: "late old generation",
					idempotencyKey: "late",
				}),
			/generation/,
		);
	});

	it("seals/freeze artifacts, detects consume-time tampering, and blocks stale inputs", () => {
		const b = broker();
		const dir = mkdtempSync(join(tmpdir(), "coord-"));
		const source = join(dir, "input.txt");
		writeFileSync(source, "v1");
		const sealed: any = b.sealArtifact({
			projection: b.snapshot(),
			actor: worker,
			artifactId: "pack",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "seal:1",
		}).event.payload;
		assert.equal(sealed.state, "sealed");
		const frozen: any = b.freezeArtifact({
			actor: reviewer,
			artifactId: "pack",
			version: 1,
			idempotencyKey: "freeze:1",
		}).event.payload;
		assert.equal(frozen.state, "frozen");
		chmodSync(frozen.path, 0o600);
		writeFileSync(frozen.path, "tampered");
		b.createTask({
			actor: coordinator,
			owner: "worker",
			title: "Use pack",
			requiredInputs: [{ artifactId: "pack", version: 1, digest: frozen.digest }],
			idempotencyKey: "task:pack",
		});
		b.createBudget({ actor: coordinator, budgetId: "gpu", limit: 1, idempotencyKey: "budget" });
		const budget: any = b.reserveBudget({
			actor: coordinator,
			budgetId: "gpu",
			idempotencyKey: "reserve",
		}).event.payload;
		const task: any = Object.values(b.snapshot().tasks)[0];
		const withLease: any = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: task.revision,
			state: "ready",
			leases: [Object.keys(budget.reservations)[0]],
			idempotencyKey: "lease-bind",
		}).event.payload;
		const started: any = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: withLease.revision,
			state: "in_progress",
			idempotencyKey: "started",
		}).event.payload;
		assert.throws(
			() =>
				b.issuePermit({
					actor: worker,
					taskId: started.id,
					budgetId: "gpu",
					reservationId: Object.keys(budget.reservations)[0],
					operation: "gpu",
					idempotencyKey: "permit",
				}),
			/tamper/i,
		);
	});

	it("reserves budgets atomically, issues single-use permits, and preserves unknown dispatch charge", () => {
		const b = broker();
		const dir = mkdtempSync(join(tmpdir(), "coord-"));
		const source = join(dir, "input.txt");
		writeFileSync(source, "v1");
		const _sealed: any = b.sealArtifact({
			projection: b.snapshot(),
			actor: worker,
			artifactId: "pack",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "seal",
		}).event.payload;
		const frozen: any = b.freezeArtifact({
			actor: reviewer,
			artifactId: "pack",
			version: 1,
			idempotencyKey: "freeze",
		}).event.payload;
		b.createBudget({ actor: coordinator, budgetId: "gpu", limit: 1, idempotencyKey: "budget" });
		const reserved: any = b.reserveBudget({
			actor: coordinator,
			budgetId: "gpu",
			idempotencyKey: "reserve",
		}).event.payload;
		assert.throws(
			() => b.reserveBudget({ actor: coordinator, budgetId: "gpu", idempotencyKey: "reserve2" }),
			/overspent|exhausted/i,
		);
		const reservationId = Object.keys(reserved.reservations)[0];
		const task: any = b.createTask({
			actor: coordinator,
			owner: "worker",
			title: "Run gpu",
			requiredInputs: [{ artifactId: "pack", version: 1, digest: frozen.digest }],
			idempotencyKey: "task",
		}).event.payload;
		const ready: any = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: task.revision,
			state: "ready",
			leases: [reservationId],
			idempotencyKey: "ready",
		}).event.payload;
		const started: any = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: ready.revision,
			state: "in_progress",
			idempotencyKey: "start",
		}).event.payload;
		const permit: any = b.issuePermit({
			actor: worker,
			taskId: started.id,
			budgetId: "gpu",
			reservationId,
			operation: "gpu",
			idempotencyKey: "permit",
		}).event.payload;
		const dispatched: any = b.dispatchPermit({
			actor: worker,
			permitId: permit.id,
			outcomeUnknown: true,
			idempotencyKey: "dispatch",
		}).event.payload;
		assert.equal(dispatched.state, "outcome_unknown");
		assert.equal(b.snapshot().budgets.gpu.used, 1);
		assert.throws(
			() =>
				b.dispatchPermit({ actor: worker, permitId: permit.id, idempotencyKey: "dispatch-again" }),
			/not usable|single-use/i,
		);
	});

	it("supports mutable channel membership, delivery states, digest bounds, redaction, and bootstrap", () => {
		const b = broker();
		const channel: any = b.createChannel({
			actor: coordinator,
			name: "review",
			purpose: "review findings",
			members: [{ roleId: "worker", mode: "participate" }],
			idempotencyKey: "chan",
		}).event.payload;
		const updated: any = b.updateChannel({
			actor: coordinator,
			channelId: channel.id,
			expectedRevision: 1,
			members: [...channel.members, { roleId: "reviewer", mode: "watch" }],
			idempotencyKey: "chan:update",
		}).event.payload;
		assert.equal(updated.revision, 2);
		const signal: any = b.emitSignal({
			actor: coordinator,
			kind: "blocked",
			severity: "critical",
			summary: "Need ack",
			requiresAck: ["worker"],
			targets: [{ roleId: "worker" }],
			idempotencyKey: "sig",
		}).event.payload;
		const delivery: any = b.queueSignalDelivery({
			actor: coordinator,
			signalId: signal.id,
			targetRoleId: "worker",
			idempotencyKey: "delivery",
		}).event.payload;
		assert.equal(delivery.state, "queued");
		assert.equal(delivery.wakeCount, 1);
		const delivered: any = b.receipt({
			actor: worker,
			deliveryId: delivery.id,
			state: "delivered",
			idempotencyKey: "delivered",
		}).event.payload;
		assert.equal(delivered.state, "delivered");
		const ack: any = b.acknowledgeSignal({
			actor: worker,
			signalId: signal.id,
			expectedRevision: 1,
			idempotencyKey: "ack",
		}).event.payload;
		assert.equal(ack.acknowledgements.length, 1);
		assert.ok(b.digest(worker, 0).lines.length <= 20);
		const boot = buildCoordinationBootstrap(b.snapshot(), "worker");
		assert.match(boot, /roleId=worker/);
		assert.match(boot, /Do not start|do not start/i);
	});

	it("restores only the same session active branch offline and fails closed on corruption", () => {
		const entries: any[] = [];
		const b = new CoordinationBroker("session-a", "leaf-a", {
			appendEntry: (customType, data) => entries.push({ customType, data }),
		});
		b.admitRole({ roleId: "coordinator", capabilities: ["coordinator"], idempotencyKey: "a" });
		b.bindRole({
			roleId: "coordinator",
			runId: "run-a",
			generation: 1,
			capabilities: ["coordinator"],
			idempotencyKey: "b",
		});
		const coord = { roleId: "coordinator", runId: "run-a", generation: 1 };
		b.admitRole({
			actor: coord,
			roleId: "private",
			capabilities: ["worker"],
			private: true,
			idempotencyKey: "c",
		});
		b.bindRole({ roleId: "private", runId: "private-run", generation: 1, idempotencyKey: "d" });
		b.checkpoint({ actor: coord, idempotencyKey: "checkpoint" });
		assert.equal(
			hasLiveCoordinationRuns(b.snapshot()),
			true,
			"tree navigation must be rejected while child runs are live",
		);
		const restored = CoordinationEventLog.restore(entries, "session-a", "new-leaf");
		assert.equal(restored.online, false);
		assert.equal(restored.roles.private.active, false);
		assert.equal(restored.roles.private.runId, undefined);
		assert.equal(hasLiveCoordinationRuns(restored), false);
		assert.equal(
			Object.keys(CoordinationEventLog.restore(entries, "session-fork", "fork-leaf").roles).length,
			0,
		);
		const corrupt = structuredClone(entries);
		corrupt.push({
			customType: "subagent-coordination-event",
			data: { sessionId: "session-a", event: { protocolVersion: 2 } },
		});
		assert.throws(
			() => CoordinationEventLog.restore(corrupt, "session-a", "leaf"),
			{ code: "CHECKPOINT_CORRUPT" },
			"a corrupt tail might contain a charged dispatch and must not be discarded",
		);
		const corruptPrefix = structuredClone(entries);
		corruptPrefix[0].data.event.payload = { invalid: true };
		const recovered = CoordinationEventLog.restore(corruptPrefix, "session-a", "leaf");
		assert.equal(recovered.seq, restored.seq);
		assert.equal(recovered.online, false);
		assert.throws(
			() =>
				CoordinationEventLog.restore(
					[
						{
							customType: "subagent-coordination-event",
							data: { sessionId: "session-a", event: { protocolVersion: 2 } },
						},
					],
					"session-a",
					"leaf",
				),
			/failed closed/,
		);
	});

	it("rejects symlink sealing and holds pending consumers after supersession", () => {
		const b = broker();
		const dir = mkdtempSync(join(tmpdir(), "coord-super-"));
		const source = join(dir, "source");
		writeFileSync(source, "one");
		symlinkSync(source, join(dir, "link"));
		assert.throws(
			() =>
				b.sealArtifact({
					actor: worker,
					artifactId: "x",
					sourcePath: join(dir, "link"),
					storeDir: join(dir, "store"),
					idempotencyKey: "symlink",
				}),
			/non-symlink/,
		);
		const one: any = b.sealArtifact({
			actor: worker,
			artifactId: "x",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "one",
		}).event.payload;
		const frozen: any = b.freezeArtifact({
			actor: reviewer,
			artifactId: "x",
			version: one.version,
			idempotencyKey: "freeze-one",
		}).event.payload;
		const task: any = b.createTask({
			actor: coordinator,
			owner: "worker",
			title: "consumer",
			requiredInputs: [{ artifactId: "x", version: 1, digest: frozen.digest }],
			idempotencyKey: "consumer",
		}).event.payload;
		writeFileSync(source, "two");
		const two: any = b.sealArtifact({
			actor: worker,
			artifactId: "x",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "two",
		}).event.payload;
		b.supersedeArtifact({
			actor: reviewer,
			artifactId: "x",
			previousVersion: 1,
			successorVersion: two.version,
			policy: "cancel",
			idempotencyKey: "supersede",
		});
		assert.equal(b.snapshot().tasks[task.id].state, "blocked");
	});

	it("rejects unauthorized GO, freeze, resolve, override, and confidential queries", () => {
		const b = broker();
		assert.throws(
			() =>
				b.emitSignal({
					actor: worker,
					kind: "go",
					severity: "critical",
					summary: "unauthorized go",
					idempotencyKey: "acl:go",
				}),
			/lacks|permission|unauthorized/i,
		);
		const blocked: any = b.emitSignal({
			actor: coordinator,
			kind: "blocked",
			severity: "blocking",
			summary: "block",
			idempotencyKey: "acl:block",
		}).event.payload;
		assert.throws(
			() =>
				b.resolveSignal({
					actor: worker,
					signalId: blocked.id,
					expectedRevision: 1,
					idempotencyKey: "acl:resolve",
				}),
			/lacks|permission/i,
		);
		const dir = mkdtempSync(join(tmpdir(), "coord-acl-"));
		const source = join(dir, "input");
		writeFileSync(source, "x");
		const one: any = b.sealArtifact({
			actor: worker,
			artifactId: "acl",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "acl:seal1",
		}).event.payload;
		assert.throws(
			() =>
				b.freezeArtifact({
					actor: worker,
					artifactId: "acl",
					version: one.version,
					idempotencyKey: "acl:freeze",
				}),
			/lacks|permission/i,
		);
		const frozen: any = b.freezeArtifact({
			actor: reviewer,
			artifactId: "acl",
			version: one.version,
			idempotencyKey: "acl:freeze-review",
		}).event.payload;
		writeFileSync(source, "y");
		const two: any = b.sealArtifact({
			actor: worker,
			artifactId: "acl",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "acl:seal2",
		}).event.payload;
		assert.throws(
			() =>
				b.supersedeArtifact({
					actor: worker,
					artifactId: "acl",
					previousVersion: frozen.version,
					successorVersion: two.version,
					policy: "audited_override",
					idempotencyKey: "acl:override",
				}),
			/lacks|permission/i,
		);
	});

	it("fences stale receipts and cached idempotent results before replay", () => {
		const b = broker();
		const signal: any = b.emitSignal({
			actor: worker,
			kind: "attention",
			severity: "critical",
			summary: "critical",
			targets: [{ roleId: "worker" }],
			idempotencyKey: "critical",
		}).event.payload;
		const delivery: any = b.queueSignalDelivery({
			actor: coordinator,
			signalId: signal.id,
			targetRoleId: "worker",
			idempotencyKey: "q",
		}).event.payload;
		b.rebindRole({
			actor: coordinator,
			roleId: "worker",
			runId: "run-worker-2",
			idempotencyKey: "rebind-worker",
		});
		assert.throws(
			() =>
				b.receipt({
					actor: worker,
					deliveryId: delivery.id,
					state: "delivered",
					idempotencyKey: "receipt",
				}),
			/generation/,
		);
		assert.throws(
			() =>
				b.emitSignal({
					actor: worker,
					kind: "attention",
					severity: "info",
					summary: "old",
					idempotencyKey: "critical",
				}),
			/generation/,
		);
	});

	it("redacts confidential data and builds bounded dashboard/TUI projections", () => {
		const b = broker();
		const dir = mkdtempSync(join(tmpdir(), "coord-private-"));
		const source = join(dir, "secret");
		writeFileSync(source, "secret");
		b.sealArtifact({
			actor: worker,
			artifactId: "secret",
			sourcePath: source,
			storeDir: join(dir, "store"),
			privateTo: ["reviewer"],
			idempotencyKey: "secret",
		});
		const workerView = b.snapshot("worker");
		assert.equal(workerView.artifacts.secret, undefined);
		assert.deepEqual(workerView.idempotency, {});
		const ops = buildOperationalView(workerView, "worker", 2);
		assert.ok(ops.audit.length <= 2);
		assert.equal(JSON.stringify(ops).includes("/var/"), false);
	});

	it("applies active-consumer invalidation, preserves dispatched ownership/charge, and retries delivery without duplicate wakes", () => {
		const b = broker();
		const dir = mkdtempSync(join(tmpdir(), "coord-active-"));
		const source = join(dir, "input");
		writeFileSync(source, "v1");
		const _sealed: any = b.sealArtifact({
			actor: worker,
			artifactId: "input",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "active:seal1",
		}).event.payload;
		const frozen: any = b.freezeArtifact({
			actor: reviewer,
			artifactId: "input",
			version: 1,
			idempotencyKey: "active:freeze",
		}).event.payload;
		b.createBudget({
			actor: coordinator,
			budgetId: "active-budget",
			limit: 1,
			idempotencyKey: "active:budget",
		});
		const budget: any = b.reserveBudget({
			actor: coordinator,
			budgetId: "active-budget",
			idempotencyKey: "active:reserve",
		}).event.payload;
		const reservationId = Object.keys(budget.reservations)[0];
		const task: any = b.createTask({
			actor: coordinator,
			owner: "worker",
			title: "active consumer",
			requiredInputs: [{ artifactId: "input", version: 1, digest: frozen.digest }],
			idempotencyKey: "active:task",
		}).event.payload;
		const ready: any = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: 1,
			state: "ready",
			leases: [reservationId],
			idempotencyKey: "active:ready",
		}).event.payload;
		const _started: any = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: ready.revision,
			state: "in_progress",
			idempotencyKey: "active:start",
		}).event.payload;
		const permit: any = b.issuePermit({
			actor: worker,
			taskId: task.id,
			budgetId: "active-budget",
			reservationId,
			operation: "gpu",
			idempotencyKey: "active:permit",
		}).event.payload;
		b.dispatchPermit({ actor: worker, permitId: permit.id, idempotencyKey: "active:dispatch" });
		b.rebindRole({
			actor: coordinator,
			roleId: "worker",
			runId: "run-worker-2",
			idempotencyKey: "active:rebind",
		});
		assert.equal(b.snapshot().tasks[task.id].activeRunId, "run-worker-1");
		assert.equal(b.snapshot().budgets["active-budget"].used, 1);
		writeFileSync(source, "second");
		const successor: any = b.sealArtifact({
			actor: { roleId: "worker", runId: "run-worker-2", generation: 2 },
			artifactId: "input",
			sourcePath: source,
			storeDir: join(dir, "store"),
			idempotencyKey: "active:seal2",
		}).event.payload;
		b.supersedeArtifact({
			actor: reviewer,
			artifactId: "input",
			previousVersion: 1,
			successorVersion: successor.version,
			policy: "finish_as_invalid",
			idempotencyKey: "active:supersede",
		});
		assert.equal(b.snapshot().tasks[task.id].state, "failed");
		const signal: any = b.emitSignal({
			actor: coordinator,
			kind: "attention",
			severity: "critical",
			summary: "retry",
			targets: [{ roleId: "reviewer" }],
			idempotencyKey: "retry:signal",
		}).event.payload;
		b.queueSignalDelivery({
			actor: coordinator,
			signalId: signal.id,
			targetRoleId: "reviewer",
			idempotencyKey: "retry:queue",
		});
		let attempts = 0;
		b.setDeliverySink(() => {
			attempts++;
		});
		b.retryPendingDeliveries("reviewer");
		const delivery = Object.values(b.snapshot().deliveries).find((d) => d.signalId === signal.id)!;
		assert.equal(delivery.wakeCount, 1);
		assert.ok(attempts >= 2);
		b.receipt({
			actor: reviewer,
			deliveryId: delivery.id,
			state: "delivered",
			idempotencyKey: "retry:delivered",
		});
		const beforeRoutine = attempts;
		const routine: any = b.emitSignal({
			actor: coordinator,
			kind: "attention",
			severity: "info",
			summary: "digest only",
			targets: [{ roleId: "reviewer" }],
			idempotencyKey: "routine:signal",
		}).event.payload;
		b.queueSignalDelivery({
			actor: coordinator,
			signalId: routine.id,
			targetRoleId: "reviewer",
			idempotencyKey: "routine:queue",
		});
		b.retryPendingDeliveries("reviewer");
		assert.equal(
			attempts,
			beforeRoutine,
			"routine events must wait for a digest instead of waking a model",
		);
	});

	it("derives socket actors from authenticated transport and ignores spoofed payload actors", async () => {
		const coordination = broker();
		const socket = new TransportBroker(
			() => {},
			undefined,
			undefined,
			(from, method, params) => {
				const role = Object.values(coordination.snapshot().roles).find(
					(candidate) => candidate.runId === from && candidate.active,
				)!;
				return (coordination as any)[method]({
					...params,
					actor: { roleId: role.roleId, runId: from, generation: role.generation },
				});
			},
		);
		await socket.start();
		const client = new TransportClient(
			socket.add("run-worker-1", "worker", "/worker", false),
			() => {},
		);
		await client.ready;
		try {
			const result: any = await client.request({
				action: "coordination",
				method: "emitSignal",
				params: {
					actor: coordinator,
					kind: "attention",
					severity: "info",
					summary: "authenticated",
					idempotencyKey: "socket-auth",
				},
			});
			assert.equal(result.event.actor.roleId, "worker");
			assert.equal(result.event.actor.runId, "run-worker-1");
		} finally {
			client.close();
			socket.close();
		}
	});

	it("tool schemas derive actors from transport instead of request payloads", () => {
		const tools: any[] = [];
		const fakePi: any = { registerTool: (tool: any) => tools.push(tool) };
		registerCoordinationTools(
			fakePi,
			() => ({ broker: broker(), actor: coordinator }),
			() => true,
		);
		assert.equal(tools.length, 4);
		for (const tool of tools)
			assert.equal(Object.hasOwn(tool.parameters.properties, "actor"), false);
	});
});
