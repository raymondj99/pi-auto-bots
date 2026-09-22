import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { registerCoordinationTools } from "../src/coordination/tools.ts";

function setup() {
	const b = new CoordinationBroker("session", "branch");
	const coordinator = { roleId: "coordinator", runId: "root", generation: 1 };
	const worker = { roleId: "worker", runId: "child", generation: 1 };
	b.admitRole({
		roleId: coordinator.roleId,
		capabilities: ["coordinator"],
		idempotencyKey: "root-admit",
	});
	b.bindRole({ roleId: coordinator.roleId, runId: coordinator.runId, idempotencyKey: "root-bind" });
	b.admitRole({
		actor: coordinator,
		roleId: worker.roleId,
		capabilities: ["worker"],
		idempotencyKey: "worker-admit",
	});
	b.bindRole({ roleId: worker.roleId, runId: worker.runId, idempotencyKey: "worker-bind" });
	return { b, coordinator, worker };
}

it("signal supersession atomically replaces blocker references and resets acknowledgements", () => {
	const { b, coordinator, worker } = setup();
	const task = b.createTask({
		actor: coordinator,
		owner: worker.roleId,
		title: "Blocked work",
		idempotencyKey: "task",
	}).event.payload;
	const signal = b.emitSignal({
		actor: coordinator,
		kind: "hold",
		severity: "critical",
		summary: "Old contract",
		targets: [{ taskId: task.id }],
		requiresAck: [worker.roleId],
		idempotencyKey: "hold",
	}).event.payload;
	b.transitionTask({
		actor: worker,
		taskId: task.id,
		expectedRevision: 1,
		state: "blocked",
		blockers: [signal.id],
		idempotencyKey: "block",
	});
	assert.throws(
		() =>
			b.supersedeSignal({
				actor: worker,
				signalId: signal.id,
				expectedRevision: 1,
				summary: "Unapproved change",
				idempotencyKey: "unauthorized",
			}),
		{ code: "UNAUTHORIZED" },
	);
	const before = b.snapshot().seq;
	const replacement = b.supersedeSignal({
		actor: coordinator,
		signalId: signal.id,
		expectedRevision: 1,
		summary: "Corrected contract",
		idempotencyKey: "correct",
	}).event.payload;
	const p = b.snapshot();
	assert.equal(p.seq, before + 1);
	assert.equal(p.signals[signal.id].state, "superseded");
	assert.equal(p.signals[replacement.id].supersedes, signal.id);
	assert.deepEqual(p.signals[replacement.id].acknowledgements, []);
	assert.deepEqual(p.tasks[task.id].blockers, [replacement.id]);
	assert.equal(p.tasks[task.id].revision, 3);
	assert.throws(
		() =>
			b.acknowledgeSignal({
				actor: worker,
				signalId: signal.id,
				expectedRevision: 2,
				idempotencyKey: "obsolete-ack",
			}),
		{ code: "INVALID_TRANSITION" },
	);
	b.acknowledgeSignal({
		actor: worker,
		signalId: replacement.id,
		expectedRevision: 1,
		idempotencyKey: "new-ack",
	});
	b.resolveSignal({
		actor: coordinator,
		signalId: replacement.id,
		expectedRevision: 2,
		idempotencyKey: "resolve",
	});
});

it("invalidated consumers stay held until a coordinator binds verified successor inputs", () => {
	const { b, coordinator, worker } = setup();
	const directory = mkdtempSync(join(tmpdir(), "coordination-correction-"));
	try {
		const sourcePath = join(directory, "source");
		writeFileSync(sourcePath, "old");
		const old = b.sealArtifact({
			actor: worker,
			artifactId: "input",
			sourcePath,
			storeDir: join(directory, "objects"),
			idempotencyKey: "old",
		}).event.payload;
		b.freezeArtifact({
			actor: coordinator,
			artifactId: "input",
			version: 1,
			idempotencyKey: "freeze-old",
		});
		const task = b.createTask({
			actor: coordinator,
			owner: worker.roleId,
			title: "Consumer",
			requiredInputs: [{ artifactId: "input", version: 1, digest: old.digest }],
			idempotencyKey: "task",
		}).event.payload;
		assert.deepEqual(b.snapshot().artifacts.input[0].consumers, [task.id]);
		assert.throws(
			() =>
				b.invalidateArtifact({
					actor: worker,
					artifactId: "input",
					version: 1,
					policy: "cancel",
					reason: "Not authorized",
					idempotencyKey: "invalid-worker",
				}),
			{ code: "UNAUTHORIZED" },
		);
		b.invalidateArtifact({
			actor: coordinator,
			artifactId: "input",
			version: 1,
			policy: "cancel",
			reason: "Input verification failed",
			idempotencyKey: "invalidate",
		});
		assert.equal(b.snapshot().tasks[task.id].state, "blocked");
		assert.equal(b.snapshot().artifacts.input[0].state, "invalidated");
		writeFileSync(sourcePath, "corrected");
		const successor = b.sealArtifact({
			actor: worker,
			artifactId: "input",
			sourcePath,
			storeDir: join(directory, "objects"),
			idempotencyKey: "successor",
		}).event.payload;
		b.freezeArtifact({
			actor: coordinator,
			artifactId: "input",
			version: 2,
			idempotencyKey: "freeze-successor",
		});
		const requiredInputs = [{ artifactId: "input", version: 2, digest: successor.digest }];
		assert.throws(
			() =>
				b.bindTaskInputs({
					actor: worker,
					taskId: task.id,
					expectedRevision: 2,
					requiredInputs,
					reason: "Self-override",
					idempotencyKey: "worker-bind-inputs",
				}),
			{ code: "UNAUTHORIZED" },
		);
		b.bindTaskInputs({
			actor: coordinator,
			taskId: task.id,
			expectedRevision: 2,
			requiredInputs,
			reason: "Reviewed corrected version",
			idempotencyKey: "bind-inputs",
		});
		assert.equal(b.snapshot().tasks[task.id].state, "draft");
		assert.deepEqual(b.snapshot().artifacts.input[0].consumers, []);
		assert.deepEqual(b.snapshot().artifacts.input[1].consumers, [task.id]);
		const secret = b.sealArtifact({
			actor: coordinator,
			artifactId: "secret",
			sourcePath,
			storeDir: join(directory, "objects"),
			privateTo: [coordinator.roleId],
			idempotencyKey: "secret",
		}).event.payload;
		const confidential = b.emitSignal({
			actor: coordinator,
			kind: "decision",
			severity: "critical",
			summary: "Private evaluator context",
			targets: [{ roleId: worker.roleId }],
			artifactRefs: [{ artifactId: "secret", version: 1, digest: secret.digest }],
			idempotencyKey: "private-signal",
		}).event.payload;
		let wakes = 0;
		b.setDeliverySink(() => {
			wakes++;
		});
		assert.throws(
			() =>
				b.queueSignalDelivery({
					actor: coordinator,
					signalId: confidential.id,
					targetRoleId: worker.roleId,
					idempotencyKey: "private-delivery",
				}),
			{ code: "UNAUTHORIZED" },
		);
		assert.equal(wakes, 0);
		assert.equal(Object.keys(b.snapshot().deliveries).length, 0);
	} finally {
		rmSync(directory, { recursive: true, force: true });
	}
});

it("signal tools route task and channel targets with bounded derived retry keys", async () => {
	const { b, coordinator, worker } = setup();
	const definitions = new Map<string, any>();
	registerCoordinationTools(
		{
			registerTool(definition: any) {
				definitions.set(definition.name, definition);
			},
		} as any,
		() => ({ broker: b, actor: coordinator }),
		() => true,
	);
	const channel = b.createChannel({
		actor: coordinator,
		name: "team",
		purpose: "Typed routing",
		members: [
			{ roleId: coordinator.roleId, mode: "participate" },
			{ roleId: worker.roleId, mode: "participate" },
		],
		idempotencyKey: "channel",
	}).event.payload;
	const task = b.createTask({
		actor: coordinator,
		owner: worker.roleId,
		title: "Task target",
		idempotencyKey: "task",
	}).event.payload;
	let wakes = 0;
	b.setDeliverySink(() => {
		wakes++;
	});
	const params = {
		action: "emit",
		kind: "attention",
		severity: "action",
		summary: "Scoped update",
		targets: [{ channelId: channel.id }, { taskId: task.id }],
		idempotencyKey: "x".repeat(200),
	};
	await definitions.get("subagent_signal").execute("call", params);
	await definitions.get("subagent_signal").execute("retry", params);
	assert.equal(wakes, 1, "do not wake the sender with its own announcement");
	assert.equal(Object.keys(b.snapshot().deliveries).length, 1);
	assert.throws(
		() =>
			b.emitSignal({
				actor: worker,
				kind: "hold",
				severity: "critical",
				summary: "Unrelated channel",
				targets: [{ channelId: "not-a-member" }],
				idempotencyKey: "bad-channel",
			}),
		{ code: "UNAUTHORIZED" },
	);
});
