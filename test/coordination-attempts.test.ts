import assert from "node:assert/strict";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import type {
	BudgetState,
	PermitStateRecord,
	TaskStateRecord,
} from "../src/coordination/protocol.ts";

function attempt() {
	const entries: unknown[] = [];
	let failPersistence = false;
	const b = new CoordinationBroker("session", "branch", {
		appendEntry(_type, data) {
			if (failPersistence) throw new Error("disk failure");
			entries.push(data);
		},
	});
	const actor = { roleId: "coordinator", runId: "coordinator-run", generation: 1 };
	b.admitRole({ roleId: actor.roleId, capabilities: ["coordinator"], idempotencyKey: "admit" });
	b.bindRole({ roleId: actor.roleId, runId: actor.runId, idempotencyKey: "bind" });
	b.createBudget({ actor, budgetId: "gpu", limit: 2, idempotencyKey: "budget" });
	const budget = b.reserveBudget({ actor, budgetId: "gpu", idempotencyKey: "reserve" }).event
		.payload as BudgetState;
	const reservationId = Object.keys(budget.reservations)[0];
	const task = b.createTask({
		actor,
		owner: actor.roleId,
		title: "Attempt",
		idempotencyKey: "task",
	}).event.payload as TaskStateRecord;
	b.transitionTask({
		actor,
		taskId: task.id,
		state: "ready",
		expectedRevision: 1,
		leases: [reservationId],
		idempotencyKey: "ready",
	});
	b.transitionTask({
		actor,
		taskId: task.id,
		state: "in_progress",
		expectedRevision: 2,
		idempotencyKey: "start",
	});
	const permit = b.issuePermit({
		actor,
		taskId: task.id,
		budgetId: "gpu",
		reservationId,
		operation: "shell",
		idempotencyKey: "permit",
	}).event.payload as PermitStateRecord;
	return {
		b,
		actor,
		task,
		permit,
		reservationId,
		entries,
		fail: () => {
			failPersistence = true;
		},
	};
}

it("canonical retries return the original result before rechecking mutated state", () => {
	const { b, actor, task, permit, reservationId } = attempt();
	const seq = b.snapshot().seq;
	assert.equal(
		b.reserveBudget({ actor, budgetId: "gpu", idempotencyKey: "reserve" }).replayed,
		true,
	);
	assert.equal(
		b.transitionTask({
			actor,
			taskId: task.id,
			state: "ready",
			expectedRevision: 1,
			leases: [reservationId],
			idempotencyKey: "ready",
		}).replayed,
		true,
	);
	assert.equal(
		b.issuePermit({
			actor,
			taskId: task.id,
			budgetId: "gpu",
			reservationId,
			operation: "shell",
			idempotencyKey: "permit",
		}).replayed,
		true,
	);
	assert.equal(b.snapshot().seq, seq);
	assert.equal(b.snapshot().tasks[task.id].state, "in_progress");
	assert.equal(b.snapshot().permits[permit.id].state, "reserved");
	assert.equal(b.snapshot().budgets.gpu.used, 1);
	assert.throws(
		() => b.reserveBudget({ actor, budgetId: "gpu", amount: 2, idempotencyKey: "reserve" }),
		{ code: "IDEMPOTENCY_CONFLICT" },
	);
	assert.throws(
		() => b.createBudget({ actor, budgetId: "gpu", limit: 100, idempotencyKey: "overwrite" }),
		/already exists/,
	);
});

it("a reservation cannot back two permits and an operation mismatch cannot dispatch", () => {
	const { b, actor, task, permit, reservationId } = attempt();
	assert.throws(
		() =>
			b.issuePermit({
				actor,
				taskId: task.id,
				budgetId: "gpu",
				reservationId,
				operation: "shell",
				idempotencyKey: "second",
			}),
		/already belongs/,
	);
	assert.throws(
		() =>
			b.dispatchPermit({
				actor,
				permitId: permit.id,
				operation: "deploy",
				idempotencyKey: "wrong-operation",
			}),
		/operation/,
	);
	assert.equal(b.snapshot().permits[permit.id].state, "reserved");
});

it("dispatch persists permit and reservation in exactly one event", () => {
	const { b, actor, permit, reservationId, entries } = attempt();
	const before = entries.length;
	b.dispatchPermit({
		actor,
		permitId: permit.id,
		operation: "shell",
		idempotencyKey: "dispatch",
		outcomeUnknown: true,
	});
	assert.equal(entries.length, before + 1);
	assert.equal(b.snapshot().permits[permit.id].state, "outcome_unknown");
	assert.equal(b.snapshot().budgets.gpu.reservations[reservationId].state, "outcome_unknown");
	assert.equal(b.snapshot().budgets.gpu.used, 1);
	assert.equal(b.snapshot().budgets.gpu.dispatchedUnknown, 1);
	b.settlePermit({ actor, permitId: permit.id, outcome: "settled", idempotencyKey: "reconcile" });
	assert.equal(b.snapshot().budgets.gpu.used, 1);
	assert.equal(b.snapshot().budgets.gpu.dispatchedUnknown, 0);
});

it("failed dispatch persistence leaves both permit and budget unchanged", () => {
	const { b, actor, permit, reservationId, fail } = attempt();
	const before = b.snapshot().seq;
	fail();
	assert.throws(
		() =>
			b.dispatchPermit({
				actor,
				permitId: permit.id,
				operation: "shell",
				idempotencyKey: "dispatch",
			}),
		/disk failure/,
	);
	assert.equal(b.snapshot().seq, before);
	assert.equal(b.snapshot().permits[permit.id].state, "reserved");
	assert.equal(b.snapshot().budgets.gpu.reservations[reservationId].state, "reserved");
});

it("typed holds apply to task, role and channel targets without a manually copied blocker", () => {
	for (const scope of ["task", "role", "channel"]) {
		const { b, actor, task, permit } = attempt();
		const channel = b.createChannel({
			actor,
			name: "control",
			purpose: "Typed holds",
			members: [{ roleId: actor.roleId, mode: "participate" }],
			idempotencyKey: "control-channel",
		}).event.payload;
		const targets = [
			scope === "task"
				? { taskId: task.id }
				: scope === "role"
					? { roleId: actor.roleId }
					: { channelId: channel.id },
		];
		const hold = b.emitSignal({
			actor,
			kind: "hold",
			severity: "critical",
			summary: "Stop this attempt",
			targets,
			idempotencyKey: "late-hold",
		}).event.payload;
		assert.throws(
			() =>
				b.dispatchPermit({
					actor,
					permitId: permit.id,
					operation: "shell",
					idempotencyKey: "blocked-dispatch",
				}),
			{ code: "UNRESOLVED_BLOCKER" },
		);
		b.resolveSignal({
			actor,
			signalId: hold.id,
			expectedRevision: 1,
			idempotencyKey: "resolve-hold",
		});
		assert.throws(
			() =>
				b.dispatchPermit({
					actor,
					permitId: permit.id,
					operation: "shell",
					idempotencyKey: "changed-gates",
				}),
			{ code: "PERMIT_NOT_USABLE" },
		);
		assert.equal(b.snapshot().permits[permit.id].state, "reserved");
	}
});

it("new acknowledgement obligations cannot be bypassed by an already issued permit", () => {
	const { b, actor, task, permit } = attempt();
	const signal = b.emitSignal({
		actor,
		kind: "decision",
		severity: "critical",
		summary: "Acknowledge the changed contract",
		targets: [{ taskId: task.id }],
		requiresAck: [actor.roleId],
		idempotencyKey: "ack-gate",
	}).event.payload;
	assert.throws(
		() =>
			b.dispatchPermit({
				actor,
				permitId: permit.id,
				operation: "shell",
				idempotencyKey: "before-ack",
			}),
		{ code: "ACK_REQUIRED" },
	);
	b.acknowledgeSignal({ actor, signalId: signal.id, expectedRevision: 1, idempotencyKey: "ack" });
	assert.throws(
		() =>
			b.dispatchPermit({
				actor,
				permitId: permit.id,
				operation: "shell",
				idempotencyKey: "after-ack",
			}),
		{ code: "PERMIT_NOT_USABLE" },
	);
});

it("concurrent reservations cannot overspend and concurrent retries charge only once", async () => {
	const { b, actor } = attempt();
	const results = await Promise.allSettled(
		Array.from({ length: 50 }, (_, index) =>
			Promise.resolve().then(() =>
				b.reserveBudget({ actor, budgetId: "gpu", idempotencyKey: `concurrent-${index}` }),
			),
		),
	);
	assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
	for (const result of results)
		if (result.status === "rejected") assert.equal(result.reason.code, "BUDGET_EXHAUSTED");
	const retries = await Promise.all(
		Array.from({ length: 50 }, () =>
			Promise.resolve().then(() =>
				b.reserveBudget({ actor, budgetId: "gpu", idempotencyKey: "concurrent-0" }),
			),
		),
	);
	assert.ok(retries.every((result) => result.replayed));
	assert.equal(b.snapshot().budgets.gpu.used, 2);
});

it("audited coordinator reconciliation closes old-generation unknown attempts without releasing charges", () => {
	const { b, actor, permit } = attempt();
	b.dispatchPermit({
		actor,
		permitId: permit.id,
		operation: "shell",
		outcomeUnknown: true,
		idempotencyKey: "unknown-dispatch",
	});
	b.rebindRole({ actor, roleId: actor.roleId, runId: "replacement", idempotencyKey: "replace" });
	const replacement = { ...actor, runId: "replacement", generation: 2 };
	assert.throws(
		() =>
			b.settlePermit({
				actor: replacement,
				permitId: permit.id,
				outcome: "settled",
				idempotencyKey: "ordinary-settlement",
			}),
		{ code: "STALE_GENERATION" },
	);
	assert.throws(
		() =>
			b.reconcilePermit({
				actor: replacement,
				permitId: permit.id,
				outcome: "cancelled_before_dispatch",
				reason: "Cannot refund an unknown dispatch",
				idempotencyKey: "refund",
			}),
		{ code: "PERMIT_NOT_USABLE" },
	);
	const result = b.reconcilePermit({
		actor: replacement,
		permitId: permit.id,
		outcome: "settled",
		reason: "Verified the external attempt completed",
		idempotencyKey: "reconcile",
	});
	assert.equal(result.event.payload.reason, "Verified the external attempt completed");
	assert.equal(b.snapshot().permits[permit.id].state, "settled");
	assert.equal(b.snapshot().budgets.gpu.used, 1);
	assert.equal(b.snapshot().budgets.gpu.dispatchedUnknown, 0);
});
