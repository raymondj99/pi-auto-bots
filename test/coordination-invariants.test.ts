import assert from "node:assert/strict";
import { it } from "node:test";
import { CoordinationEventLog } from "../src/coordination/event-log.ts";
import {
	createBudget,
	markReservationDispatched,
	reserveBudget,
	settlePermit,
} from "../src/coordination/permits.ts";
import { createProjection } from "../src/coordination/projection.ts";
import type { PermitStateRecord } from "../src/coordination/protocol.ts";

it("events, payloads, actors, sink callbacks, snapshots and cached results are detached", () => {
	const actor = { roleId: "broker", runId: "bootstrap", generation: 0 };
	const payload = { roleId: "worker", capabilities: ["worker"] };
	const log = new CoordinationEventLog(createProjection("s", "b"), {
		appendEntry(_type, data) {
			const entry = data as { event: { payload: typeof payload } };
			entry.event.payload.capabilities.push("sink-injection");
		},
	});
	const result = log.append("role.admitted", actor, payload, "admit");
	payload.capabilities.push("coordinator");
	actor.roleId = "changed";
	result.event.payload.capabilities.push("returned-injection");
	result.projection.roles.worker.capabilities.push("snapshot-injection");
	const replay = log.append(
		"role.admitted",
		{ roleId: "broker", runId: "bootstrap", generation: 0 },
		{ roleId: "worker", capabilities: ["worker"] },
		"admit",
	);
	replay.event.payload.capabilities.push("retry-injection");
	assert.deepEqual(log.snapshot().roles.worker.capabilities, ["worker"]);
	assert.deepEqual(log.allEvents()[0].payload, { roleId: "worker", capabilities: ["worker"] });
	assert.equal(log.allEvents()[0].actor.roleId, "broker");
	log.append(
		"role.admitted",
		{ roleId: "broker", runId: "bootstrap", generation: 0 },
		{ roleId: "second", capabilities: ["worker"] },
		"second",
	);
	assert.equal(result.projection.seq, 1, "lazy result projections retain append-time state");
	assert.equal(result.projection.roles.second, undefined);
	assert.deepEqual(result.projection.roles.worker.capabilities, ["worker"]);
	const role = log.roleSnapshot("worker")!;
	role.capabilities.push("coordinator");
	assert.deepEqual(log.roleSnapshot("worker")?.capabilities, ["worker"]);
});

it("checkpoints preserve retry keys and reject tampered snapshots as replay bases", () => {
	const entries: Array<{ customType: string; data: any }> = [];
	const actor = { roleId: "broker", runId: "bootstrap", generation: 0 };
	const log = new CoordinationEventLog(createProjection("s", "b"), {
		appendEntry(customType, data) {
			entries.push({ customType, data });
		},
	});
	const first = log.append(
		"role.admitted",
		actor,
		{ roleId: "worker", capabilities: ["worker"] },
		"admit",
	);
	log.checkpoint("checkpoint", actor);
	const restored = new CoordinationEventLog(CoordinationEventLog.restore(entries, "s", "b"));
	assert.equal(
		restored.append("role.admitted", actor, { roleId: "worker", capabilities: ["worker"] }, "admit")
			.replayed,
		true,
	);
	assert.equal(restored.snapshot().seq, 2);
	const checkpoint = JSON.parse(entries[1].data.event.payload.checkpoint);
	checkpoint.roles.worker.capabilities.push("coordinator");
	entries[1].data.event.payload.checkpoint = JSON.stringify(checkpoint);
	const recovered = CoordinationEventLog.restore(entries, "s", "b");
	assert.deepEqual(recovered.roles.worker.capabilities, ["worker"]);
	assert.equal(first.event.seq, 1);
	assert.throws(
		() => CoordinationEventLog.restore([...entries].reverse(), "s", "b"),
		/out of order/,
	);
});

it("idempotency keys are actor-scoped and bind event references", () => {
	const log = new CoordinationEventLog(createProjection());
	const actor = { roleId: "a", runId: "run", generation: 1 };
	log.append("role.admitted", actor, { roleId: "first" }, "key", { goalId: "one" });
	assert.throws(
		() => log.append("role.admitted", actor, { roleId: "first" }, "key", { goalId: "two" }),
		{ code: "IDEMPOTENCY_CONFLICT" },
	);
	assert.equal(
		log.append("role.admitted", { ...actor, roleId: "b" }, { roleId: "second" }, "key").event.seq,
		2,
	);
});

it("budget reservations reject invalid counts, duplicate IDs and repeat dispatch", () => {
	const budget = createBudget("gpu", 10);
	for (const amount of [-10, 0, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
		assert.throws(() => reserveBudget(budget, amount, "reservation"));
	}
	const reserved = reserveBudget(budget, 1, "reservation");
	assert.throws(() => reserveBudget(reserved, 1, "reservation"));
	const dispatched = markReservationDispatched(reserved, "reservation", true);
	assert.throws(() => markReservationDispatched(dispatched, "reservation", true));
	assert.equal(dispatched.used, 1);
	assert.equal(dispatched.dispatchedUnknown, 1);
	assert.equal(budget.used, 0);
});

it("a dispatched or unknown attempt cannot be cancelled or expired to release its charge", () => {
	const permit = { state: "dispatched" } as PermitStateRecord;
	for (const state of ["dispatched", "outcome_unknown"] as const) {
		assert.throws(() => settlePermit({ ...permit, state }, "cancelled_before_dispatch"));
		assert.throws(() => settlePermit({ ...permit, state }, "expired"));
	}
	const unknown = settlePermit(permit, "outcome_unknown");
	assert.equal(settlePermit(unknown, "settled").state, "settled");
	assert.throws(() => settlePermit({ ...permit, state: "settled" }, "outcome_unknown"));
});
