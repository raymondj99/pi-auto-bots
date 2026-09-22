import assert from "node:assert/strict";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { buildDigest } from "../src/coordination/delivery.ts";
import {
	createProjection,
	makeEnvelope,
	reduceCoordinationEvent,
} from "../src/coordination/projection.ts";

it("digest pagination never advances past an omitted targeted event", () => {
	let projection = createProjection("session", "branch");
	const actor = { roleId: "coordinator", runId: "run", generation: 1 };
	for (let i = 1; i <= 6; i++) {
		const event = makeEnvelope(
			projection,
			"signal.emitted",
			actor,
			{
				id: `signal-${i}`,
				targets: [{ roleId: i % 2 ? "worker" : "other" }],
				summary: `Action ${i}`,
			},
			`event-${i}`,
		);
		projection = reduceCoordinationEvent(projection, { ...event, actor });
	}
	const first = buildDigest(projection, "worker", 0, 1);
	assert.equal(first.cursor, 2);
	assert.equal(first.truncated, 2);
	const second = buildDigest(projection, "worker", first.cursor, 1);
	assert.equal(second.cursor, 4);
	const third = buildDigest(projection, "worker", second.cursor, 1);
	assert.equal(third.cursor, 6);
	assert.deepEqual(
		[first, second, third].flatMap((page) => page.lines),
		[
			"1 signal.emitted signal-1: Action 1",
			"3 signal.emitted signal-3: Action 3",
			"5 signal.emitted signal-5: Action 5",
		],
	);
	assert.deepEqual(buildDigest(projection, "worker", third.cursor, 1).lines, []);
});

it("digest cursors and page limits reject invalid numeric inputs", () => {
	const projection = createProjection();
	for (const cursor of [-1, 1, NaN, Infinity, 0.5])
		assert.throws(() => buildDigest(projection, "worker", cursor));
	for (const limit of [-1, 0, 101, NaN, Infinity, 0.5])
		assert.throws(() => buildDigest(projection, "worker", 0, limit));
});

it("explicit acknowledgement and resolution update deliveries without inventing injection receipts", () => {
	for (const injectFirst of [true, false]) {
		const b = new CoordinationBroker("session", "branch");
		const actor = { roleId: "coordinator", runId: "run", generation: 1 };
		b.admitRole({ roleId: actor.roleId, capabilities: ["coordinator"], idempotencyKey: "admit" });
		b.bindRole({ roleId: actor.roleId, runId: actor.runId, idempotencyKey: "bind" });
		const signal = b.emitSignal({
			actor,
			kind: "decision",
			severity: "critical",
			summary: "Explicit acknowledgement",
			targets: [{ roleId: actor.roleId }],
			requiresAck: [actor.roleId],
			idempotencyKey: "signal",
		}).event.payload;
		const delivery = b.queueSignalDelivery({
			actor,
			signalId: signal.id,
			targetRoleId: actor.roleId,
			idempotencyKey: "queue",
		}).event.payload as { id: string };
		assert.equal(b.snapshot().deliveries[delivery.id].state, "queued");
		if (injectFirst)
			b.receipt({ actor, deliveryId: delivery.id, state: "delivered", idempotencyKey: "injected" });
		b.acknowledgeSignal({ actor, signalId: signal.id, expectedRevision: 1, idempotencyKey: "ack" });
		assert.equal(
			b.snapshot().deliveries[delivery.id].state,
			injectFirst ? "acknowledged" : "queued",
		);
		b.resolveSignal({ actor, signalId: signal.id, expectedRevision: 2, idempotencyKey: "resolve" });
		assert.equal(b.snapshot().deliveries[delivery.id].state, injectFirst ? "resolved" : "queued");
		if (!injectFirst) {
			assert.equal(b.snapshot().deliveries[delivery.id].deliveredAt, undefined);
			b.receipt({ actor, deliveryId: delivery.id, state: "delivered", idempotencyKey: "injected" });
		}
		const final = b.snapshot().deliveries[delivery.id];
		assert.equal(final.state, "resolved");
		assert.ok(final.deliveredAt && final.acknowledgedAt && final.resolvedAt);
		assert.throws(
			() =>
				b.receipt({
					actor,
					deliveryId: delivery.id,
					state: "resolved" as any,
					idempotencyKey: "forged-resolution",
				}),
			{ code: "UNAUTHORIZED" },
		);
	}
});
