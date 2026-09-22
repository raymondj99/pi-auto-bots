import assert from "node:assert/strict";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";

function team() {
	const b = new CoordinationBroker("session", "branch");
	const coordinator = { roleId: "coordinator", runId: "coordinator", generation: 1 };
	b.admitRole({
		roleId: "coordinator",
		capabilities: ["coordinator"],
		idempotencyKey: "coordinator-admit",
	});
	b.bindRole({ roleId: "coordinator", runId: "coordinator", idempotencyKey: "coordinator-bind" });
	for (const roleId of ["worker", "watcher", "outsider"]) {
		b.admitRole({
			actor: coordinator,
			roleId,
			capabilities: ["worker"],
			idempotencyKey: `${roleId}-admit`,
		});
		b.bindRole({ roleId, runId: roleId, idempotencyKey: `${roleId}-bind` });
	}
	const worker = { roleId: "worker", runId: "worker", generation: 1 };
	const watcher = { roleId: "watcher", runId: "watcher", generation: 1 };
	const channel = b.createChannel({
		actor: coordinator,
		name: "contract",
		purpose: "Shared inputs",
		members: [
			{ roleId: "worker", mode: "participate" },
			{ roleId: "watcher", mode: "watch" },
		],
		idempotencyKey: "channel",
	}).event.payload;
	return { b, coordinator, worker, watcher, channel };
}

it("role channels persist one fan-out event, digest routine messages, and enforce membership revisions", () => {
	const { b, worker, watcher, coordinator, channel } = team();
	let wakes = 0;
	b.setDeliverySink(() => {
		wakes++;
	});
	const before = b.snapshot().seq;
	const result = b.sendChannel({
		actor: worker,
		channelId: channel.id,
		expectedRevision: 1,
		text: "Shared contract update",
		idempotencyKey: "send",
	});
	assert.equal(b.snapshot().seq, before + 1);
	assert.equal(wakes, 0);
	assert.deepEqual(result.event.payload.queued, ["watcher"]);
	assert.equal(
		b.digest(watcher).lines.some((line) => line.includes("Shared contract update")),
		true,
	);
	assert.equal(
		b.snapshot("outsider").audit.some((event) => event.type === "channel.message"),
		false,
	);
	assert.throws(
		() =>
			b.sendChannel({
				actor: watcher,
				channelId: channel.id,
				expectedRevision: 1,
				text: "watcher write",
				idempotencyKey: "watcher-send",
			}),
		{ code: "UNAUTHORIZED" },
	);
	b.updateChannel({
		actor: coordinator,
		channelId: channel.id,
		expectedRevision: 1,
		members: [{ roleId: "watcher", mode: "participate" }],
		idempotencyKey: "membership",
	});
	assert.throws(
		() =>
			b.sendChannel({
				actor: worker,
				channelId: channel.id,
				expectedRevision: 1,
				text: "stale",
				idempotencyKey: "stale",
			}),
		{ code: "STALE_REVISION" },
	);
	assert.throws(
		() =>
			b.sendChannel({
				actor: worker,
				channelId: channel.id,
				expectedRevision: 2,
				text: "removed",
				idempotencyKey: "removed",
			}),
		{ code: "UNAUTHORIZED" },
	);
	assert.throws(
		() =>
			b.queueSignalDelivery({
				actor: watcher,
				signalId: result.event.payload.signal.id,
				targetRoleId: "outsider",
				idempotencyKey: "leak",
			}),
		{ code: "UNAUTHORIZED" },
	);
});

it("urgent channel retries do not schedule another wake and failed sinks retain queued delivery", () => {
	const { b, coordinator, worker, channel } = team();
	b.updateChannel({
		actor: coordinator,
		channelId: channel.id,
		expectedRevision: 1,
		delivery: "urgent",
		idempotencyKey: "urgent",
	});
	let calls = 0;
	b.setDeliverySink(() => {
		calls++;
		throw new Error("backpressure");
	});
	const params = {
		actor: worker,
		channelId: channel.id,
		expectedRevision: 2,
		text: "Action required",
		idempotencyKey: "send",
	};
	const result = b.sendChannel(params);
	assert.equal(result.event.payload.signal.severity, "action");
	assert.equal(b.sendChannel(params).replayed, true);
	assert.equal(calls, 1);
	assert.equal(b.snapshot().deliveries[result.event.payload.deliveries[0].id].state, "queued");
});

it("raw mutation validation rejects malformed/prototype/credential payloads without appending", () => {
	const { b, coordinator } = team();
	const before = b.snapshot().seq;
	const invalid: unknown[] = [
		null,
		4,
		"dependencies",
		[42],
		["__proto__"],
		Array(101).fill("dependency"),
	];
	for (const dependsOn of invalid)
		assert.throws(
			() =>
				b.createTask({
					actor: coordinator,
					title: "bad",
					owner: "worker",
					dependsOn,
					idempotencyKey: "bad",
				} as any),
			{ code: "BOUNDS_EXCEEDED" },
		);
	assert.throws(
		() =>
			b.createTask({
				actor: { ...coordinator, token: "never persist" },
				title: "bad",
				owner: "worker",
				idempotencyKey: "credentials",
			} as any),
		{ code: "BOUNDS_EXCEEDED" },
	);
	assert.throws(
		() =>
			b.createTask({
				actor: coordinator,
				title: "bad",
				owner: "__proto__",
				idempotencyKey: "prototype",
			}),
		{ code: "BOUNDS_EXCEEDED" },
	);
	assert.equal(b.snapshot().seq, before);
	assert.doesNotMatch(JSON.stringify(b.snapshot()), /never persist/);
});
