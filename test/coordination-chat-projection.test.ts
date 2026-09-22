import assert from "node:assert/strict";
import { it } from "node:test";
import { buildCoordinationChatThreads, CoordinationChatStore } from "../src/chat/records.ts";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { coordinationChatMembers, coordinationChatRecords } from "../src/coordination/chat.ts";

it("chat preserves channel conversations, direct messages and stable agents without transport state", () => {
	const b = new CoordinationBroker("ui", "branch");
	const actor = { roleId: "coordinator", runId: "parent-run", generation: 1 };
	b.admitRole({ roleId: "coordinator", capabilities: ["coordinator"], idempotencyKey: "admit" });
	b.bindRole({ roleId: "coordinator", runId: actor.runId, idempotencyKey: "bind" });
	b.admitRole({ actor, roleId: "worker", capabilities: ["worker"], idempotencyKey: "worker" });
	b.bindRole({ roleId: "worker", runId: "secret-run", idempotencyKey: "bind-worker" });
	const channel = b.createChannel({
		actor,
		name: "Review",
		purpose: "Shared room",
		members: [
			{ roleId: "coordinator", mode: "participate" },
			{ roleId: "worker", mode: "participate" },
		],
		idempotencyKey: "channel",
	}).event;
	const sent = b.sendChannel({
		actor,
		channelId: channel.payload.id,
		expectedRevision: 1,
		text: "Channel payload <img src=x>",
		idempotencyKey: "send",
	}).event;
	const direct = b.emitSignal({
		actor,
		kind: "decision",
		severity: "info",
		summary: "Direct worker context",
		targets: [{ roleId: "worker" }],
		idempotencyKey: "direct",
	}).event;
	const store = new CoordinationChatStore();
	for (const event of [channel, sent, direct])
		for (const record of coordinationChatRecords(event, b.snapshot())) {
			assert.equal(store.add(record), true);
			assert.equal(store.add(record), false, "backfill is idempotent");
		}
	const threads = buildCoordinationChatThreads(store.snapshot().records);
	assert.ok(
		threads
			.find((thread) => thread.id === `channel:${channel.payload.id}`)
			?.records.some((r) => r.event.text === "Channel payload <img src=x>"),
	);
	assert.ok(
		threads
			.find((thread) => thread.id === "agent:worker")
			?.records.some((r) => r.event.text.includes("Direct worker context")),
	);
	assert.doesNotMatch(JSON.stringify(store.snapshot()), /secret-run|parent-run|socket|checkpoint/);
	assert.deepEqual(
		coordinationChatMembers(b.snapshot(), [
			{ id: "secret-run", name: "Live worker", status: "thinking" },
		]),
		[{ id: "worker", name: "Live worker", role: "worker", status: "thinking", backend: "pi" }],
	);
	const closed = b.closeChannel({
		actor,
		channelId: channel.payload.id,
		expectedRevision: 1,
		idempotencyKey: "close",
	}).event;
	const closeRecord = coordinationChatRecords(closed, b.snapshot())[0];
	assert.equal(closeRecord.event.channel?.status, "closed");
	assert.equal(
		store.add(closeRecord),
		true,
		"partial channel-close events preserve membership for rendering",
	);
	b.rebindRole({ actor, roleId: "worker", runId: "replacement-run", idempotencyKey: "rebind" });
	assert.equal(
		coordinationChatMembers(b.snapshot(), [
			{ id: "replacement-run", name: "Resumed worker", status: "working" },
		])[0].id,
		"worker",
	);
	b.detachRole({ actor, roleId: "worker", idempotencyKey: "detach" });
	assert.deepEqual(
		coordinationChatMembers(b.snapshot(), [], new Map([["worker", "Resumed worker"]]))[0],
		{
			id: "worker",
			name: "Resumed worker",
			role: "worker",
			status: "archived",
			backend: "pi",
		},
	);
});

it("UI accepts channels larger than the historic twenty-member bound", () => {
	const store = new CoordinationChatStore();
	assert.equal(
		store.add({
			timestamp: 1,
			fromName: "Coordinator",
			toName: "Large room",
			event: {
				id: "large",
				from: "parent",
				to: "room",
				kind: "channel_create",
				text: "Large room",
				channel: {
					id: "room",
					name: "Large room",
					status: "open",
					members: Array.from({ length: 100 }, (_, i) => `role-${i}`),
				},
			},
		}),
		true,
	);
});
