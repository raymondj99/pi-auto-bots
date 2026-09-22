import assert from "node:assert/strict";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { installCoordinationDeliveryRouting } from "../src/coordination/routing.ts";
import {
	registerCoordinationTools,
	registerCoordinationWorkflowTools,
} from "../src/coordination/tools.ts";
import { TransportBroker, TransportClient } from "../src/transport.ts";

const coordinator = { roleId: "coordinator", runId: "coordinator:session:branch", generation: 1 };
const worker = { roleId: "worker", runId: "worker-run", generation: 1 };
function fixture() {
	const broker = new CoordinationBroker("s", "b");
	broker.admitRole({
		roleId: "coordinator",
		capabilities: ["coordinator"],
		idempotencyKey: "admit",
	});
	broker.bindRole({ roleId: "coordinator", runId: coordinator.runId, idempotencyKey: "bind" });
	broker.admitRole({
		actor: coordinator,
		roleId: "worker",
		capabilities: ["worker"],
		idempotencyKey: "admit-worker",
	});
	broker.bindRole({ roleId: "worker", runId: worker.runId, idempotencyKey: "bind-worker" });
	const tools = new Map<string, any>();
	const handlers = new Map<string, ((...args: any[]) => any)[]>();
	const messages: any[] = [];
	const pi: any = {
		registerTool(t: any) {
			tools.set(t.name, t);
		},
		on(name: string, handler: (...args: any[]) => any) {
			handlers.set(name, [...(handlers.get(name) ?? []), handler]);
		},
		sendMessage(message: any, options: any) {
			messages.push({ message, options });
		},
	};
	registerCoordinationWorkflowTools(pi, () => ({ broker, actor: coordinator }));
	registerCoordinationTools(
		pi,
		() => ({ broker, actor: coordinator }),
		() => true,
	);
	const call = async (name: string, params: any) =>
		(await tools.get(name).execute("call", params)).details as any;
	return { broker, pi, call, handlers, messages };
}

it("actual tool adapters retain task IDs, leases, verification and atomic reservation attachments", async () => {
	const { broker: b, call } = fixture();
	await call("subagent_permit", {
		action: "create_budget",
		budgetId: "b",
		limit: 2,
		idempotencyKey: "budget",
	});
	const budget = await call("subagent_permit", {
		action: "reserve",
		budgetId: "b",
		idempotencyKey: "reserve",
	});
	const lease = Object.keys(budget.event.payload.reservations)[0];
	await call("subagent_task", {
		action: "create",
		taskId: "chosen",
		title: "Smoke",
		owner: "coordinator",
		leases: [lease],
		verification: ["criterion"],
		nextActions: ["test"],
		idempotencyKey: "task",
	});
	assert.deepEqual(b.snapshot().tasks.chosen.leases, [lease]);
	assert.deepEqual(b.snapshot().tasks.chosen.verification, ["criterion"]);
	await call("subagent_task", {
		action: "ready",
		taskId: "chosen",
		expectedRevision: 1,
		idempotencyKey: "ready",
	});
	await call("subagent_task", {
		action: "start",
		taskId: "chosen",
		expectedRevision: 2,
		idempotencyKey: "start",
	});
	await assert.rejects(
		call("subagent_permit", {
			action: "reserve",
			budgetId: "b",
			taskId: "chosen",
			expectedRevision: 2,
			idempotencyKey: "stale",
		}),
		/STALE_REVISION/,
	);
	assert.equal(b.snapshot().budgets.b.used, 1);
	const attached = await call("subagent_permit", {
		action: "reserve",
		budgetId: "b",
		taskId: "chosen",
		expectedRevision: 3,
		idempotencyKey: "attach",
	});
	assert.equal(attached.event.payload.task.revision, 4);
	assert.equal(b.snapshot().tasks.chosen.leases.length, 2);
	assert.equal(b.snapshot().tasks.chosen.state, "in_progress");
	await assert.rejects(
		call("subagent_task", {
			action: "start",
			taskId: "chosen",
			expectedRevision: 4,
			title: "silently ignored?",
			idempotencyKey: "bad-field",
		}),
		/does not accept title/,
	);
	await assert.rejects(
		call("subagent_permit", { action: "reserve", budgetId: "b", idempotencyKey: "exhausted" }),
		/BUDGET_EXHAUSTED/,
	);
});

it("mixed signal targets normalize, scope gates to named tasks, and supersede replaces obligations", async () => {
	const { broker: b, call } = fixture();
	b.createTask({
		actor: coordinator,
		id: "work",
		title: "Work",
		owner: "worker",
		idempotencyKey: "work",
	});
	b.createTask({
		actor: coordinator,
		id: "other",
		title: "Unrelated",
		owner: "coordinator",
		idempotencyKey: "other",
	});
	const channel = (
		await call("subagent_channel", {
			action: "create",
			name: "handoff",
			purpose: "Handoff",
			members: [
				{ roleId: "coordinator", mode: "participate" },
				{ roleId: "worker", mode: "participate" },
			],
			delivery: "urgent",
			idempotencyKey: "channel",
		})
	).event.payload;
	const signal = (
		await call("subagent_signal", {
			action: "emit",
			kind: "ready",
			severity: "action",
			summary: "Work ready",
			targets: [{ taskId: "work", roleId: "worker", channelId: channel.id }],
			requiresAck: ["worker"],
			idempotencyKey: "signal",
		})
	).event.payload;
	assert.equal(signal.targets.length, 3);
	b.transitionTask({
		actor: coordinator,
		taskId: "other",
		expectedRevision: 1,
		state: "ready",
		idempotencyKey: "other-ready",
	});
	assert.throws(
		() =>
			b.transitionTask({
				actor: worker,
				taskId: "work",
				expectedRevision: 1,
				state: "ready",
				idempotencyKey: "work-ready",
			}),
		{ code: "ACK_REQUIRED" },
	);
	const successor = (
		await call("subagent_signal", {
			action: "supersede",
			signalId: signal.id,
			expectedRevision: 1,
			summary: "Cancel the abandoned obligation",
			targets: [{ roleId: "coordinator" }],
			requiresAck: [],
			severity: "info",
			idempotencyKey: "supersede",
		})
	).event.payload;
	assert.deepEqual(successor.targets, [{ roleId: "coordinator" }]);
	assert.deepEqual(successor.requiresAck, []);
	await call("subagent_signal", {
		action: "resolve",
		signalId: successor.id,
		expectedRevision: 1,
		idempotencyKey: "resolve",
	});
	b.transitionTask({
		actor: worker,
		taskId: "work",
		expectedRevision: 1,
		state: "ready",
		idempotencyKey: "now-ready",
	});
	const quiet = (
		await call("subagent_channel", {
			action: "send",
			channelId: channel.id,
			expectedRevision: 1,
			text: "No wake",
			delivery: "next_turn",
			idempotencyKey: "quiet",
		})
	).event.payload;
	assert.equal(quiet.signal.severity, "info");
	assert.equal(quiet.deliveries[0].wakeCount, 0);
});

it("coordinator routing is local and only observed injection produces receipts", async () => {
	const { broker: b, pi, messages, handlers } = fixture();
	const routed: any[] = [];
	const stop = installCoordinationDeliveryRouting(pi, b, async (request) => {
		routed.push(request);
	});
	const s = b.emitSignal({
		actor: worker,
		kind: "attention",
		severity: "action",
		summary: "Review needed",
		targets: [{ roleId: "coordinator" }],
		idempotencyKey: "review",
	}).event.payload;
	const d = b.queueSignalDelivery({
		actor: coordinator,
		signalId: s.id,
		targetRoleId: "coordinator",
		idempotencyKey: "queue",
	}).event.payload as any;
	assert.equal(routed.length, 0);
	assert.equal(messages.length, 1);
	assert.deepEqual(messages[0].options, { triggerTurn: true, deliverAs: "followUp" });
	assert.equal(b.snapshot().deliveries[d.id].state, "queued");
	for (const handler of handlers.get("message_end") ?? [])
		handler({ message: messages[0].message });
	assert.equal(b.snapshot().deliveries[d.id].state, "delivered");
	stop();
});

it("parallel receipt responses stay small/private and the real socket remains usable", async () => {
	const { broker: b } = fixture();
	for (let n = 0; n < 80; n++)
		b.emitSignal({
			actor: coordinator,
			kind: "attention",
			severity: "info",
			summary: "private ".repeat(50),
			targets: [{ roleId: "coordinator" }],
			idempotencyKey: `private-${n}`,
		});
	const s = b.emitSignal({
		actor: coordinator,
		kind: "ready",
		severity: "action",
		summary: "Ready",
		targets: [{ roleId: "worker" }],
		requiresAck: ["worker"],
		idempotencyKey: "ready",
	}).event.payload;
	const d = b.queueSignalDelivery({
		actor: coordinator,
		signalId: s.id,
		targetRoleId: "worker",
		idempotencyKey: "queue",
	}).event.payload as any;
	const socket = new TransportBroker(
		() => {},
		undefined,
		(_from, deliveryId) =>
			b.receipt({
				actor: worker,
				deliveryId,
				state: "delivered",
				idempotencyKey: `receipt-${deliveryId}`,
			}),
		(_from, method, params) => (b as any)[method]({ ...params, actor: worker }),
	);
	await socket.start();
	const client = new TransportClient(
		socket.add(worker.runId, "worker", "/worker", false),
		() => {},
	);
	try {
		await client.ready;
		const replies = await Promise.all(
			Array.from({ length: 12 }, () =>
				client.request({ action: "delivery_receipt", deliveryId: d.id }),
			),
		);
		for (const reply of replies) {
			assert.ok(Buffer.byteLength(JSON.stringify(reply)) < 2000);
			assert.doesNotMatch(JSON.stringify(reply), /projection|private|idempotency"/);
		}
		const ack = await client.request({
			action: "coordination",
			method: "acknowledgeSignal",
			params: { signalId: s.id, expectedRevision: 1, idempotencyKey: "ack" },
		});
		assert.equal(ack.event.payload.acknowledgements[0].roleId, "worker");
		assert.equal(b.snapshot().deliveries[d.id].state, "acknowledged");
	} finally {
		client.close();
		socket.close();
	}
});

it("typed handoff transfers responsibility and rejects the old owner", async () => {
	const { broker: b, call } = fixture();
	await call("subagent_task", {
		action: "create",
		taskId: "handoff",
		owner: "coordinator",
		title: "Handoff",
		idempotencyKey: "task",
	});
	const result = await call("subagent_task", {
		action: "handoff",
		taskId: "handoff",
		expectedRevision: 1,
		owner: "worker",
		reason: "Delegate next stage",
		nextActions: ["Review the sealed contract"],
		idempotencyKey: "handoff",
	});
	assert.equal(result.event.type, "task.handed_off");
	assert.equal(b.snapshot().tasks.handoff.activeRunId, worker.runId);
	assert.equal(b.snapshot().tasks.handoff.state, "draft");
	b.handoffTask({
		actor: worker,
		taskId: "handoff",
		expectedRevision: 2,
		owner: "coordinator",
		reason: "Return for review",
		nextActions: [],
		idempotencyKey: "return",
	});
	assert.throws(
		() =>
			b.transitionTask({
				actor: worker,
				taskId: "handoff",
				expectedRevision: 3,
				state: "ready",
				idempotencyKey: "old-owner",
			}),
		{ code: "UNAUTHORIZED" },
	);
});
