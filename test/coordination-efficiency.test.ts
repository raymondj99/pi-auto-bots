import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { parseCoordinationConfig } from "../src/coordination/config.ts";
import { CoordinationEventLog } from "../src/coordination/event-log.ts";
import { installCoordinationInbox } from "../src/coordination/inbox.ts";
import { createProjection } from "../src/coordination/projection.ts";
import { compactMutationResult, publicMutationResult } from "../src/coordination/results.ts";
import {
	registerCoordinationTools,
	registerCoordinationWorkflowTools,
} from "../src/coordination/tools.ts";

const coordinator = { roleId: "coordinator", runId: "parent", generation: 1 };
const worker = { roleId: "worker", runId: "child", generation: 1 };
function fixture(mode: "fast" | "strict" = "fast") {
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
		{ workflowMode: mode },
	);
	b.admitRole({ roleId: "coordinator", capabilities: ["coordinator"], idempotencyKey: "admit" });
	b.bindRole({ roleId: "coordinator", runId: coordinator.runId, idempotencyKey: "bind" });
	b.admitRole({
		actor: coordinator,
		roleId: "worker",
		capabilities: ["worker"],
		idempotencyKey: "worker",
	});
	b.bindRole({ roleId: "worker", runId: worker.runId, idempotencyKey: "worker-bind" });
	return { b, entries };
}
function task(b: CoordinationBroker, start = false) {
	return b.createTask({
		actor: coordinator,
		id: "work",
		title: "Work",
		owner: "worker",
		start,
		idempotencyKey: "task",
	});
}
function channel(b: CoordinationBroker) {
	return b.createChannel({
		actor: coordinator,
		name: "work",
		purpose: "Shared decisions",
		members: [
			{ roleId: "coordinator", mode: "participate" },
			{ roleId: "worker", mode: "participate" },
		],
		idempotencyKey: "channel",
	}).event.payload;
}

it("fast defaults remove shell ceremonies; strict and explicit operation lists remain available", () => {
	assert.equal(parseCoordinationConfig({}).workflowMode, "fast");
	assert.deepEqual(parseCoordinationConfig({}).protectedOperations, ["costly_call", "deploy"]);
	assert.ok(
		parseCoordinationConfig({
			coordination: { workflowMode: "strict" },
		}).protectedOperations.includes("shell"),
	);
	assert.deepEqual(
		parseCoordinationConfig({
			coordination: { workflowMode: "fast", protectedOperations: ["shell"] },
		}).protectedOperations,
		["shell"],
	);
	assert.throws(() => parseCoordinationConfig({ coordination: { workflowMode: "typo" } }));
});

it("fast create/start is atomic and lease-free, but dependencies, holds and generations still gate it", () => {
	const { b } = fixture();
	assert.equal(task(b, true).event.payload.state, "in_progress");
	assert.equal(b.snapshot().tasks.work.revision, 1);
	assert.deepEqual(b.snapshot().budgets, {});
	assert.throws(
		() =>
			b.createTask({
				actor: coordinator,
				id: "dependent",
				title: "Wait",
				owner: "worker",
				start: true,
				dependsOn: ["work"],
				idempotencyKey: "dependent",
			}),
		{ code: "UNRESOLVED_DEPENDENCY" },
	);
	assert.equal(b.snapshot().tasks.dependent, undefined);
	b.createTask({
		actor: coordinator,
		id: "held",
		title: "Held",
		owner: "worker",
		idempotencyKey: "held",
	});
	const hold = b.emitSignal({
		actor: worker,
		kind: "blocked",
		severity: "blocking",
		summary: "Need input",
		targets: [{ taskId: "held" }],
		idempotencyKey: "hold",
	}).event.payload;
	assert.throws(
		() =>
			b.transitionTask({
				actor: worker,
				taskId: "held",
				expectedRevision: 1,
				state: "in_progress",
				idempotencyKey: "blocked",
			}),
		{ code: "UNRESOLVED_BLOCKER" },
	);
	b.resolveSignal({
		actor: worker,
		signalId: hold.id,
		expectedRevision: 1,
		idempotencyKey: "resolve-own",
	});
	b.transitionTask({
		actor: worker,
		taskId: "held",
		expectedRevision: 1,
		state: "in_progress",
		idempotencyKey: "start",
	});
	b.rebindRole({ actor: coordinator, roleId: "worker", runId: "new", idempotencyKey: "rebind" });
	assert.throws(
		() =>
			b.transitionTask({
				actor: worker,
				taskId: "held",
				expectedRevision: 2,
				state: "submitted",
				idempotencyKey: "stale",
			}),
		{ code: "STALE_GENERATION" },
	);
	const { b: strict } = fixture("strict");
	assert.throws(() => task(strict, true), { code: "LEASE_REQUIRED" });
	task(strict);
	assert.throws(
		() =>
			strict.transitionTask({
				actor: worker,
				taskId: "work",
				expectedRevision: 1,
				state: "in_progress",
				idempotencyKey: "no-shortcut",
			}),
		{ code: "INVALID_TRANSITION" },
	);
});

it("automatic spawn assignments start when ready but preserve pending role gates without aborting launch", () => {
	const { b } = fixture();
	const hold = b.emitSignal({
		actor: coordinator,
		kind: "hold",
		severity: "blocking",
		summary: "Await contract",
		targets: [{ roleId: "worker" }],
		idempotencyKey: "hold",
	}).event.payload;
	const params = {
		actor: coordinator,
		owner: "worker",
		title: "Spawn assignment",
		idempotencyKey: "assignment",
	};
	const assigned = b.assignDefaultTask(params);
	assert.equal(assigned.event.payload.state, "draft");
	assert.equal(b.assignDefaultTask(params).replayed, true);
	b.resolveSignal({
		actor: coordinator,
		signalId: hold.id,
		expectedRevision: 1,
		idempotencyKey: "resolved",
	});
	assert.equal(
		b.assignDefaultTask(params).event.payload.state,
		"draft",
		"retry cannot create a second assignment after gates change",
	);
	const ready = b.assignDefaultTask({ ...params, idempotencyKey: "another-assignment" });
	assert.equal(ready.event.payload.state, "in_progress");
});

it("publish freezes a producer's exact bytes in one event without granting review authority", () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-fast-publish-"));
	try {
		const sourcePath = join(directory, "output.txt");
		writeFileSync(sourcePath, "verified bytes");
		const { b, entries } = fixture();
		task(b, true);
		const params = {
			actor: worker,
			artifactId: "output",
			sourcePath,
			storeDir: join(directory, "store"),
			freeze: true,
			idempotencyKey: "publish",
		};
		const before = b.snapshot().seq;
		const result = b.sealArtifact(params);
		assert.equal(result.event.type, "artifact.frozen");
		assert.equal(b.snapshot().seq, before + 1);
		assert.equal(b.sealArtifact(params).replayed, true);
		assert.equal(b.snapshot().artifacts.output.length, 1);
		const restored = CoordinationBroker.restore(entries, "s", "b");
		assert.equal(restored.snapshot().artifacts.output[0].state, "frozen");
		assert.throws(
			() =>
				b.transitionTask({
					actor: worker,
					taskId: "work",
					expectedRevision: 1,
					state: "submitted",
					reviewSatisfied: true,
					idempotencyKey: "self-review",
				}),
			{ code: "UNAUTHORIZED" },
		);
		const { b: strict } = fixture("strict");
		assert.throws(() => strict.sealArtifact(params), { code: "UNAUTHORIZED" });
	} finally {
		rmSync(directory, { recursive: true, force: true });
	}
});

it("prepare reserves and issues in one durable event, retries once, and never charges a rejected attempt", () => {
	const { b, entries } = fixture();
	task(b, true);
	b.createBudget({ actor: coordinator, budgetId: "pool", limit: 3, idempotencyKey: "pool" });
	assert.throws(
		() =>
			b.preparePermit({
				actor: worker,
				taskId: "work",
				expectedRevision: 1,
				budgetId: "pool",
				operation: "deploy",
				idempotencyKey: "unassigned",
			}),
		{ code: "UNAUTHORIZED" },
	);
	b.reserveBudget({
		actor: coordinator,
		taskId: "work",
		expectedRevision: 1,
		budgetId: "pool",
		idempotencyKey: "grant",
	});
	const p = {
		actor: worker,
		taskId: "work",
		expectedRevision: 2,
		budgetId: "pool",
		operation: "deploy",
		idempotencyKey: "prepare",
	};
	const before = b.snapshot().seq;
	const prepared = b.preparePermit(p);
	assert.equal(b.snapshot().seq, before + 1);
	assert.equal(prepared.event.payload.taskRevision, 2, "reuse already granted capacity");
	assert.equal(b.preparePermit(p).replayed, true);
	assert.equal(b.snapshot().budgets.pool.used, 1);
	assert.throws(() => b.preparePermit({ ...p, expectedRevision: 1, idempotencyKey: "stale" }), {
		code: "STALE_REVISION",
	});
	assert.equal(b.snapshot().budgets.pool.used, 1);
	b.dispatchPermit({
		actor: worker,
		permitId: prepared.event.payload.id,
		operation: "deploy",
		idempotencyKey: "dispatch",
	});
	b.settlePermit({
		actor: worker,
		permitId: prepared.event.payload.id,
		outcome: "outcome_unknown",
		idempotencyKey: "unknown",
	});
	const replenished = b.preparePermit({ ...p, idempotencyKey: "replenish" });
	assert.equal(replenished.event.payload.taskRevision, 3);
	const restored = CoordinationBroker.restore(entries, "s", "b").snapshot();
	assert.equal(restored.budgets.pool.used, 2);
	assert.equal(restored.budgets.pool.dispatchedUnknown, 1);
	assert.equal(restored.tasks.work.revision, 3);
	assert.equal(restored.permits[prepared.event.payload.id].state, "outcome_unknown");
});

it("routine inbox delivery is bounded, not self-echoed, and does not resurrect resolved signals", () => {
	const { b } = fixture();
	const c = channel(b);
	let wakes = 0;
	b.setDeliverySink(() => {
		wakes++;
	});
	b.sendChannel({
		actor: coordinator,
		channelId: c.id,
		expectedRevision: 1,
		text: "Context once",
		delivery: "next_turn",
		idempotencyKey: "send",
	});
	assert.equal(wakes, 0);
	assert.deepEqual(b.inbox({ actor: coordinator }).lines, []);
	assert.equal(b.inbox({ actor: worker }).lines.length, 1);
	const s = b.emitSignal({
		actor: coordinator,
		kind: "attention",
		severity: "info",
		summary: "Obsolete",
		targets: [{ roleId: "worker" }],
		idempotencyKey: "obsolete",
	}).event.payload;
	b.queueSignalDelivery({
		actor: coordinator,
		signalId: s.id,
		targetRoleId: "worker",
		idempotencyKey: "delivery",
	});
	b.resolveSignal({
		actor: coordinator,
		signalId: s.id,
		expectedRevision: 1,
		idempotencyKey: "resolve",
	});
	assert.equal(b.inbox({ actor: worker }).lines.length, 1);
	for (let n = 0; n < 5; n++)
		b.sendChannel({
			actor: coordinator,
			channelId: c.id,
			expectedRevision: 1,
			text: "🚀".repeat(3500),
			idempotencyKey: `long-${n}`,
		});
	const inbox = b.inbox({ actor: worker });
	assert.ok(Buffer.byteLength(inbox.lines.join("")) <= 20 * 1024);
	assert.ok(inbox.lines.length < 6);
});

it("inbox hooks piggyback once and receipt only observed injection, including error results and new turns", async () => {
	const { b } = fixture();
	const c = channel(b);
	const handlers = new Map<string, any>();
	const pi: any = {
		on(name: string, fn: any) {
			handlers.set(name, fn);
		},
	};
	installCoordinationInbox(
		pi,
		() => ({ broker: b, actor: worker }),
		() => true,
	);
	const sent = b.sendChannel({
		actor: coordinator,
		channelId: c.id,
		expectedRevision: 1,
		text: "Use new contract",
		idempotencyKey: "send",
	}).event.payload;
	const event = {
		content: [{ type: "text", text: "command failed" }],
		details: { exitCode: 1 },
		isError: true,
	};
	assert.equal(
		await handlers.get("tool_result")({ ...event, details: [1, 2] }),
		undefined,
		"preserve foreign details",
	);
	assert.equal(
		await handlers.get("tool_result")({
			...event,
			content: [{ type: "text", text: "x".repeat(40 * 1024) }],
		}),
		undefined,
		"defer rather than overflow large output",
	);
	const result = await handlers.get("tool_result")(event);
	assert.equal(result.details.exitCode, 1);
	assert.equal(result.content.length, 2);
	assert.equal(b.snapshot().deliveries[sent.deliveries[0].id].state, "queued");
	assert.equal(await handlers.get("tool_result")(event), undefined);
	await handlers.get("message_end")({ message: { role: "toolResult", ...result, isError: true } });
	assert.equal(b.snapshot().deliveries[sent.deliveries[0].id].state, "delivered");
	b.sendChannel({
		actor: coordinator,
		channelId: c.id,
		expectedRevision: 1,
		text: "Next turn",
		idempotencyKey: "next",
	});
	const next = await handlers.get("before_agent_start")({});
	await handlers.get("message_end")({ message: next.message });
	assert.equal(b.inbox({ actor: worker }).lines.length, 0);
});

it("compact receipts retain actionable IDs/revisions and full details without resending history", async () => {
	const { b } = fixture();
	const result = task(b, true);
	const compact = compactMutationResult(publicMutationResult(result)) as any;
	assert.equal(compact.id, "work");
	assert.equal(compact.revision, 1);
	assert.equal(compact.state, "in_progress");
	assert.ok(
		JSON.stringify(compact).length < JSON.stringify(publicMutationResult(result)).length / 2,
	);
	const tools = new Map<string, any>();
	const pi: any = {
		registerTool(t: any) {
			tools.set(t.name, t);
		},
	};
	registerCoordinationWorkflowTools(pi, () => ({ broker: b, actor: coordinator }));
	const stateOnly = await tools.get("subagent_team").execute("query", { kind: "tasks" });
	assert.deepEqual(
		stateOnly.details.digest.lines,
		[],
		"queries do not replay the caller's own history",
	);
	const reply = await tools.get("subagent_task").execute("tool", {
		action: "create",
		taskId: "short",
		title: "Short",
		owner: "coordinator",
		start: true,
		idempotencyKey: "short",
	});
	assert.equal(reply.details.event.payload.state, "in_progress");
	assert.equal(JSON.parse(reply.content[0].text).id, "short");
	let armed = "";
	registerCoordinationTools(
		pi,
		() => ({
			broker: b,
			actor: coordinator,
			protectedOperations: ["deploy"],
			armPermit: async (id) => {
				armed = id;
			},
		}),
		() => true,
	);
	b.createBudget({ actor: coordinator, budgetId: "pool", limit: 1, idempotencyKey: "pool" });
	const prepared = await tools.get("subagent_permit").execute("tool", {
		action: "prepare",
		taskId: "short",
		expectedRevision: 1,
		budgetId: "pool",
		operation: "deploy",
		idempotencyKey: "prepare",
	});
	assert.equal(armed, prepared.details.event.payload.id);
	assert.equal(JSON.parse(prepared.content[0].text).taskRevision, 2);
});

it("copy-on-write revisions stay immutable across appends and detached snapshots", () => {
	const log = new CoordinationEventLog(createProjection());
	const admitted = log.append(
		"role.admitted",
		coordinator,
		{ roleId: "coordinator", capabilities: ["coordinator"] },
		"admit",
	);
	log.append("role.bound", coordinator, { ...coordinator }, "bind");
	assert.equal(admitted.projection.roles.coordinator.active, false);
	const bound = log.append("role.detached", coordinator, { roleId: "coordinator" }, "detach");
	const snapshot = log.snapshot();
	snapshot.roles.coordinator.capabilities.push("bad");
	snapshot.audit[0].actor.roleId = "bad";
	assert.equal(bound.projection.roles.coordinator.capabilities.includes("bad"), false);
	assert.equal(log.snapshot().audit[0].actor.roleId, "coordinator");
});
