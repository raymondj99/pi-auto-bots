import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import { validateToolArguments } from "@mariozechner/pi-ai";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { coordinationChatRecords } from "../src/coordination/chat.ts";
import {
	registerCoordinationTools,
	registerCoordinationWorkflowTools,
} from "../src/coordination/tools.ts";
import subagentDone from "../src/subagent-done.ts";

const Value = {
	Check: (schema: any, value: any) => {
		try {
			validateToolArguments(
				{ name: "check", description: "Test", parameters: schema },
				{ type: "toolCall", id: "check", name: "check", arguments: value },
			);
			return true;
		} catch {
			return false;
		}
	},
};
const actor = (roleId: string) => ({ roleId, runId: `${roleId}-run`, generation: 1 });
const parent = actor("coordinator"),
	storage = actor("storage"),
	domain = actor("domain"),
	reviewer = actor("reviewer");
function setup() {
	const dir = mkdtempSync(join(tmpdir(), "pi-autonomy-"));
	const entries: any[] = [],
		deliveries: any[] = [];
	const b = new CoordinationBroker(
		"autonomy",
		"branch",
		{
			appendEntry(customType, data) {
				entries.push({ customType, data });
			},
		},
		1000,
		undefined,
		{ workflowMode: "fast", artifactStoreDir: join(dir, "store") },
	);
	b.admitRole({ roleId: "Coordinator", capabilities: ["coordinator"], idempotencyKey: "root" });
	b.bindRole({ roleId: "Coordinator", runId: parent.runId, idempotencyKey: "root-bind" });
	for (const role of ["Storage", "Domain", "Reviewer"]) {
		b.admitRole({
			actor: parent,
			roleId: role,
			capabilities: role === "Reviewer" ? ["worker", "reviewer"] : ["worker"],
			idempotencyKey: role,
		});
		b.bindRole({
			roleId: role,
			runId: `${role.toLowerCase()}-run`,
			idempotencyKey: `${role}-bind`,
		});
	}
	b.setDeliverySink((d, s) => deliveries.push({ delivery: d, signal: s }));
	const channel = b.createChannel({
		actor: parent,
		name: "integration",
		purpose: "Autonomous review",
		members: ["Coordinator", "Storage", "Domain", "Reviewer"].map((roleId) => ({
			roleId,
			mode: "participate",
		})),
		review: { taskIds: ["a", "b"], reviewerRoleId: "Reviewer" },
		idempotencyKey: "channel",
	}).event.payload;
	const publish = (who: typeof parent, artifactId: string, content: string, key = artifactId) => {
		const sourcePath = join(dir, `${artifactId}.txt`);
		writeFileSync(sourcePath, content);
		const a = b.sealArtifact({
			actor: who,
			artifactId,
			sourcePath,
			freeze: true,
			idempotencyKey: `publish-${key}`,
		}).event.payload;
		return { artifactId: a.artifactId, version: a.version, digest: a.digest };
	};
	const contract = publish(parent, "contract", "frozen API");
	for (const [id, owner] of [
		["a", "Storage"],
		["b", "Domain"],
	])
		b.assignDefaultTask({
			actor: parent,
			id,
			owner,
			title: id,
			requiredInputs: [contract],
			idempotencyKey: `assignment-${id}`,
		});
	const submit = (who: typeof parent, id: string, revision = 1, content = id) => {
		const output = publish(who, `output-${id}`, content, `${id}-${revision}`);
		return b.transitionTask({
			actor: who,
			taskId: id,
			expectedRevision: revision,
			state: "submitted",
			outputs: [output],
			verification: ["Unit tests pass"],
			nextActions: [],
			idempotencyKey: `submit-${id}-${revision}`,
		});
	};
	return {
		b,
		entries,
		deliveries,
		channel,
		contract,
		publish,
		submit,
		cleanup: () => rmSync(dir, { recursive: true, force: true }),
	};
}

it("admission, channels and launch assignments share canonical roles and bind inputs before start", () => {
	const f = setup();
	try {
		const p = f.b.snapshot();
		assert.deepEqual(Object.keys(p.roles).sort(), ["coordinator", "domain", "reviewer", "storage"]);
		assert.equal(p.tasks.a.owner, "storage");
		assert.equal(p.tasks.a.state, "in_progress");
		assert.deepEqual(p.tasks.a.requiredInputs, [f.contract]);
		assert.deepEqual(p.artifacts.contract[0].consumers.sort(), ["a", "b"]);
		assert.deepEqual(
			p.channels[f.channel.id].members.map((m) => m.roleId),
			["coordinator", "storage", "domain", "reviewer"],
		);
		assert.equal(p.channels[f.channel.id].review?.reviewerRoleId, "reviewer");
		const seq = p.seq;
		assert.throws(
			() =>
				f.b.validateSpawnAssignment({
					actor: parent,
					owner: "Storage",
					id: "a",
					requiredInputs: [],
				}),
			/different inputs/,
		);
		assert.throws(
			() =>
				f.b.validateSpawnAssignment({
					actor: parent,
					owner: "Storage",
					id: "new",
					requiredInputs: [{ ...f.contract, digest: "wrong" }],
				}),
			{ code: "INPUT_NOT_FROZEN" },
		);
		assert.equal(f.b.snapshot().seq, seq);
		assert.equal(
			f.b.validateSpawnAssignment({
				actor: parent,
				owner: "Storage",
				id: "a",
				requiredInputs: [f.contract],
			})?.id,
			"a",
		);
		assert.equal(
			f.b.snapshot().seq,
			seq,
			"preflight finds the existing assignment without rebinding or restarting",
		);
	} finally {
		f.cleanup();
	}
});

it("last frozen submission emits one Review GO while workers are still online; exit/replay is not a gate", () => {
	const f = setup();
	try {
		f.submit(storage, "a");
		assert.equal(f.deliveries.length, 0);
		const submitted = f.submit(domain, "b");
		const p = f.b.snapshot(),
			ready = p.channels[f.channel.id].readiness!;
		assert.equal(ready.ready, true);
		assert.equal(p.roles.storage.active, true);
		assert.equal(p.roles.domain.active, true);
		assert.equal(f.deliveries.filter((d) => d.delivery.targetRoleId === "coordinator").length, 1);
		assert.match(f.deliveries[0].signal.summary, /do not wait for worker exit/);
		assert.equal(f.deliveries[0].signal.artifactRefs.length, 2);
		assert.deepEqual(f.deliveries[0].signal.requiresAck, []);
		const reviewTasks = f.b.query({ actor: reviewer, kind: "tasks" }).items as any[];
		assert.equal(reviewTasks.length, 2);
		assert.deepEqual(reviewTasks[0].verification, ["Unit tests pass"]);
		assert.equal((f.b.query({ actor: storage, kind: "tasks" }).items as any[]).length, 1);
		f.b.transitionTask({
			actor: domain,
			taskId: "b",
			expectedRevision: 1,
			state: "submitted",
			outputs: submitted.event.payload.outputs,
			verification: ["Unit tests pass"],
			nextActions: [],
			idempotencyKey: "submit-b-1",
		});
		f.b.detachRole({
			actor: parent,
			roleId: "Storage",
			runId: storage.runId,
			outcome: "finished",
			idempotencyKey: "detach",
		});
		assert.equal(f.deliveries.filter((d) => d.delivery.targetRoleId === "coordinator").length, 1);
		const restored = CoordinationBroker.restore(f.entries, "autonomy", "branch");
		assert.equal(restored.snapshot().channels[f.channel.id].readiness?.signalId, ready.signalId);
	} finally {
		f.cleanup();
	}
});

it("review decisions atomically deliver reviewer-authored peer handoffs and replay without duplicates", () => {
	const f = setup();
	try {
		f.submit(storage, "a");
		f.submit(domain, "b");
		const before = f.b.snapshot().seq;
		const params = {
			actor: reviewer,
			taskId: "a",
			expectedRevision: 2,
			decision: "approved" as const,
			reason: "Storage: persistence and missing-file behavior verified.",
			nextActions: [],
			idempotencyKey: "approve-a",
		};
		const result = f.b.reviewTask(params);
		assert.equal(f.b.snapshot().seq, before + 1, "approval and peer handoff are one event");
		assert.equal(result.event.payload.state, "completed");
		assert.equal(result.event.actor.roleId, "reviewer");
		const notice = result.event.payload.reviewMessage!;
		assert.match(notice.summary, /Storage: persistence/);
		assert.deepEqual(notice.deliveries.map((d) => d.targetRoleId).sort(), [
			"coordinator",
			"domain",
			"storage",
		]);
		assert.equal(
			coordinationChatRecords(result.event, f.b.snapshot())[0].event.channel?.id,
			f.channel.id,
		);
		assert.equal(f.b.reviewTask(params).replayed, true);
		assert.equal(f.b.snapshot().seq, before + 1);
		assert.equal(
			(f.b.snapshot().tasks.a as any).reviewMessage,
			undefined,
			"delivery metadata is not copied into tasks",
		);
		const changes = f.b.reviewTask({
			actor: reviewer,
			taskId: "b",
			expectedRevision: 2,
			decision: "changes_requested",
			reason: "Domain: reject empty input.",
			idempotencyKey: "changes",
		});
		assert.match(changes.event.payload.reviewMessage!.summary, /CHANGES REQUESTED/);
		assert.equal(f.b.snapshot().channels[f.channel.id].readiness?.ready, false);
	} finally {
		f.cleanup();
	}
});

it("supersession withdraws stale GO and replacement submissions emit a fresh version without stale deliveries", () => {
	const f = setup();
	try {
		f.submit(storage, "a");
		f.submit(domain, "b");
		const old = f.b.snapshot().channels[f.channel.id].readiness!.signalId;
		const next = f.publish(storage, "output-a", "corrected", "corrected");
		let p = f.b.snapshot();
		assert.equal(p.channels[f.channel.id].readiness?.ready, false);
		assert.equal(p.signals[old].state, "superseded");
		assert.ok(
			Object.values(p.deliveries)
				.filter((d) => d.signalId === old)
				.every((d) => d.state === "resolved"),
		);
		f.b.transitionTask({
			actor: storage,
			taskId: "a",
			expectedRevision: 2,
			state: "in_progress",
			idempotencyKey: "rework",
		});
		f.b.transitionTask({
			actor: storage,
			taskId: "a",
			expectedRevision: 3,
			state: "submitted",
			outputs: [next],
			verification: ["Corrected tests pass"],
			nextActions: [],
			idempotencyKey: "resubmit",
		});
		p = f.b.snapshot();
		assert.equal(p.channels[f.channel.id].readiness?.ready, true);
		assert.equal(p.channels[f.channel.id].readiness?.revision, 3);
		const signal = p.signals[p.channels[f.channel.id].readiness!.signalId];
		assert.equal(signal.artifactRefs.find((ref) => ref.artifactId === "output-a")?.version, 2);
	} finally {
		f.cleanup();
	}
});

it("missing submission evidence wakes an actionable not-ready notice rather than a false GO", () => {
	const f = setup();
	try {
		f.submit(storage, "a");
		f.b.transitionTask({
			actor: domain,
			taskId: "b",
			expectedRevision: 1,
			state: "submitted",
			outputs: [],
			verification: [],
			nextActions: [],
			idempotencyKey: "empty-submit",
		});
		assert.equal(f.b.snapshot().channels[f.channel.id].readiness?.ready, false);
		assert.equal(f.deliveries.length, 2);
		assert.ok(
			f.deliveries.every(
				(d) => d.signal.kind === "attention" && d.signal.artifactRefs.length === 0,
			),
		);
		assert.match(f.deliveries[0].signal.summary, /Review not ready/);
	} finally {
		f.cleanup();
	}
});

it("scoped reviewers cannot inspect unrelated tasks, self-approve, or skip a closed handoff channel", () => {
	const f = setup();
	try {
		f.b.createTask({
			actor: parent,
			owner: "storage",
			id: "unrelated",
			title: "Unrelated",
			verification: ["not for this reviewer"],
			idempotencyKey: "unrelated",
		});
		assert.equal(
			(f.b.query({ actor: reviewer, kind: "tasks" }).items as any[]).some(
				(t) => t.id === "unrelated",
			),
			false,
		);
		f.b.createTask({
			actor: parent,
			owner: "reviewer",
			id: "r",
			title: "Report",
			start: true,
			idempotencyKey: "r",
		});
		f.submit(reviewer, "r");
		assert.throws(
			() =>
				f.b.reviewTask({
					actor: reviewer,
					taskId: "r",
					expectedRevision: 2,
					decision: "approved",
					reason: "Self approval",
					nextActions: [],
					idempotencyKey: "self",
				}),
			{ code: "UNAUTHORIZED" },
		);
		f.submit(storage, "a");
		f.submit(domain, "b");
		f.b.closeChannel({
			actor: parent,
			channelId: f.channel.id,
			expectedRevision: 1,
			idempotencyKey: "close",
		});
		const seq = f.b.snapshot().seq;
		assert.throws(
			() =>
				f.b.reviewTask({
					actor: reviewer,
					taskId: "a",
					expectedRevision: 2,
					decision: "approved",
					reason: "No peer handoff",
					nextActions: [],
					idempotencyKey: "closed",
				}),
			{ code: "UNAUTHORIZED" },
		);
		assert.equal(f.b.snapshot().seq, seq);
		assert.equal(f.b.snapshot().tasks.a.state, "submitted");
	} finally {
		f.cleanup();
	}
});

it("default integration command cannot silently enter live-model delegation suites", () => {
	const scripts = JSON.parse(
		readFileSync(new URL("../package.json", import.meta.url), "utf8"),
	).scripts;
	assert.doesNotMatch(scripts["test:integration"], /\*|subagent-lifecycle|tool-rendering/);
	assert.match(scripts["test:integration"], /coordination-autonomy\.test\.ts/);
});

it("schemas expose required action-specific fields and accept legacy flat preparation", async () => {
	const f = setup();
	try {
		const tools = new Map<string, any>(),
			pi: any = {
				registerTool(t: any) {
					tools.set(t.name, t);
				},
			};
		const ctx = () => ({ broker: f.b, actor: parent });
		registerCoordinationTools(pi, ctx, () => true);
		registerCoordinationWorkflowTools(pi, ctx);
		const channel = tools.get("subagent_channel"),
			signal = tools.get("subagent_signal");
		const send = {
			action: "send",
			channelId: f.channel.id,
			text: "Contract check",
			idempotencyKey: "send",
		};
		assert.equal(Value.Check(channel.parameters, { request: send }), false);
		assert.throws(
			() => channel.prepareArguments(send),
			/subagent_channel send requires expectedRevision/,
		);
		assert.equal(
			Value.Check(channel.parameters, { request: { ...send, expectedRevision: 1 } }),
			true,
		);
		assert.equal(
			Value.Check(channel.parameters, {
				request: {
					action: "create",
					channelId: "unsupported",
					name: "x",
					purpose: "x",
					members: [],
					idempotencyKey: "x",
				},
			}),
			false,
		);
		assert.equal(
			Value.Check(signal.parameters, {
				request: {
					action: "resolve",
					signalId: "s",
					expectedRevision: 1,
					summary: "unsupported",
					idempotencyKey: "x",
				},
			}),
			false,
		);
		const prepared = channel.prepareArguments({ ...send, expectedRevision: 1 });
		assert.equal(Value.Check(channel.parameters, prepared), true);
		const result = await channel.execute("call", prepared);
		assert.equal(result.details.event.type, "channel.message");
	} finally {
		f.cleanup();
	}
});

it("auto-exit waits for actual settlement, leaves abort/deferred runs open and closes once", async () => {
	const prior = process.env.PI_SUBAGENT_AUTO_EXIT;
	const credentials = process.env.PI_SUBAGENT_COORDINATION;
	process.env.PI_SUBAGENT_AUTO_EXIT = "1";
	delete process.env.PI_SUBAGENT_COORDINATION;
	try {
		const handlers = new Map<string, ((...args: any[]) => any)[]>(),
			entries: any[] = [];
		const pi: any = {
			on(name: string, fn: (...args: any[]) => any) {
				handlers.set(name, [...(handlers.get(name) ?? []), fn]);
			},
			registerTool() {},
			registerShortcut() {},
			appendEntry(...args: any[]) {
				entries.push(args);
			},
		};
		subagentDone(pi);
		let exits = 0;
		const ctx = {
			mode: "rpc",
			shutdown() {
				exits++;
			},
		};
		const emit = async (name: string, event: any = {}) => {
			for (const fn of handlers.get(name) ?? []) await fn(event, ctx);
		};
		await emit("agent_end", {
			messages: [{ role: "assistant", stopReason: "error", errorMessage: "Will retry" }],
		});
		assert.equal(exits, 0, "low-level run ending is not settlement");
		for (const stopReason of ["aborted", "deferred"]) {
			await emit("agent_end", { messages: [{ role: "assistant", stopReason }] });
			await emit("agent_settled");
			assert.equal(exits, 0);
		}
		await emit("agent_end", { messages: [{ role: "assistant", stopReason: "stop" }] });
		await emit("agent_settled");
		await emit("agent_settled");
		assert.equal(exits, 1);
		assert.equal(entries.filter((e) => e[0] === "subagent-run-closure").length, 1);
	} finally {
		if (prior === undefined) delete process.env.PI_SUBAGENT_AUTO_EXIT;
		else process.env.PI_SUBAGENT_AUTO_EXIT = prior;
		if (credentials === undefined) delete process.env.PI_SUBAGENT_COORDINATION;
		else process.env.PI_SUBAGENT_COORDINATION = credentials;
	}
});
