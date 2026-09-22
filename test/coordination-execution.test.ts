import assert from "node:assert/strict";
import { it } from "node:test";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { installProtectedExecution } from "../src/coordination/execution.ts";
import type { CoordinationToolContext } from "../src/coordination/tools.ts";

function harness() {
	const handlers = new Map<string, (event: any) => any>();
	const dispatched: any[] = [],
		settled: any[] = [];
	let failDispatch = false;
	let replayDispatch = false;
	const pi = {
		on(name: string, handler: (event: any) => any) {
			handlers.set(name, handler);
		},
	} as unknown as ExtensionAPI;
	const broker = {
		async getPermit() {
			return { id: "permit", state: "reserved", operation: "shell", expiresAt: Date.now() + 10000 };
		},
		async dispatchPermit(params: unknown) {
			if (failDispatch) throw new Error("changed generation");
			dispatched.push(params);
			return { replayed: replayDispatch };
		},
		async settlePermit(params: unknown) {
			settled.push(params);
		},
	};
	const context = {
		broker,
		actor: { roleId: "worker", runId: "run", generation: 1 },
	} as unknown as CoordinationToolContext;
	const arm = installProtectedExecution(
		pi,
		() => context,
		() => ["shell"],
		() => true,
	);
	return {
		arm,
		dispatched,
		settled,
		emit: (name: string, event: any) => handlers.get(name)!(event),
		fail: () => {
			failDispatch = true;
		},
		replay: () => {
			replayDispatch = true;
		},
	};
}

it("protected execution blocks plain shell calls and uses standard tool arguments after explicit arm", async () => {
	const h = harness();
	const call = { toolName: "bash", toolCallId: "attempt", input: { command: "echo ok" } };
	assert.equal((await h.emit("tool_call", call)).block, true);
	await h.arm("permit", "shell");
	assert.equal(await h.emit("tool_call", call), undefined);
	assert.equal(h.dispatched.length, 1);
	assert.equal(h.dispatched[0].operation, "shell");
	assert.deepEqual(call.input, { command: "echo ok" });
	assert.equal((await h.emit("tool_call", { ...call, toolCallId: "reuse" })).block, true);
	await h.emit("tool_execution_end", { toolCallId: "attempt", isError: false });
	assert.equal(h.settled[0].outcome, "settled");
});

it("failed tools settle as outcome unknown, and dispatch rejection blocks execution", async () => {
	const h = harness();
	await h.arm("permit", "shell");
	await h.emit("tool_call", { toolName: "bash", toolCallId: "attempt", input: {} });
	await h.emit("tool_execution_end", { toolCallId: "attempt", isError: true });
	assert.equal(h.settled[0].outcome, "outcome_unknown");
	await h.arm("permit", "shell");
	h.fail();
	assert.equal(
		(await h.emit("tool_call", { toolName: "powershell", toolCallId: "rejected", input: {} }))
			.block,
		true,
	);
});

it("a replayed dispatch response never invokes the side effect again", async () => {
	const h = harness();
	await h.arm("permit", "shell");
	h.replay();
	const result = await h.emit("tool_call", {
		toolName: "bash",
		toolCallId: "same-call",
		input: {},
	});
	assert.equal(result.block, true);
	assert.match(result.reason, /already dispatched/);
});

it("shutdown drops armed permits and unrelated operations never consume them", async () => {
	const h = harness();
	await assert.rejects(h.arm("permit", "deploy"));
	await h.arm("permit", "shell");
	assert.equal((await h.arm("permit", "shell")).armed, true, "retrying arm is idempotent");
	await h.emit("tool_call", { toolName: "read", toolCallId: "read", input: {} });
	assert.equal(h.dispatched.length, 0);
	h.emit("session_shutdown", {});
	assert.equal(
		(await h.emit("tool_call", { toolName: "bash", toolCallId: "after-shutdown", input: {} }))
			.block,
		true,
	);
});
