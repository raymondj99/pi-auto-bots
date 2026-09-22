import assert from "node:assert/strict";
import { join } from "node:path";
import type { Step } from "./worked-example.ts";

export function* childExample(directory: string, fast = false): Generator<Step, void, any> {
	let key = 0;
	function* call(name: string, args: any, error?: string): Generator<Step, any, any> {
		const result = yield {
			name,
			args: {
				...args,
				...(name.startsWith("subagent_") && name !== "subagent_team"
					? { idempotencyKey: `child-${++key}` }
					: {}),
			},
			error,
		};
		return result?.event?.payload ?? result;
	}
	const tasks = yield* call("subagent_team", { kind: "tasks", sinceCursor: 0 });
	const task = tasks.state.items[0];
	const signals = yield* call("subagent_team", { kind: "signals" });
	const ready = signals.state.items.find((s: any) => s.kind === "ready");
	const channels = yield* call("subagent_team", { kind: "channels" });
	const channelId = channels.state.items[0].id;
	yield* call("subagent_team", { kind: "roles" });
	yield* call("subagent_channel", {
		action: "send",
		channelId,
		expectedRevision: 1,
		text: "Assignment acknowledged",
	});
	yield* call("subagent_signal", {
		action: "acknowledge",
		signalId: ready.id,
		expectedRevision: 1,
	});
	yield* call("subagent_task", {
		action: "start",
		taskId: task.id,
		expectedRevision: task.revision,
	});
	yield* call(
		"subagent_permit",
		{ action: "create_budget", budgetId: "unauthorized", limit: 1 },
		"UNAUTHORIZED",
	);
	yield* call(
		"subagent_signal",
		{ action: "emit", kind: "go", summary: "Cannot self-authorize" },
		"UNAUTHORIZED",
	);
	if (fast) {
		yield* call("bash", { command: "printf 'ordinary-shell-needs-no-permit\\n'" });
	} else {
		yield* call("bash", { command: "printf 'must-not-run\\n'" }, "permit");
		const permit = yield* call("subagent_permit", {
			action: "issue",
			taskId: task.id,
			budgetId: "child-budget",
			reservationId: task.leases[0],
			operation: "shell",
		});
		yield* call("subagent_permit", { action: "arm", permitId: permit.id, operation: "shell" });
	}
	const shell = yield* call("bash", { command: "printf 'child-permit-ok\\n'" });
	assert.match(shell.content.map((c: any) => c.text ?? "").join(""), /child-permit-ok/);
	yield* call("write", { path: join(directory, "child-result.txt"), content: "handoff-ok\n" });
	const output = yield* call("subagent_artifact", {
		action: fast ? "publish" : "seal",
		artifactId: "child-output",
		sourcePath: fast ? "child-result.txt" : join(directory, "child-result.txt"),
		...(fast
			? { version: 1, reason: "Verified child output" }
			: { storeDir: join(directory, "store") }),
	});
	const ref = { artifactId: output.artifactId, version: output.version, digest: output.digest };
	if (!fast) {
		yield* call(
			"subagent_artifact",
			{ action: "freeze", artifactId: output.artifactId, version: output.version },
			"UNAUTHORIZED",
		);
		yield* call("subagent_channel", {
			action: "send",
			channelId,
			expectedRevision: 1,
			text: "Request reviewer freeze of child-output@1",
		});
	}
	yield* call("subagent_task", {
		action: "submit",
		taskId: task.id,
		expectedRevision: task.revision + 1,
		outputs: [ref],
		verification: ["Actual child Pi shell returned child-permit-ok"],
		nextActions: ["Coordinator review"],
	});
	yield* call(
		"subagent_task",
		{
			action: "approve",
			taskId: task.id,
			expectedRevision: task.revision + 2,
			reason: "Cannot self-approve",
			nextActions: [],
		},
		"UNAUTHORIZED",
	);
	yield* call("subagent_task", {
		action: "request_review",
		taskId: task.id,
		expectedRevision: task.revision + 2,
	});
	yield* call("subagent_signal", {
		action: "emit",
		kind: "artifact_published",
		severity: "action",
		summary: "Output ready for coordinator",
		artifactRefs: [ref],
		targets: [{ roleId: "coordinator", channelId, taskId: task.id }],
		requiresAck: ["coordinator"],
	});
	yield* call("subagent_task", {
		action: "handoff",
		taskId: task.id,
		expectedRevision: task.revision + 3,
		owner: "coordinator",
		reason: "Transfer verified output for integration",
		nextActions: ["Review output"],
	});
}
