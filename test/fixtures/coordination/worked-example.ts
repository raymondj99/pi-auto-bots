import assert from "node:assert/strict";
import { join } from "node:path";

export interface Step {
	name: string;
	args: any;
	error?: string;
	check?: (result: any) => void;
}
export function* workedExample(directory: string, reloaded = false): Generator<Step, void, any> {
	let key = 0;
	function* call(name: string, args: any, error?: string): Generator<Step, any, any> {
		const result = yield {
			name,
			args: {
				...args,
				...(name.startsWith("subagent_") && name !== "subagent_team"
					? { idempotencyKey: `example-${reloaded}-${++key}` }
					: {}),
			},
			error,
		};
		return result?.event?.payload ?? result;
	}
	const task = (args: any, error?: string) => call("subagent_task", args, error);
	const permit = (args: any, error?: string) => call("subagent_permit", args, error);
	const signal = (args: any, error?: string) => call("subagent_signal", args, error);
	const artifact = (args: any, error?: string) => call("subagent_artifact", args, error);
	const team = (args: any) => call("subagent_team", args);
	if (reloaded) {
		const tasks = yield* team({ kind: "tasks", id: "example-task" });
		assert.equal(tasks.state.items[0].state, "completed");
		const roles = yield* team({ kind: "roles", id: "example-worker" });
		assert.equal(roles.state.items[0].active, false);
		yield* call("subagent_role", {
			action: "rebind",
			roleId: "example-worker",
			runId: "after-reload",
		});
		const budget = yield* team({ kind: "budgets", id: "example-budget" });
		assert.equal(budget.state.items[0].used, 1);
		yield* call("subagent_checkpoint", {});
		return;
	}
	yield* team({ kind: "overview" });
	yield* call("subagent_role", {
		action: "admit",
		roleId: "example-worker",
		capabilities: ["worker"],
	});
	yield* call("subagent_role", {
		action: "rebind",
		roleId: "example-worker",
		runId: "local-fixture-run",
	});
	const channel = yield* call("subagent_channel", {
		action: "create",
		name: "example",
		purpose: "Worked example",
		members: [
			{ roleId: "coordinator", mode: "participate" },
			{ roleId: "example-worker", mode: "participate" },
		],
		delivery: "urgent",
	});
	yield* call("subagent_channel", {
		action: "send",
		channelId: channel.id,
		expectedRevision: 1,
		text: "Assignment context",
		delivery: "next_turn",
	});
	yield* call("subagent_channel", {
		action: "update",
		channelId: channel.id,
		expectedRevision: 1,
		delivery: "digest",
	});
	yield* call(
		"subagent_channel",
		{ action: "send", channelId: channel.id, expectedRevision: 1, text: "stale" },
		"STALE_REVISION",
	);
	const sourcePath = join(directory, "input.txt"),
		storeDir = join(directory, "store");
	yield* call("write", { path: sourcePath, content: "worked-example-input\n" });
	const input = yield* artifact({
		action: "seal",
		artifactId: "example-input",
		sourcePath,
		storeDir,
	});
	const ref = { artifactId: input.artifactId, version: input.version, digest: input.digest };
	yield* permit({ action: "create_budget", budgetId: "example-budget", limit: 1 });
	const budget = yield* permit({ action: "reserve", budgetId: "example-budget" });
	const reservationId = budget.reservationId;
	yield* task({
		action: "create",
		taskId: "example-task",
		title: "Pi worked example",
		owner: "coordinator",
		requiredInputs: [ref],
		leases: [reservationId],
		verification: ["Will verify permitted shell output"],
		nextActions: ["Execute smoke test"],
	});
	yield* task({ action: "ready", taskId: "example-task", expectedRevision: 1 }, "INPUT_NOT_FROZEN");
	yield* artifact({ action: "freeze", artifactId: input.artifactId, version: input.version });
	yield* task({
		action: "bind_inputs",
		taskId: "example-task",
		expectedRevision: 1,
		requiredInputs: [ref],
		leases: [reservationId],
		reason: "Use exact frozen input",
	});
	const hold = yield* signal({
		action: "emit",
		kind: "hold",
		severity: "blocking",
		summary: "Explicit barrier",
		targets: [{ taskId: "example-task", channelId: channel.id }],
		requiresAck: ["coordinator"],
	});
	yield* task(
		{ action: "ready", taskId: "example-task", expectedRevision: 2 },
		"UNRESOLVED_BLOCKER",
	);
	yield* signal({ action: "resolve", signalId: hold.id, expectedRevision: 1 }, "ACK_REQUIRED");
	yield* signal({ action: "acknowledge", signalId: hold.id, expectedRevision: 1 });
	yield* signal({ action: "resolve", signalId: hold.id, expectedRevision: 2 });
	const old = yield* signal({
		action: "emit",
		kind: "ready",
		severity: "info",
		summary: "Old plan",
		targets: [{ taskId: "example-task" }],
	});
	const next = yield* signal({
		action: "supersede",
		signalId: old.id,
		expectedRevision: 1,
		summary: "New plan",
		targets: [{ roleId: "coordinator" }],
		requiresAck: [],
		kind: "decision",
	});
	yield* signal({ action: "resolve", signalId: next.id, expectedRevision: 1 });
	yield* task({ action: "ready", taskId: "example-task", expectedRevision: 2 });
	yield* task({ action: "start", taskId: "example-task", expectedRevision: 3 });
	yield* call("bash", { command: "printf 'must-not-execute\\n'" }, "permit");
	const issued = yield* permit({
		action: "issue",
		taskId: "example-task",
		budgetId: "example-budget",
		reservationId,
		operation: "shell",
	});
	yield* permit({ action: "arm", permitId: issued.id, operation: "shell" });
	const shell = yield* call("bash", { command: "printf 'permit-ok\\n'" });
	assert.match(shell.content.map((c: any) => c.text ?? "").join(""), /permit-ok/);
	yield* permit(
		{ action: "arm", permitId: issued.id, operation: "shell" },
		"unavailable|not.*usable|reserved|settled",
	);
	yield* permit({ action: "reserve", budgetId: "example-budget" }, "BUDGET_EXHAUSTED");
	const outputPath = join(directory, "result.txt");
	yield* call("write", { path: outputPath, content: "handoff-ok\n" });
	const output = yield* artifact({
		action: "seal",
		artifactId: "example-output",
		sourcePath: outputPath,
		storeDir,
	});
	yield* artifact({ action: "freeze", artifactId: output.artifactId, version: output.version });
	const outputRef = {
		artifactId: output.artifactId,
		version: output.version,
		digest: output.digest,
	};
	yield* task({
		action: "submit",
		taskId: "example-task",
		expectedRevision: 4,
		outputs: [outputRef],
		verification: ["Actual Pi Bash returned permit-ok"],
		nextActions: [],
	});
	yield* task({ action: "request_review", taskId: "example-task", expectedRevision: 5 });
	yield* task(
		{ action: "complete", taskId: "example-task", expectedRevision: 6, nextActions: [] },
		"review|Completion",
	);
	yield* task({
		action: "changes_requested",
		taskId: "example-task",
		expectedRevision: 6,
		reason: "Exercise review revision",
	});
	yield* task({ action: "unblock", taskId: "example-task", expectedRevision: 7 });
	// A consumed attempt cannot start again. Reserve a distinct budget for review work,
	// never to bypass the exhausted one-attempt smoke budget.
	yield* task({ action: "start", taskId: "example-task", expectedRevision: 8 }, "LEASE_REQUIRED");
	yield* permit({ action: "create_budget", budgetId: "review-budget", limit: 2 });
	yield* permit({
		action: "reserve",
		budgetId: "review-budget",
		taskId: "example-task",
		expectedRevision: 8,
	});
	yield* task({ action: "start", taskId: "example-task", expectedRevision: 9 });
	yield* task({
		action: "block",
		taskId: "example-task",
		expectedRevision: 10,
		reason: "Exercise block",
	});
	yield* task({ action: "unblock", taskId: "example-task", expectedRevision: 11 });
	yield* task({ action: "start", taskId: "example-task", expectedRevision: 12 });
	yield* task({
		action: "submit",
		taskId: "example-task",
		expectedRevision: 13,
		verification: ["Verified sealed output and successful Pi shell"],
		nextActions: [],
	});
	yield* task({
		action: "approve",
		taskId: "example-task",
		expectedRevision: 14,
		reason: "Verified exact frozen output",
		nextActions: [],
	});
	yield* task({
		action: "complete",
		taskId: "example-task",
		expectedRevision: 15,
		nextActions: [],
	});
	// Explicit ownership handoff and recovery, not a prose-only assignment.
	yield* task({ action: "create", taskId: "handoff-task", title: "Handoff", owner: "coordinator" });
	yield* task({
		action: "handoff",
		taskId: "handoff-task",
		expectedRevision: 1,
		owner: "example-worker",
		reason: "Next stage",
		nextActions: ["Review contract"],
	});
	yield* call("subagent_role", { action: "detach", roleId: "example-worker" });
	yield* call("subagent_role", {
		action: "rebind",
		roleId: "example-worker",
		runId: "replacement-run",
	});
	yield* task({
		action: "cancel",
		taskId: "handoff-task",
		expectedRevision: 2,
		reason: "Example finished",
	});
	yield* task({ action: "create", taskId: "failed-task", title: "Failure", owner: "coordinator" });
	yield* task({ action: "block", taskId: "failed-task", expectedRevision: 1 });
	yield* task({
		action: "fail",
		taskId: "failed-task",
		expectedRevision: 2,
		reason: "Intentional terminal failure",
	});
	const spare = yield* permit({ action: "reserve", budgetId: "review-budget" });
	const spareId = spare.reservationId;
	yield* permit({ action: "release", budgetId: "review-budget", reservationId: spareId });
	yield* call("write", { path: sourcePath, content: "corrected-input\n" });
	yield* artifact({ action: "seal", artifactId: "example-input", sourcePath, storeDir });
	yield* artifact({ action: "freeze", artifactId: "example-input", version: 2 });
	yield* artifact({
		action: "supersede",
		artifactId: "example-input",
		previousVersion: 1,
		successorVersion: 2,
		policy: "cancel",
		reason: "Correct the contract",
	});
	yield* artifact({
		action: "invalidate",
		artifactId: "example-input",
		version: 2,
		policy: "cancel",
		reason: "End of example",
	});
	// Cancellation, legacy dispatch alias, failed execution and audited reconciliation.
	yield* permit({ action: "create_budget", budgetId: "recovery-budget", limit: 2 });
	yield* task({
		action: "create",
		taskId: "attempt-task",
		title: "Recovery paths",
		owner: "coordinator",
	});
	const attemptBudget = yield* permit({
		action: "reserve",
		budgetId: "recovery-budget",
		taskId: "attempt-task",
		expectedRevision: 1,
	});
	const unusedId = attemptBudget.reservationId;
	yield* task({ action: "ready", taskId: "attempt-task", expectedRevision: 2 });
	yield* task({ action: "start", taskId: "attempt-task", expectedRevision: 3 });
	const unused = yield* permit({
		action: "issue",
		taskId: "attempt-task",
		budgetId: "recovery-budget",
		reservationId: unusedId,
		operation: "shell",
	});
	yield* permit({ action: "settle", permitId: unused.id, outcome: "cancelled_before_dispatch" });
	const retryBudget = yield* permit({
		action: "reserve",
		budgetId: "recovery-budget",
		taskId: "attempt-task",
		expectedRevision: 4,
	});
	const retryId = retryBudget.reservationId;
	const retry = yield* permit({
		action: "issue",
		taskId: "attempt-task",
		budgetId: "recovery-budget",
		reservationId: retryId,
		operation: "shell",
	});
	yield* permit({ action: "dispatch", permitId: retry.id, operation: "shell" });
	yield* call("bash", { command: "exit 1" }, "code 1");
	const unknown = yield* team({ kind: "permits", id: retry.id });
	assert.equal(unknown.state.items[0].state, "outcome_unknown");
	yield* permit({
		action: "reconcile",
		permitId: retry.id,
		outcome: "settled",
		reason: "Local command exited; no uncertain side effect remains",
	});
	yield* task({
		action: "cancel",
		taskId: "attempt-task",
		expectedRevision: 5,
		reason: "Recovery example finished",
	});
	yield* call("subagent_channel", { action: "close", channelId: channel.id, expectedRevision: 2 });
	for (const kind of [
		"roles",
		"channels",
		"tasks",
		"signals",
		"artifacts",
		"budgets",
		"permits",
		"deliveries",
		"audit",
	])
		yield* team({ kind, limit: 100 });
	yield* team({ kind: "tasks", id: "example-task", field: "outputs" });
	yield* call("subagent_checkpoint", {});
}
