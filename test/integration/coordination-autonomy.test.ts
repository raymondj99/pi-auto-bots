import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { it } from "node:test";
import { buildCoordinationBootstrap } from "../../src/coordination/bootstrap.ts";
import { CoordinationBroker } from "../../src/coordination/broker.ts";
import { publicMutationResult } from "../../src/coordination/results.ts";
import { TransportBroker } from "../../src/transport.ts";
import { PI_CLI } from "./pi-cli.ts";

it("real Pi: Review GO launches review before worker exit; nested schemas, bound inputs and autonomous closure", {
	timeout: 40000,
}, async () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-autonomy-runtime-"));
	const entries: any[] = [],
		processes: ChildProcess[] = [],
		outputs: string[] = [];
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
		{ workflowMode: "fast", artifactStoreDir: join(directory, "artifacts") },
	);
	const actor = (roleId: string) => ({ roleId, runId: `${roleId}-run`, generation: 1 });
	const coordinator = actor("coordinator");
	b.admitRole({ roleId: "Coordinator", capabilities: ["coordinator"], idempotencyKey: "root" });
	b.bindRole({ roleId: "Coordinator", runId: coordinator.runId, idempotencyKey: "root-bind" });
	for (const role of ["Worker", "Other", "Reviewer"])
		b.admitRole({
			actor: coordinator,
			roleId: role,
			capabilities: role === "Reviewer" ? ["worker", "reviewer"] : ["worker"],
			idempotencyKey: role,
		});
	for (const role of ["worker", "other"])
		b.bindRole({ roleId: role, runId: actor(role).runId, idempotencyKey: `${role}-bind` });
	const sourcePath = join(directory, "contract.txt");
	writeFileSync(sourcePath, "immutable contract\n");
	const artifact = b.sealArtifact({
		actor: coordinator,
		artifactId: "contract",
		sourcePath,
		freeze: true,
		idempotencyKey: "contract",
	}).event.payload;
	const contract = {
		artifactId: artifact.artifactId,
		version: artifact.version,
		digest: artifact.digest,
	};
	const channel = b.createChannel({
		actor: coordinator,
		name: "integration",
		purpose: "Review scheduling regression",
		members: ["Coordinator", "Worker", "Other", "Reviewer"].map((roleId) => ({
			roleId,
			mode: "participate",
		})),
		review: { taskIds: ["worker-task", "other-task"], reviewerRoleId: "Reviewer" },
		idempotencyKey: "channel",
	}).event.payload;
	for (const role of ["worker", "other"])
		b.assignDefaultTask({
			actor: coordinator,
			owner: role,
			id: `${role}-task`,
			title: role,
			requiredInputs: [contract],
			idempotencyKey: `${role}-task`,
		});
	b.transitionTask({
		actor: actor("other"),
		taskId: "other-task",
		expectedRevision: 1,
		state: "submitted",
		outputs: [contract],
		verification: ["Other fixture verified"],
		nextActions: [],
		idempotencyKey: "other-submit",
	});
	const transport = new TransportBroker(
		() => {},
		undefined,
		undefined,
		(from, method, params) => {
			const roleId = from.replace(/-run$/, ""),
				who = actor(roleId);
			if (method === "bootstrap")
				return {
					bootstrap: buildCoordinationBootstrap(b.snapshot(), roleId, 0, "fast"),
					actor: who,
					policy: { workflowMode: "fast", protectedOperations: [] },
				};
			if (method === "digest") return b.digest(who, Number(params.sinceSeq ?? 0));
			return publicMutationResult((b as any)[method]({ ...params, actor: who }));
		},
	);
	await transport.start();
	const launch = (role: string) => {
		const cwd = join(directory, role);
		mkdirSync(cwd);
		const session = join(cwd, "session.jsonl");
		const credentials = transport.add(actor(role).runId, role, session, false);
		const env: NodeJS.ProcessEnv = { ...process.env };
		for (const key of Object.keys(env))
			if (
				key.startsWith("PI_SUBAGENT_") ||
				key.startsWith("PI_COORDINATION_") ||
				key === "PI_DENY_TOOLS"
			)
				delete env[key];
		Object.assign(env, {
			PI_OFFLINE: "1",
			PI_TELEMETRY: "0",
			PI_CODING_AGENT_DIR: join(cwd, "config"),
			PI_COORDINATION_EXAMPLE_DIR: cwd,
			PI_COORDINATION_AUTONOMY_ROLE: role,
			PI_SUBAGENT_COORDINATION: JSON.stringify(credentials),
			PI_SUBAGENT_AUTO_EXIT: "1",
			PI_SUBAGENT_INTERACTIVE: "0",
			PI_SUBAGENT_ID: actor(role).runId,
			PI_SUBAGENT_SESSION: session,
		});
		const child = spawn(
			PI_CLI,
			[
				"--mode",
				"rpc",
				"--no-extensions",
				"--no-skills",
				"--no-context-files",
				"--session",
				session,
				"--provider",
				"coordination-test",
				"--model",
				"scripted",
				"-e",
				resolve("src/subagent-done.ts"),
				"-e",
				resolve("test/fixtures/coordination/runtime-driver.ts"),
			],
			{ cwd, env, stdio: ["pipe", "pipe", "pipe"] },
		);
		processes.push(child);
		child.stdout!.on("data", (data) => outputs.push(`${role}: ${data}`));
		child.stderr!.on("data", (data) => outputs.push(`${role}: ${data}`));
		const exited = once(child, "exit").then(([code]) => {
			assert.equal(code, 0, outputs.join("").slice(-10000));
			b.detachRole({
				actor: coordinator,
				roleId: role,
				runId: actor(role).runId,
				outcome: "finished",
				idempotencyKey: `${role}-detach`,
			});
			const log = readFileSync(session, "utf8")
				.trim()
				.split("\n")
				.map((line) => JSON.parse(line));
			assert.equal(log.filter((e) => e.customType === "subagent-run-closure").length, 1);
			assert.equal(
				log.some((e) => e.message?.toolName === "subagent_done"),
				false,
			);
			assert.equal(JSON.parse(readFileSync(join(cwd, "report.json"), "utf8")).pass, true);
		});
		child.stdin!.write(
			JSON.stringify({ type: "prompt", message: "Execute provider-free autonomy fixture" }) + "\n",
		);
		return { child, exited };
	};
	let reviewerRun: ReturnType<typeof launch> | undefined;
	let worker: ChildProcess | undefined;
	let goBeforeExit = false;
	let callbackError: unknown;
	b.setDeliverySink((delivery, signal) => {
		if (delivery.targetRoleId !== "coordinator" || signal.kind !== "ready" || reviewerRun) return;
		try {
			goBeforeExit = !!worker && worker.exitCode === null;
			b.bindRole({
				roleId: "Reviewer",
				runId: actor("reviewer").runId,
				idempotencyKey: "reviewer-bind",
			});
			b.assignDefaultTask({
				actor: coordinator,
				owner: "reviewer",
				id: "reviewer-task",
				title: "Review",
				requiredInputs: signal.artifactRefs,
				idempotencyKey: "reviewer-task",
			});
			reviewerRun = launch("reviewer");
		} catch (error) {
			callbackError = error;
		}
	});
	const timeout = setTimeout(() => {
		for (const child of processes) child.kill("SIGKILL");
	}, 30000);
	try {
		const run = launch("worker");
		worker = run.child;
		await run.exited;
		assert.ifError(callbackError);
		assert.ok(reviewerRun, outputs.join("").slice(-10000));
		await reviewerRun.exited;
		assert.equal(goBeforeExit, true);
		const task = b.snapshot().tasks["reviewer-task"];
		b.reviewTask({
			actor: coordinator,
			taskId: task.id,
			expectedRevision: task.revision,
			decision: "approved",
			reason: "Fixture and peer decisions verified",
			nextActions: [],
			idempotencyKey: "reviewer-approve",
		});
		assert.ok(Object.values(b.snapshot().tasks).every((t) => t.state === "completed"));
		const decisions = entries
			.map((e) => e.data.event)
			.filter((e) => e.actor.roleId === "reviewer" && e.payload.reviewMessage);
		assert.equal(decisions.length, 2);
		assert.ok(decisions.every((e) => e.payload.reviewMessage.id === channel.id));
		assert.equal(
			entries.filter(
				(e) => e.data.event.type === "review.readiness" && e.data.event.payload.readiness.ready,
			).length,
			1,
		);
	} finally {
		clearTimeout(timeout);
		for (const child of processes) if (child.exitCode === null) child.kill("SIGKILL");
		transport.close();
	}
});
