/** Real Pi/socket peer lifecycle; scripted provider only, no external model or coordinator agent. */

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { it } from "node:test";
import { buildCoordinationBootstrap } from "../../src/coordination/bootstrap.ts";
import { CoordinationBroker } from "../../src/coordination/broker.ts";
import { publicMutationResult } from "../../src/coordination/results.ts";
import { installCoordinationDeliveryRouting } from "../../src/coordination/routing.ts";
import { TransportBroker } from "../../src/transport.ts";
import { PI_CLI } from "./pi-cli.ts";

it("real Pi peers: parked reviewer, channel Q&A, correction/re-review and release without coordinator intervention", {
	timeout: 35000,
}, async () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-peer-runtime-"));
	const entries: any[] = [],
		processes: ChildProcess[] = [],
		output: string[] = [],
		rootMessages: any[] = [];
	const actor = (roleId: string) => ({ roleId, runId: roleId, generation: 1 });
	const b = new CoordinationBroker(
		"peers",
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
	for (const roleId of ["coordinator", "reviewer", "worker", "other"]) {
		b.admitRole({
			...(roleId === "coordinator" ? {} : { actor: actor("coordinator") }),
			roleId,
			capabilities:
				roleId === "coordinator"
					? ["coordinator"]
					: roleId === "reviewer"
						? ["worker", "reviewer"]
						: ["worker"],
			idempotencyKey: `${roleId}-admit`,
		});
		b.bindRole({ roleId, runId: roleId, idempotencyKey: `${roleId}-bind` });
	}
	const sourcePath = join(directory, "contract.txt");
	writeFileSync(sourcePath, "Normalize negative zero.\n");
	const a = b.sealArtifact({
		actor: actor("coordinator"),
		artifactId: "contract",
		sourcePath,
		freeze: true,
		idempotencyKey: "contract",
	}).event.payload;
	const contract = { artifactId: a.artifactId, version: a.version, digest: a.digest };
	b.createChannel({
		actor: actor("coordinator"),
		name: "integration",
		purpose: "Direct questions, findings and corrections",
		members: ["coordinator", "reviewer", "worker", "other"].map((roleId) => ({
			roleId,
			mode: "participate",
		})),
		review: {
			taskIds: ["worker", "other"],
			reviewerRoleId: "reviewer",
			reviewTaskId: "review",
			maxCorrections: 1,
		},
		idempotencyKey: "channel",
	});
	for (const role of ["reviewer", "worker", "other"])
		b.assignDefaultTask({
			actor: actor("coordinator"),
			owner: role,
			title: role,
			id: role === "reviewer" ? "review" : role,
			requiredInputs: [contract],
			idempotencyKey: `${role}-task`,
		});
	const setupSeq = b.snapshot().seq;
	const parked = Promise.withResolvers<void>();
	const transport = new TransportBroker(
		() => {},
		undefined,
		(from, deliveryId) =>
			b.receipt({
				actor: actor(from),
				deliveryId,
				state: "delivered",
				idempotencyKey: `receipt-${deliveryId}`,
			}),
		(from, method, params) => {
			if (method === "bootstrap")
				return {
					bootstrap: buildCoordinationBootstrap(b.snapshot(), from, 0, "fast"),
					actor: actor(from),
					peerLifecycleSupported: true,
					policy: { workflowMode: "fast", protectedOperations: [] },
				};
			if (method === "digest") return b.digest(actor(from), Number(params.sinceSeq ?? 0));
			const result = (b as any)[method]({ ...params, actor: actor(from) });
			if (
				method === "runDisposition" &&
				from === "reviewer" &&
				result.phase === "waiting_for_submissions"
			)
				parked.resolve();
			return publicMutationResult(result);
		},
		(from) => b.retryPendingDeliveries(from),
	);
	await transport.start();
	const dispose = installCoordinationDeliveryRouting(
		{
			on() {},
			sendMessage(message: any) {
				rootMessages.push(message);
			},
		} as any,
		b,
		async (request) => transport.request("parent", request),
	);
	const launch = (role: string) => {
		const cwd = join(directory, role);
		mkdirSync(cwd);
		const session = join(cwd, "session.jsonl");
		const credentials = transport.add(role, role, session, false);
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
			PI_PEER_TEST_ROLE: role,
			PI_SUBAGENT_COORDINATION: JSON.stringify(credentials),
			PI_SUBAGENT_AUTO_EXIT: "1",
			PI_SUBAGENT_INTERACTIVE: "0",
			PI_SUBAGENT_ID: role,
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
				"peer-fixture",
				"--model",
				"scripted",
				"-e",
				resolve("src/subagent-done.ts"),
				"-e",
				resolve("test/fixtures/coordination/peer-driver.ts"),
			],
			{ cwd, env, stdio: ["pipe", "pipe", "pipe"] },
		);
		processes.push(child);
		child.stdout!.on("data", (s) => output.push(`${role}: ${s}`));
		child.stderr!.on("data", (s) => output.push(`${role}: ${s}`));
		const exited = once(child, "exit").then(([code]) => {
			assert.equal(code, 0, output.join("").slice(-8000));
			assert.equal(
				existsSync(join(cwd, "failure.json")),
				false,
				existsSync(join(cwd, "failure.json"))
					? readFileSync(join(cwd, "failure.json"), "utf8")
					: "",
			);
			const log = readFileSync(session, "utf8")
				.trim()
				.split("\n")
				.map((line) => JSON.parse(line));
			const bootstrap = log.findIndex((e) => e.customType === "subagent_coordination_bootstrap");
			const firstTool = log.findIndex((e) => e.message?.role === "toolResult");
			assert.ok(
				bootstrap >= 0 && bootstrap < firstTool,
				"reconnect messages cannot outrun caller identity/policy bootstrap",
			);
			assert.equal(log.filter((e) => e.customType === "subagent-run-closure").length, 1);
			assert.equal(
				log.some((e) => e.message?.toolName === "subagent_done"),
				false,
			);
			b.detachRole({
				actor: actor("coordinator"),
				roleId: role,
				runId: role,
				outcome: "finished",
				idempotencyKey: `${role}-detach`,
			});
			return log;
		});
		// Attach immediately: a failure during another child's run must not be unhandled.
		void exited.catch(() => {});
		child.stdin!.write(
			JSON.stringify({
				type: "prompt",
				message: "Execute the deterministic peer lifecycle fixture",
			}) + "\n",
		);
		return { child, exited };
	};
	const timer = setTimeout(() => {
		for (const child of processes) child.kill("SIGKILL");
	}, 25000);
	try {
		const reviewer = launch("reviewer");
		await Promise.race([
			parked.promise,
			reviewer.exited.then(() => {
				throw new Error("Reviewer exited instead of parking");
			}),
		]);
		const worker = launch("worker"),
			other = launch("other");
		const logs = await Promise.all([reviewer.exited, worker.exited, other.exited]);
		const events = entries.map((e) => e.data.event);
		const reviewEvents = events.filter((e) => e.type === "review.readiness");
		assert.equal(
			reviewEvents.filter((e) => e.payload.task?.state === "in_progress").length,
			2,
			"initial review plus corrected review activated by broker",
		);
		assert.equal(
			rootMessages.length,
			0,
			"no coordinator turn for GO, channel Q&A, correction or release",
		);
		assert.equal(
			events.filter(
				(e) => e.seq > setupSeq && e.actor.roleId === "coordinator" && e.type !== "role.detached",
			).length,
			0,
		);
		assert.ok(
			logs[0].some(
				(e) => e.customType === "subagent-run-wait" && e.data.phase === "waiting_for_submissions",
			),
		);
		assert.equal(b.snapshot().artifacts["worker-output"].length, 2);
		assert.equal(b.snapshot().tasks.review.state, "review");
		assert.equal(b.snapshot().tasks.review.reviewSatisfied, false);
		const conversations = events.filter((e) => e.type === "channel.message");
		assert.ok(conversations.some((e) => e.actor.roleId === "reviewer"));
		assert.ok(conversations.some((e) => e.actor.roleId === "worker"));
		for (const message of conversations) {
			const recipient = message.actor.roleId === "reviewer" ? "worker" : "reviewer";
			assert.ok(
				Object.values(b.snapshot().deliveries).some(
					(d) =>
						d.signalId === message.payload.signal.id &&
						d.targetRoleId === recipient &&
						d.state === "delivered",
				),
				"manual peer context has an injection receipt, not merely a queued notice",
			);
		}
		const correction = events.find((e) =>
			e.payload.reviewMessage?.summary.startsWith("CHANGES REQUESTED"),
		);
		assert.ok(
			Object.values(b.snapshot().deliveries).some(
				(d) =>
					d.signalId === correction.payload.reviewMessage.signal.id &&
					d.targetRoleId === "worker" &&
					d.state === "delivered",
			),
		);
		const task = b.snapshot().tasks.review;
		b.reviewTask({
			actor: actor("coordinator"),
			taskId: task.id,
			expectedRevision: task.revision,
			decision: "approved",
			reason: "Independent aggregate check",
			nextActions: [],
			idempotencyKey: "final-approve",
		});
		assert.ok(Object.values(b.snapshot().tasks).every((t) => t.state === "completed"));
		console.log(
			"Peer fixture: 3 real Pi runs, 2 broker activations, direct channel Q&A, 1 correction, 0 coordinator workflow mutations/turns before final approval.",
		);
	} finally {
		clearTimeout(timer);
		dispose();
		for (const child of processes) if (child.exitCode === null) child.kill("SIGKILL");
		transport.close();
	}
});
