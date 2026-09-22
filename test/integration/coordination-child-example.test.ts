import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { it } from "node:test";
import { buildCoordinationBootstrap } from "../../src/coordination/bootstrap.ts";
import { CoordinationBroker } from "../../src/coordination/broker.ts";
import { TransportBroker } from "../../src/transport.ts";
import { PI_CLI } from "./pi-cli.ts";

for (const mode of ["strict", "fast"] as const)
	it(`real Pi child ${mode}: shell, artifact and handoff over authenticated sockets`, {
		timeout: 45000,
	}, async () => {
		const directory = mkdtempSync(join(tmpdir(), "pi-child-example-"));
		console.log(`Child-example artifacts: ${directory}`);
		const sessionFile = join(directory, "child.jsonl");
		const b = new CoordinationBroker("example", "branch", undefined, 1000, undefined, {
			workflowMode: mode,
			artifactStoreDir: join(directory, "automatic-store"),
		});
		const coordinator = { roleId: "coordinator", runId: "parent-run", generation: 1 };
		const worker = { roleId: "worker", runId: "child-run", generation: 1 };
		b.admitRole({
			roleId: coordinator.roleId,
			capabilities: ["coordinator"],
			idempotencyKey: "admit",
		});
		b.bindRole({ roleId: coordinator.roleId, runId: coordinator.runId, idempotencyKey: "bind" });
		b.admitRole({
			actor: coordinator,
			roleId: worker.roleId,
			capabilities: ["worker"],
			idempotencyKey: "admit-worker",
		});
		b.bindRole({ roleId: worker.roleId, runId: worker.runId, idempotencyKey: "bind-worker" });
		const channel = b.createChannel({
			actor: coordinator,
			name: "handoff",
			purpose: "Live child handoff",
			members: [
				{ roleId: "coordinator", mode: "participate" },
				{ roleId: "worker", mode: "participate" },
			],
			delivery: "digest",
			idempotencyKey: "channel",
		}).event.payload;
		const welcome = b.sendChannel({
			actor: coordinator,
			channelId: channel.id,
			expectedRevision: 1,
			text: "Routine inbox delivered without a poll",
			idempotencyKey: "welcome",
		}).event.payload;
		let leases: string[] = [];
		if (mode === "strict") {
			b.createBudget({
				actor: coordinator,
				budgetId: "child-budget",
				limit: 1,
				idempotencyKey: "budget",
			});
			leases = Object.keys(
				b.reserveBudget({ actor: coordinator, budgetId: "child-budget", idempotencyKey: "reserve" })
					.event.payload.reservations,
			);
		}
		b.createTask({
			actor: coordinator,
			id: "child-task",
			title: "Actual child example",
			owner: "worker",
			leases,
			idempotencyKey: "task",
		});
		b.transitionTask({
			actor: coordinator,
			taskId: "child-task",
			expectedRevision: 1,
			state: "ready",
			idempotencyKey: "ready",
		});
		// Ensure receipts would be much larger than the transport limit before the fix.
		for (let i = 0; i < 80; i++)
			b.emitSignal({
				actor: coordinator,
				kind: "attention",
				severity: "info",
				summary: "private ".repeat(50),
				targets: [{ roleId: "coordinator" }],
				idempotencyKey: `private-${i}`,
			});
		const s = b.emitSignal({
			actor: coordinator,
			kind: "ready",
			severity: "action",
			summary: "Worker must acknowledge",
			targets: [{ taskId: "child-task" }],
			requiresAck: ["worker"],
			idempotencyKey: "worker-ready",
		}).event.payload;
		b.queueSignalDelivery({
			actor: coordinator,
			signalId: s.id,
			targetRoleId: "worker",
			idempotencyKey: "delivery",
		});
		const transport = new TransportBroker(
			() => {},
			undefined,
			(_from, deliveryId) =>
				b.receipt({
					actor: worker,
					deliveryId,
					state: "delivered",
					idempotencyKey: `receipt-${deliveryId}`,
				}),
			(from, method, params) => {
				assert.equal(from, worker.runId);
				if (method === "bootstrap")
					return {
						bootstrap: buildCoordinationBootstrap(b.snapshot(), worker.roleId),
						actor: worker,
						policy: { workflowMode: mode, protectedOperations: mode === "strict" ? ["shell"] : [] },
					};
				if (method === "digest") return b.digest(worker, Number(params.sinceSeq ?? 0));
				const result = (b as any)[method]({ ...params, actor: worker });
				// The reviewer, not the worker, freezes the published output on request.
				if (method === "sendChannel" && String(params.text).startsWith("Request reviewer freeze"))
					b.freezeArtifact({
						actor: coordinator,
						artifactId: "child-output",
						version: 1,
						idempotencyKey: "reviewer-freeze",
					});
				return result;
			},
		);
		await transport.start();
		const credentials = transport.add(worker.runId, "Worker", sessionFile, true);
		const env: NodeJS.ProcessEnv = {
			...process.env,
			PI_OFFLINE: "1",
			PI_TELEMETRY: "0",
			PI_CODING_AGENT_DIR: join(directory, "config"),
			PI_COORDINATION_EXAMPLE_DIR: directory,
			PI_COORDINATION_CHILD_EXAMPLE: "1",
			PI_COORDINATION_FAST_EXAMPLE: mode === "fast" ? "1" : "0",
		};
		for (const key of Object.keys(env))
			if (key.startsWith("PI_SUBAGENT_") || key === "PI_DENY_TOOLS") delete env[key];
		Object.assign(env, {
			PI_SUBAGENT_COORDINATION: JSON.stringify(credentials),
			PI_SUBAGENT_INTERACTIVE: "1",
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
				sessionFile,
				"--provider",
				"coordination-test",
				"--model",
				"scripted",
				"-e",
				resolve("src/subagent-done.ts"),
				"-e",
				resolve("test/fixtures/coordination/runtime-driver.ts"),
			],
			{ env, cwd: directory, stdio: ["pipe", "pipe", "pipe"] },
		);
		const exited = once(child, "exit");
		let output = "";
		child.stdout.on("data", (data) => {
			output += data;
		});
		child.stderr.on("data", (data) => {
			output += data;
		});
		const wait = async (predicate: () => boolean) => {
			const deadline = Date.now() + 20000;
			while (!predicate()) {
				if (existsSync(join(directory, "failure.json")))
					assert.fail(readFileSync(join(directory, "failure.json"), "utf8"));
				assert.ok(child.exitCode === null && Date.now() < deadline, output.slice(-10000));
				await new Promise((resolve) => setTimeout(resolve, 20));
			}
		};
		try {
			await wait(() => existsSync(join(directory, "loaded.json")));
			child.stdin.write(
				JSON.stringify({ type: "prompt", message: "Run child worked example" }) + "\n",
			);
			await wait(() => existsSync(join(directory, "report.json")));
			const report = JSON.parse(readFileSync(join(directory, "report.json"), "utf8"));
			assert.equal(report.pass, true);
			assert.equal(b.snapshot().tasks["child-task"].owner, "coordinator");
			if (mode === "strict") assert.equal(b.snapshot().budgets["child-budget"].used, 1);
			else {
				assert.deepEqual(b.snapshot().budgets, {});
				assert.deepEqual(b.snapshot().permits, {});
			}
			assert.equal(b.snapshot().signals[s.id].acknowledgements[0].roleId, "worker");
			assert.equal(
				Object.values(b.snapshot().deliveries).find((d) => d.signalId === s.id)?.state,
				"acknowledged",
			);
			assert.equal(readFileSync(join(directory, "child-result.txt"), "utf8"), "handoff-ok\n");
			assert.equal(b.snapshot().artifacts["child-output"][0].state, "frozen");
			if (mode === "fast") {
				assert.match(b.snapshot().artifacts["child-output"][0].path!, /automatic-store/);
				assert.equal(
					b.snapshot().artifacts["child-output"][0].publicationReason,
					"Verified child output",
				);
			}
			assert.equal(b.snapshot().deliveries[welcome.deliveries[0].id].state, "delivered");
			const session = readFileSync(sessionFile, "utf8")
				.trim()
				.split("\n")
				.map((line) => JSON.parse(line));
			assert.equal(
				session.filter((entry) => entry.customType === "subagent_coordination_bootstrap").length,
				1,
				"bootstrap once, not on every turn",
			);
			assert.ok(
				session.some(
					(entry) =>
						entry.customType === "subagent_coordination_inbox" &&
						JSON.stringify(entry).includes("Routine inbox delivered without a poll"),
				),
			);
			console.log(`${report.results.length} child tool steps passed; channel=${channel.id}`);
		} finally {
			child.kill("SIGTERM");
			const kill = setTimeout(() => child.kill("SIGKILL"), 2000);
			await exited;
			clearTimeout(kill);
			transport.close();
		}
	});
