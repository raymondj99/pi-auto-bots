/** Real Pi runtime + real socket broker, without provider calls or terminal UI. */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { it } from "node:test";
import { COORDINATION_TOOLS } from "../../src/coordination/tools.ts";
import { TransportBroker } from "../../src/transport.ts";
import { PI_CLI } from "./pi-cli.ts";

it("loads child tools in real Pi and queues interactive peer context without a model turn", {
	timeout: 20_000,
}, async () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-coordination-child-runtime-"));
	const sessionFile = join(directory, "session.jsonl");
	const marker = join(directory, "received.json");
	const probe = join(directory, "probe.ts");
	const receipts: Array<{ from: string; deliveryId: string }> = [];
	// Every coordinated child bootstraps on connect, so the transport needs a tunnel.
	const broker = new TransportBroker(
		() => {},
		undefined,
		(from, deliveryId) => {
			receipts.push({ from, deliveryId });
			return { delivered: true };
		},
		(_from, method) =>
			method === "bootstrap"
				? {
						bootstrap: "Coordination bootstrap.",
						actor: { roleId: "child", runId: "child", generation: 1 },
						peerLifecycleSupported: false,
					}
				: {},
	);
	await broker.start();
	const credentials = broker.add("child", "Runtime Child", sessionFile, true);
	writeFileSync(
		probe,
		`
export default function(pi) {
  let timer;
  pi.on("session_start", (_event, ctx) => {
    timer = setTimeout(async () => {
      const { writeFileSync } = await import("node:fs");
      writeFileSync(${JSON.stringify(marker)}, JSON.stringify({ tools: pi.getAllTools().map(t => t.name), idle: ctx.isIdle() }));
    }, 500);
  });
  pi.on("session_shutdown", () => clearTimeout(timer));
}
`,
	);
	const env = { ...process.env };
	for (const key of Object.keys(env))
		if (key.startsWith("PI_SUBAGENT_") || key === "PI_DENY_TOOLS") delete env[key];
	Object.assign(env, {
		PI_CODING_AGENT_DIR: join(directory, "config"),
		PI_SUBAGENT_COORDINATION: JSON.stringify(credentials),
		PI_SUBAGENT_SESSION: sessionFile,
		PI_SUBAGENT_ID: "child",
		PI_SUBAGENT_INTERACTIVE: "1",
		PI_OFFLINE: "1",
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
			"-e",
			resolve("src/index.ts"),
			"-e",
			resolve("src/subagent-done.ts"),
			"-e",
			probe,
		],
		{ env, stdio: ["pipe", "pipe", "pipe"] },
	);
	const exited = once(child, "exit");
	let output = "";
	child.stdout.on("data", (chunk) => {
		output += chunk;
	});
	child.stderr.on("data", (chunk) => {
		output += chunk;
	});
	async function waitFor(check: () => boolean) {
		const deadline = Date.now() + 10_000;
		while (!check() && Date.now() < deadline && child.exitCode === null)
			await new Promise((r) => setTimeout(r, 20));
		assert.ok(check(), output || "Timed out waiting for Pi runtime");
	}
	try {
		await waitFor(() => broker.isAvailable("child"));
		broker.request("parent", {
			action: "message",
			to: "child",
			text: "Runtime peer context",
			deliveryId: "delivery-runtime-1",
		});
		await new Promise((r) => setTimeout(r, 50));
		assert.equal(receipts.length, 0, "nextTurn queue acceptance must not count as delivered");
		await waitFor(() => existsSync(marker));
		const result = JSON.parse(readFileSync(marker, "utf8"));
		for (const name of [...COORDINATION_TOOLS, "caller_ping", "subagent_done"]) {
			assert.equal(
				result.tools.filter((tool: string) => tool === name).length,
				1,
				`Missing or duplicate ${name}`,
			);
		}
		assert.equal(result.idle, true);
		assert.equal(broker.isAvailable("child"), true);
		child.stdin.write(
			JSON.stringify({
				id: "inject",
				type: "prompt",
				message: "confirm pending session injection",
			}) + "\n",
		);
		await waitFor(() => output.includes('"id":"inject"'));
		assert.equal(receipts.length, 0, "a rejected prompt must not fabricate session delivery");
		assert.doesNotMatch(output, /"type":"extension_error"/);
		child.stdin.write(JSON.stringify({ id: "switch", type: "new_session" }) + "\n");
		await waitFor(() =>
			output.split("\n").some((line) => {
				try {
					const event = JSON.parse(line);
					return event.id === "switch" && event.success === true;
				} catch {
					return false;
				}
			}),
		);
		await waitFor(() => !broker.isAvailable("child"));
		assert.throws(
			() => broker.request("parent", { action: "message", to: "child", text: "must not leak" }),
			/unavailable/,
		);
	} finally {
		child.kill("SIGTERM");
		const forceKill = setTimeout(() => child.kill("SIGKILL"), 2000);
		await exited;
		clearTimeout(forceKill);
		broker.close();
		rmSync(directory, { recursive: true, force: true });
	}
});
