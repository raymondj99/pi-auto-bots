import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { it } from "node:test";
import { buildCoordinationChatThreads } from "../../src/chat/records.ts";
import { PI_CLI } from "./pi-cli.ts";

it("runs the coordination worked example through real Pi tools, hooks and runtime reload", {
	timeout: 90000,
}, async () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-worked-example-"));
	console.log(`Worked-example artifacts: ${directory}`);
	const env: NodeJS.ProcessEnv = {
		...process.env,
		PI_OFFLINE: "1",
		PI_TELEMETRY: "0",
		PI_COORDINATION_EXAMPLE_DIR: directory,
		PI_CODING_AGENT_DIR: join(directory, "config"),
	};
	for (const key of Object.keys(env))
		if (key.startsWith("PI_SUBAGENT_") || key === "PI_DENY_TOOLS") delete env[key];
	env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE = "strict";
	const child = spawn(
		PI_CLI,
		[
			"--mode",
			"rpc",
			"--no-extensions",
			"--no-skills",
			"--no-context-files",
			"--session",
			join(directory, "session.jsonl"),
			"--provider",
			"coordination-test",
			"--model",
			"scripted",
			"-e",
			resolve("src/index.ts"),
			"-e",
			resolve("test/fixtures/coordination/runtime-driver.ts"),
		],
		{ env, stdio: ["pipe", "pipe", "pipe"] },
	);
	const exited = once(child, "exit");
	let output = "",
		buffer = "",
		ended = 0;
	child.stderr.on("data", (data) => {
		output += data;
	});
	child.stdout.on("data", (data) => {
		output += data;
		buffer += data;
		for (;;) {
			const end = buffer.indexOf("\n");
			if (end < 0) break;
			const line = buffer.slice(0, end);
			buffer = buffer.slice(end + 1);
			try {
				if (JSON.parse(line).type === "agent_end") ended++;
			} catch {}
		}
	});
	const wait = async (predicate: () => boolean) => {
		const deadline = Date.now() + 30000;
		while (!predicate()) {
			if (existsSync(join(directory, "failure.json")))
				assert.fail(readFileSync(join(directory, "failure.json"), "utf8"));
			assert.ok(child.exitCode === null && Date.now() < deadline, output.slice(-12000));
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
	};
	const prompt = (message: string) =>
		child.stdin.write(JSON.stringify({ type: "prompt", message }) + "\n");
	try {
		await wait(() => existsSync(join(directory, "loaded.json")));
		prompt("Run the deterministic local coordination worked example.");
		await wait(() => existsSync(join(directory, "report.json")) && ended > 0);
		const before = JSON.parse(readFileSync(join(directory, "report.json"), "utf8"));
		assert.equal(before.pass, true);
		const dashboard = async () => {
			const previous = output.length;
			prompt("/auto-bots url");
			await wait(() => /http:\/\/127\.0\.0\.1:\d+\/#\w+/.test(output.slice(previous)));
			const url = new URL(output.slice(previous).match(/http:\/\/127\.0\.0\.1:\d+\/#\w+/)![0]);
			const response = await fetch(`${url.origin}/api/snapshot`, {
				headers: { Authorization: `Bearer ${url.hash.slice(1)}` },
			});
			assert.equal(response.status, 200);
			return (await response.json()) as any;
		};
		const beforeUI = await dashboard();
		assert.ok(beforeUI.members.some((member: any) => member.id === "example-worker"));
		const threads = buildCoordinationChatThreads(beforeUI.records);
		assert.ok(threads.some((thread) => thread.kind === "channel" && thread.title === "example"));
		assert.ok(threads.some((thread) => thread.id === "agent:example-worker"));
		assert.equal(
			beforeUI.records.filter((record: any) => record.event.text === "Assignment context").length,
			1,
		);
		prompt("/example-reload");
		await wait(() => existsSync(join(directory, "reloaded.json")));
		assert.equal(
			JSON.parse(readFileSync(join(directory, "reloaded.json"), "utf8")).reason,
			"reload",
		);
		prompt("Verify durable state after actual runtime reload.");
		await wait(() => existsSync(join(directory, "report-reloaded.json")));
		const after = JSON.parse(readFileSync(join(directory, "report-reloaded.json"), "utf8"));
		assert.equal(after.pass, true);
		await wait(() => ended >= 2);
		const afterUI = await dashboard();
		assert.ok(afterUI.members.some((member: any) => member.id === "example-worker"));
		assert.equal(
			afterUI.records.filter((record: any) => record.event.text === "Assignment context").length,
			1,
			"reload preserves coordination channel messages without duplicating backfill",
		);
		console.log(
			"Real Pi dashboard: stable agents, channel conversations and messages verified before/after reload",
		);
		assert.equal(readFileSync(join(directory, "result.txt"), "utf8"), "handoff-ok\n");
		console.log(
			`${before.results.length} pre-reload steps, ${after.results.length} post-reload steps passed`,
		);
	} finally {
		child.kill("SIGTERM");
		const kill = setTimeout(() => child.kill("SIGKILL"), 2000);
		await exited;
		clearTimeout(kill);
	}
});
