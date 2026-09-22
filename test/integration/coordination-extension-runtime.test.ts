/** Provider-free real Pi runtime smoke test for the coordination tool and bootstrap wiring. */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { it } from "node:test";
import { PI_CLI } from "./pi-cli.ts";

it("loads coordination tools in real Pi without persisting credentials or control endpoints", {
	timeout: 20_000,
}, async () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-coordination-runtime-"));
	const sessionFile = join(directory, "session.jsonl"),
		marker = join(directory, "marker.json"),
		probe = join(directory, "probe.ts");
	writeFileSync(
		probe,
		`import { writeFileSync } from "node:fs"; export default function(pi){pi.on("session_start",()=>{writeFileSync(${JSON.stringify(marker)},JSON.stringify({tools:pi.getAllTools().map(t=>({name:t.name,schema:t.parameters}))}));});}`,
	);
	const env: NodeJS.ProcessEnv = {
		...process.env,
		PI_OFFLINE: "1",
		PI_CODING_AGENT_DIR: join(directory, "config"),
	};
	for (const key of Object.keys(env))
		if (key.startsWith("PI_SUBAGENT_") || key === "PI_DENY_TOOLS") delete env[key];
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
			probe,
		],
		{ env, stdio: ["pipe", "pipe", "pipe"] },
	);
	const exited = once(child, "exit");
	let output = "";
	child.stdout.on("data", (c) => {
		output += c;
	});
	child.stderr.on("data", (c) => {
		output += c;
	});
	try {
		const deadline = Date.now() + 10_000;
		while (!existsSync(marker) && Date.now() < deadline && child.exitCode === null)
			await new Promise((r) => setTimeout(r, 20));
		assert.ok(existsSync(marker), output || "coordination runtime marker timeout");
		const tools = JSON.parse(readFileSync(marker, "utf8")).tools;
		for (const name of [
			"subagent_team",
			"subagent_task",
			"subagent_channel",
			"subagent_role",
			"subagent_signal",
			"subagent_artifact",
			"subagent_permit",
			"subagent_checkpoint",
		])
			assert.equal(tools.filter((t: any) => t.name === name).length, 1, name);
		for (const tool of tools.filter((t: any) => t.name.startsWith("subagent_")))
			assert.equal(Object.hasOwn(tool.schema?.properties ?? {}, "actor"), false);
		const taskActions = tools
			.find((t: any) => t.name === "subagent_task")
			.schema.properties.request.anyOf.flatMap((variant: any) => variant.properties.action.enum);
		const spawn = tools.find((t: any) => t.name === "subagent").schema;
		assert.ok(
			spawn.properties.taskId && spawn.properties.requiredInputs && spawn.properties.autoExit,
		);
		assert.equal(
			taskActions.includes("assign"),
			false,
			"stored legacy mutations cannot bypass typed task gates",
		);
		assert.ok(taskActions.includes("start"));
		const persisted = existsSync(sessionFile) ? readFileSync(sessionFile, "utf8") : "";
		assert.doesNotMatch(persisted, /PI_SUBAGENT_COORDINATION|socket|token|capability/i);
		assert.doesNotMatch(output, /extension_error/);
	} finally {
		child.kill("SIGTERM");
		const kill = setTimeout(() => child.kill("SIGKILL"), 2000);
		await exited;
		clearTimeout(kill);
		rmSync(directory, { recursive: true, force: true });
	}
});
