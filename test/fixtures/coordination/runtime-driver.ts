// Deterministic test driver: emits scripted tool calls, never calls a model/service.
import assert from "node:assert/strict";
import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createAssistantMessageEventStream } from "@mariozechner/pi-ai";
import { autonomyExample } from "./autonomy-example.ts";
import { childExample } from "./child-example.ts";
import { workedExample } from "./worked-example.ts";

export default function (pi: any) {
	const directory = process.env.PI_COORDINATION_EXAMPLE_DIR!;
	const reloadFile = join(directory, "reload.json");
	const reloaded = existsSync(reloadFile);
	const steps = process.env.PI_COORDINATION_AUTONOMY_ROLE
		? autonomyExample(process.env.PI_COORDINATION_AUTONOMY_ROLE)
		: process.env.PI_COORDINATION_CHILD_EXAMPLE === "1"
			? childExample(directory, process.env.PI_COORDINATION_FAST_EXAMPLE === "1")
			: workedExample(directory, reloaded);
	const results: any[] = [];
	let last: any;
	let finished = false;
	let sequence = 0;
	pi.registerCommand("example-reload", {
		description: "Reload actual Pi extension runtime during the worked example",
		handler: async (_args: string, ctx: any) => {
			writeFileSync(reloadFile, JSON.stringify({ requested: true }));
			await ctx.reload();
		},
	});
	pi.on("session_start", (event: any) => {
		writeFileSync(
			join(directory, reloaded ? "reloaded.json" : "loaded.json"),
			JSON.stringify({
				reason: event.reason,
				tools: pi.getAllTools().map((tool: any) => tool.name),
			}),
		);
	});
	pi.registerProvider("coordination-test", {
		baseUrl: "http://127.0.0.1:1/never-used",
		apiKey: "local-test-only",
		api: "openai-completions",
		models: [
			{
				id: "scripted",
				name: "Deterministic test driver (no model)",
				reasoning: false,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 1000000,
				maxTokens: 4096,
			},
		],
		streamSimple(model: any, context: any) {
			const stream = createAssistantMessageEventStream();
			const message: any = {
				role: "assistant",
				content: [],
				api: model.api,
				provider: model.provider,
				model: model.id,
				usage: {
					input: 0,
					output: 0,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 0,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				},
				stopReason: "stop",
				timestamp: Date.now(),
			};
			queueMicrotask(() => {
				try {
					let value: any;
					if (last) {
						const result = context.messages.findLast(
							(m: any) => m.role === "toolResult" && m.toolCallId === last.id,
						);
						assert.ok(result, `Missing result for ${last.name}`);
						const text = result.content.map((c: any) => c.text ?? "").join("\n");
						if (last.error) {
							assert.equal(result.isError, true, `${last.name} should reject: ${text}`);
							assert.match(text, new RegExp(last.error, "i"));
						} else assert.ok(!result.isError, `${last.name} failed: ${text}`);
						// Provider transcripts omit tool details; parse the actual public result.
						try {
							value = JSON.parse(result.content[0]?.text ?? text);
						} catch {
							value = result;
						}
						results.push({
							name: last.name,
							action: last.args.request?.action ?? last.args.action,
							expectedRejection: last.error,
							pass: true,
						});
						last = undefined;
					}
					const next = finished ? { done: true as const, value: undefined } : steps.next(value);
					if (next.done) {
						finished = true;
						writeFileSync(
							join(directory, reloaded ? "report-reloaded.json" : "report.json"),
							JSON.stringify({ pass: true, results }, null, 2),
						);
						message.content = [{ type: "text", text: "Coordination worked example PASS" }];
					} else {
						const step: any = next.value;
						last = { ...step, id: `example-call-${reloaded}-${++sequence}` };
						message.content = [
							{ type: "toolCall", id: last.id, name: last.name, arguments: last.args },
						];
						message.stopReason = "toolUse";
					}
					stream.push({ type: "start", partial: message });
					stream.push({ type: "done", reason: message.stopReason, message });
					stream.end();
				} catch (error) {
					const failure = String((error as Error).stack ?? error);
					writeFileSync(
						join(directory, "failure.json"),
						JSON.stringify({ failure, last, results }, null, 2),
					);
					message.stopReason = "error";
					message.errorMessage = failure;
					stream.push({ type: "error", reason: "error", error: message });
					stream.end();
				}
			});
			return stream;
		},
	});
}
