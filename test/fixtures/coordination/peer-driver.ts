/** Reactive scripted provider: no model/network calls, no timers or completion polling. */
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { createAssistantMessageEventStream } from "@mariozechner/pi-ai";

type Step = { name: string; args: any };
export default function (pi: any) {
	const role = process.env.PI_PEER_TEST_ROLE!;
	let sequence = 0,
		key = 0,
		round = 0,
		greeted = false;
	let steps: Generator<Step, void, any> | undefined, last: any;
	function* call(name: string, args: any): Generator<Step, any, any> {
		const result = yield {
			name,
			args:
				name.startsWith("subagent_") && name !== "subagent_team"
					? { request: { ...args, idempotencyKey: `${role}-${++key}` } }
					: args,
		};
		return result?.event?.payload ?? result;
	}
	function* turn(): Generator<Step, void, any> {
		const tasks = (yield* call("subagent_team", { kind: "tasks" })).state.items;
		const own = tasks.find((t: any) => t.owner === role);
		const channel = (yield* call("subagent_team", { kind: "channels" })).state.items[0];
		if (role === "reviewer") {
			if (!greeted) {
				greeted = true;
				assert.equal(own.state, "draft", "reviewer starts before submissions");
				yield* call("subagent_channel", {
					action: "send",
					channelId: channel.id,
					expectedRevision: channel.revision,
					text: "worker: confirm whether negative zero must normalize to positive zero.",
				});
				return;
			}
			if (own.state !== "in_progress") return;
			assert.equal(
				own.requiredInputs.length,
				3,
				"contract and both source outputs bound by broker",
			);
			const worker = tasks.find((t: any) => t.owner === "worker");
			if (round++ === 0) {
				yield* call("subagent_task", {
					action: "changes_requested",
					taskId: worker.id,
					expectedRevision: worker.revision,
					reason:
						"worker: the zero fixture is negative zero; normalize it and add the boundary assertion.",
					nextActions: ["Normalize zero and republish"],
				});
				return;
			}
			assert.equal(
				worker.outputs[0].version,
				2,
				"corrected output pinned without coordinator rebinding",
			);
			for (const task of tasks.filter((t: any) => t.owner !== role)) {
				const result = yield* call("subagent_task", {
					action: "approve",
					taskId: task.id,
					expectedRevision: task.revision,
					reason: `${task.owner}: checked exact frozen output and boundary evidence.`,
					nextActions: [],
				});
				assert.equal(result.state, "completed");
			}
		} else {
			if (own.state === "completed") return;
			if (own.state === "submitted" || own.state === "review") return;
			if (role === "worker" && !greeted) {
				greeted = true;
				yield* call("subagent_channel", {
					action: "send",
					channelId: channel.id,
					expectedRevision: channel.revision,
					text: "reviewer: confirmed, negative zero must normalize to positive zero; the boundary belongs in this module.",
				});
			}
			if (own.state === "blocked") {
				const started = yield* call("subagent_task", {
					action: "start",
					taskId: own.id,
					expectedRevision: own.revision,
				});
				own.revision = started.revision;
				yield* call("subagent_channel", {
					action: "send",
					channelId: channel.id,
					expectedRevision: channel.revision,
					text: "reviewer: corrected negative zero and added the exact boundary assertion; publishing the replacement now.",
					delivery: "digest",
				});
			}
		}
		yield* call("write", { path: `${role}.txt`, content: `${role} output ${++sequence}\n` });
		const artifact = yield* call("subagent_artifact", {
			action: "publish",
			artifactId: `${role}-output`,
			sourcePath: `${role}.txt`,
		});
		yield* call("subagent_task", {
			action: "submit",
			taskId: own.id,
			expectedRevision: own.revision,
			outputs: [
				{ artifactId: artifact.artifactId, version: artifact.version, digest: artifact.digest },
			],
			verification: ["Scripted fixture evidence verified"],
			nextActions: [],
		});
	}
	pi.registerProvider("peer-fixture", {
		baseUrl: "http://127.0.0.1:1/never-used",
		apiKey: "fixture-only",
		api: "openai-completions",
		models: [
			{
				id: "scripted",
				name: "Provider-free peer fixture",
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
						assert.ok(
							result && !result.isError,
							`${role} ${last.name}: ${JSON.stringify(result?.content)}`,
						);
						try {
							value = JSON.parse(result.content[0]?.text);
						} catch {
							value = result;
						}
						last = undefined;
					}
					steps ??= turn();
					const next = steps.next(value);
					if (next.done) {
						steps = undefined;
						message.content = [
							{
								type: "text",
								text: `${role}: turn complete; waiting or released according to broker.`,
							},
						];
					} else {
						last = { ...next.value, id: `${role}-call-${++sequence}` };
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
					writeFileSync(join(process.cwd(), "failure.json"), JSON.stringify({ failure }));
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
