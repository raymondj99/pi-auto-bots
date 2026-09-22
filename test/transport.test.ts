import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { existsSync } from "node:fs";
import { describe, it } from "node:test";
import { deliverCoordination, installChildCoordination } from "../src/child-coordination.ts";
import type { CoordinationEvent } from "../src/events.ts";
import { TransportBroker, TransportClient } from "../src/transport.ts";

async function fixture(run: (f: any) => Promise<void>) {
	const parent: CoordinationEvent[] = [];
	const aEvents: CoordinationEvent[] = [];
	const bEvents: CoordinationEvent[] = [];
	const broker = new TransportBroker((event) => parent.push(event));
	await broker.start();
	const aCredentials = broker.add("a", "Same name", "/a.jsonl", false);
	const bCredentials = broker.add("b", "Same name", "/b.jsonl", true);
	const a = new TransportClient(aCredentials, (event) => aEvents.push(event));
	const b = new TransportClient(bCredentials, (event) => bEvents.push(event));
	try {
		await Promise.all([a.ready, b.ready]);
		await run({ broker, a, b, parent, aEvents, bEvents, aCredentials, bCredentials });
	} finally {
		a.close();
		b.close();
		broker.close();
	}
}

async function received(events: any[], count: number) {
	const deadline = Date.now() + 2000;
	while (events.length < count && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
	assert.equal(events.length, count);
}

function mockPi() {
	const handlers = new Map<string, ((...args: any[]) => any)[]>();
	const tools = new Map<string, any>();
	const messages: any[] = [];
	const bus = new EventEmitter();
	return {
		events: {
			on(name: string, listener: (data: unknown) => void) {
				bus.on(name, listener);
				return () => {
					bus.off(name, listener);
				};
			},
			emit(name: string, data: unknown) {
				bus.emit(name, data);
			},
		},
		registerCommand() {},
		registerMessageRenderer() {},
		registerShortcut() {},
		handlers,
		tools,
		messages,
		on(name: string, handler: (...args: any[]) => any) {
			handlers.set(name, [...(handlers.get(name) ?? []), handler]);
		},
		registerTool(tool: any) {
			tools.set(tool.name, tool);
		},
		sendMessage(message: any, options: any) {
			messages.push({ message, options });
		},
	};
}

describe("coordination transport over live sockets", () => {
	it("routes coordinator/peer/reply messages by exact ID, never by display name", async () =>
		fixture(async ({ broker, a, b, parent, aEvents, bEvents }) => {
			broker.request("parent", { action: "message", to: "a", text: "start" });
			await a.request({ action: "message", to: "b", text: "peer question" });
			await b.request({ action: "message", to: "parent", text: "reply" });
			await received(aEvents, 1);
			await received(bEvents, 1);
			assert.equal(aEvents[0].from, "parent");
			assert.equal(bEvents[0].from, "a");
			assert.equal(parent[0].from, "b");
			await assert.rejects(
				a.request({ action: "message", to: "Same name", text: "no" }),
				/unavailable/,
			);
		}));

	it("rejects empty, oversize, self-addressed, unknown-recipient and unknown-action requests", async () =>
		fixture(async ({ a }) => {
			for (const request of [
				{ action: "message", to: "b", text: " " },
				{ action: "message", to: "b", text: "x".repeat(8001) },
				{ action: "message", to: "a", text: "self" },
				{ action: "message", to: "missing", text: "no" },
				{ action: "invalid" },
			])
				await assert.rejects(a.request(request));
		}));

	it("unconnected and removed members are unavailable; revoked credentials cannot reconnect", async () =>
		fixture(async ({ broker, a, bCredentials }) => {
			broker.add("starting", "Starting", "/starting.jsonl", false);
			await assert.rejects(
				a.request({ action: "message", to: "starting", text: "no" }),
				/unavailable/,
			);
			broker.remove("b");
			await assert.rejects(a.request({ action: "message", to: "b", text: "no" }), /unavailable/);
			for (const token of [bCredentials.token, ""]) {
				const revoked = new TransportClient({ ...bCredentials, token }, () => {});
				try {
					await assert.rejects(revoked.ready, /identity/);
				} finally {
					revoked.close();
				}
			}
		}));

	it("isolates brokers, credentials and session files", async () =>
		fixture(async ({ aCredentials }) => {
			const other = new TransportBroker(() => {});
			await other.start();
			const otherCredentials = other.add("a", "Other", "/a.jsonl", false);
			const intruder = new TransportClient(
				{ ...aCredentials, socket: otherCredentials.socket },
				() => {},
			);
			const wrongSession = new TransportClient(
				{ ...otherCredentials, sessionFile: "/fork.jsonl" },
				() => {},
			);
			try {
				await assert.rejects(intruder.ready, /identity/);
				await assert.rejects(wrongSession.ready, /identity/);
				const duplicate = new TransportClient(aCredentials, () => {});
				try {
					await assert.rejects(duplicate.ready, /identity/);
				} finally {
					duplicate.close();
				}
			} finally {
				intruder.close();
				wrongSession.close();
				other.close();
			}
		}));

	it("tunnels coordination requests from authenticated children only", async () =>
		fixture(async ({ broker, a }) => {
			const calls: Array<[string, string, unknown]> = [];
			const tunnelled = new TransportBroker(
				() => {},
				undefined,
				undefined,
				(from, method, params) => {
					calls.push([from, method, params]);
					return { ok: true };
				},
			);
			await tunnelled.start();
			const credentials = tunnelled.add("child", "Child", "/child.jsonl", false);
			const client = new TransportClient(credentials, () => {});
			try {
				await client.ready;
				await client.request({ action: "coordination", method: "snapshot", params: {} });
				assert.deepEqual(calls, [["child", "snapshot", {}]]);
				// The coordinator owns the broker directly and must not tunnel to itself.
				assert.throws(
					() =>
						tunnelled.request("parent", { action: "coordination", method: "snapshot", params: {} }),
					/local broker/,
				);
			} finally {
				client.close();
				tunnelled.close();
			}
			// A broker with no tunnel installed rejects coordination requests outright.
			await assert.rejects(a.request({ action: "coordination", method: "snapshot", params: {} }));
			assert.equal(typeof broker.close, "function");
		}));

	it("cleans socket files and rejects calls after shutdown", async () =>
		fixture(async ({ broker, a, aCredentials }) => {
			assert.equal(existsSync(aCredentials.socket), true);
			broker.close();
			broker.close();
			assert.equal(existsSync(aCredentials.socket), false);
			assert.throws(
				() => broker.request("parent", { action: "message", to: "a", text: "x" }),
				/closed/,
			);
			await assert.rejects(a.request({ action: "message", to: "b", text: "x" }));
		}));
});

describe("child coordination adapters", () => {
	it("uses steer messages for autonomous runs and nextTurn for interactive runs", () => {
		const pi = mockPi();
		const event = { id: "event", from: "a", to: "b", kind: "message", text: "context" };
		deliverCoordination(pi as any, event);
		deliverCoordination(pi as any, event, true);
		assert.deepEqual(pi.messages[0].options, { triggerTurn: true, deliverAs: "steer" });
		assert.deepEqual(pi.messages[1].options, { deliverAs: "nextTurn" });
		assert.match(pi.messages[0].message.content, /from a to b/);
	});

	it("child attaches only to its launch session, confirms receipts on injection, and closes on shutdown", async () => {
		const old = process.env.PI_SUBAGENT_COORDINATION;
		const receipts: string[] = [];
		const broker = new TransportBroker(
			() => {},
			undefined,
			(_from, deliveryId) => {
				receipts.push(deliveryId);
				return { delivered: true };
			},
			(_from, method) => {
				if (method === "bootstrap")
					return {
						bootstrap: "Coordination bootstrap.",
						actor: { roleId: "child", runId: "child", generation: 1 },
						peerLifecycleSupported: false,
					};
				return {};
			},
		);
		await broker.start();
		const credentials = broker.add("child", "Child", "/child.jsonl", false);
		process.env.PI_SUBAGENT_COORDINATION = JSON.stringify(credentials);
		const pi = mockPi();
		installChildCoordination(pi as any);
		const start = async (event: any, ctx: any) => {
			for (const handler of pi.handlers.get("session_start")!) await handler(event, ctx);
		};
		const stop = () => {
			for (const handler of pi.handlers.get("session_shutdown")!) handler({});
		};
		const ctx = (path: string) => ({
			sessionManager: {
				getSessionFile: () => path,
				getSessionId: () => path,
				getEntries: () => [],
			},
			hasUI: false,
			isIdle: () => true,
		});
		try {
			// A forked session inherits the environment but must not adopt the credentials.
			await start({}, ctx("/fork.jsonl"));
			assert.equal(
				pi.messages.some((m) => m.message.customType === "subagent_coordination_bootstrap"),
				false,
			);

			await start({}, ctx("/child.jsonl"));
			await new Promise((resolve) => setTimeout(resolve, 20));
			assert.equal(
				pi.messages.some((m) => m.message.customType === "subagent_coordination_bootstrap"),
				true,
			);

			broker.request("parent", {
				action: "message",
				to: "child",
				text: "critical",
				deliveryId: "delivery-1",
			});
			await new Promise((resolve) => setTimeout(resolve, 5));
			assert.equal(receipts.length, 0, "queue acceptance is not a receipt");

			const injected = pi.messages.at(-1)?.message;
			for (const handler of pi.handlers.get("message_end") ?? [])
				await handler({ message: injected }, ctx("/child.jsonl"));
			await new Promise((resolve) => setTimeout(resolve, 5));
			assert.deepEqual(receipts, ["delivery-1"]);

			stop();
			await new Promise((resolve) => setTimeout(resolve, 10));
			assert.equal(broker.isAvailable("child"), false);
			assert.throws(
				() => broker.request("parent", { action: "message", to: "child", text: "after" }),
				/unavailable/,
			);
		} finally {
			try {
				stop();
			} catch {
				/* Install may have failed before handlers were registered. */
			}
			broker.close();
			if (old === undefined) delete process.env.PI_SUBAGENT_COORDINATION;
			else process.env.PI_SUBAGENT_COORDINATION = old;
		}
	});
});
