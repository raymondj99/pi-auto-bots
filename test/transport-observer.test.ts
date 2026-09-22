import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { CoordinationChatRecord } from "../src/chat/records.ts";
import type { CoordinationEvent } from "../src/events.ts";
import { TransportBroker, TransportClient } from "../src/transport.ts";

async function observed(records: CoordinationChatRecord[], count: number) {
	const deadline = Date.now() + 2_000;
	while (records.length < count && Date.now() < deadline)
		await new Promise((resolve) => setTimeout(resolve, 5));
	assert.equal(records.length, count);
}

async function fixture(
	run: (value: {
		broker: TransportBroker;
		a: TransportClient;
		b: TransportClient;
		parent: CoordinationEvent[];
		records: CoordinationChatRecord[];
	}) => Promise<void>,
	observer?: (record: CoordinationChatRecord) => void,
) {
	const parent: CoordinationEvent[] = [];
	const records: CoordinationChatRecord[] = [];
	const broker = new TransportBroker(
		(event) => parent.push(event),
		observer ?? ((record) => records.push(record)),
	);
	await broker.start();
	const a = new TransportClient(broker.add("a", "Duplicate", "/a.jsonl", false), () => {});
	const b = new TransportClient(broker.add("b", "Duplicate", "/b.jsonl", false), () => {});
	try {
		await Promise.all([a.ready, b.ready]);
		await run({ broker, a, b, parent, records });
	} finally {
		a.close();
		b.close();
		broker.close();
	}
}

describe("transport observation", () => {
	it("observes coordinator, child and peer messages once without injecting peer traffic into the coordinator", async () =>
		fixture(async ({ broker, a, parent, records }) => {
			broker.request("parent", { action: "message", to: "a", text: "out" });
			await a.request({ action: "message", to: "parent", text: "back" });
			await a.request({ action: "message", to: "b", text: "peer only" });
			await observed(records, 3);
			assert.deepEqual(
				records.map((record) => [record.event.from, record.event.to]),
				[
					["parent", "a"],
					["a", "parent"],
					["a", "b"],
				],
			);
			assert.deepEqual(
				records.map((record) => [record.fromName, record.toName]),
				[
					["Coordinator", "Duplicate"],
					["Duplicate", "Coordinator"],
					["Duplicate", "Duplicate"],
				],
			);
			assert.equal(parent.length, 1);
			assert.equal(parent[0].text, "back");
		}));

	it("does not observe failed operations", async () =>
		fixture(async ({ a, records }) => {
			await assert.rejects(a.request({ action: "message", to: "missing", text: "no" }));
			await assert.rejects(a.request({ action: "message", to: "b", text: "" }));
			await assert.rejects(a.request({ action: "invalid" }));
			assert.equal(records.length, 0);
		}));

	it("insulates delivery from throwing or mutating observers", async () => {
		let calls = 0;
		await fixture(
			async ({ broker, a, b }) => {
				const result = broker.request("parent", {
					action: "message",
					to: "a",
					text: "safe",
				}) as any;
				assert.equal(result.queued, true);
				// Delivery still works after the observer threw.
				await a.request({ action: "message", to: "b", text: "still routing" });
				assert.equal(calls, 2);
				assert.ok(b);
			},
			(record) => {
				calls++;
				assert.throws(() => {
					record.event.text = "corrupted";
				}, TypeError);
				throw new Error("observer failed");
			},
		);
	});

	it("exposes no transport credentials or session paths", async () =>
		fixture(async ({ a, records }) => {
			await a.request({ action: "message", to: "parent", text: "hello" });
			await observed(records, 1);
			const serialized = JSON.stringify(records[0]);
			assert.equal(serialized.includes("token"), false);
			assert.equal(serialized.includes("socket"), false);
			assert.equal(serialized.includes("sessionFile"), false);
			assert.equal(serialized.includes("/a.jsonl"), false);
		}));
});
