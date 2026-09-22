import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	buildCoordinationChatThreads,
	type CoordinationChatRecord,
	CoordinationChatStore,
} from "../src/chat/records.ts";

function message(id: string, from = "a", to = "b", timestamp = 1): CoordinationChatRecord {
	return {
		event: { id, kind: "message", from, to, text: `body ${id}` },
		timestamp,
		fromName: "Same",
		toName: "Same",
	};
}

function task(id: string, eventId: string, kind = "assign", timestamp = 1): CoordinationChatRecord {
	return {
		event: {
			id: eventId,
			kind,
			from: "parent",
			to: "a",
			text: "Build\nContext: ctx\nNext steps: test",
			task: {
				id,
				title: "Build",
				context: "ctx",
				nextSteps: "test",
				owner: "a",
				createdBy: "parent",
				status: "assigned",
				revision: 1,
			},
		},
		timestamp,
		fromName: "Parent",
		toName: "Worker",
	};
}

describe("CoordinationChatStore", () => {
	it("validates restored values, strips extra fields, and deduplicates event IDs", () => {
		const store = new CoordinationChatStore();
		assert.equal(store.add(null as any), false);
		assert.equal(store.add({ ...message("x"), timestamp: NaN }), false);
		assert.equal(
			store.add({ ...message("x"), event: { ...message("x").event, kind: "unknown" } }),
			false,
		);
		const input: any = message("x");
		input.secret = "socket-token";
		input.event.secret = "/private/session.jsonl";
		assert.equal(store.add(input), true);
		assert.equal(store.add(message("x")), false);
		assert.equal(JSON.stringify(store.snapshot()).includes("socket-token"), false);
		assert.equal(JSON.stringify(store.snapshot()).includes("private/session"), false);

		const channel: any = {
			timestamp: 2,
			fromName: "A",
			toName: "Room",
			goalId: "session:10",
			goalTitle: "Release",
			event: {
				id: "channel",
				kind: "channel_message",
				from: "a",
				to: "room",
				text: "hello",
				channel: { id: "room", name: "Room", members: ["a", "b"], status: "open", secret: "drop" },
				recipients: ["b"],
				secret: "drop",
			},
		};
		assert.equal(store.add(channel), true);
		channel.event.channel.members.push("intruder");
		channel.event.recipients.push("intruder");
		const restored = store.snapshot().records[1];
		assert.deepEqual(restored.event.channel?.members, ["a", "b"]);
		assert.deepEqual(restored.event.recipients, ["b"]);
		assert.equal(restored.goalId, "session:10");
		assert.equal(JSON.stringify(restored).includes("secret"), false);
	});

	it("caps count and bytes and reports every omitted record", () => {
		const countStore = new CoordinationChatStore({ maxRecords: 2 });
		countStore.add(message("1"));
		countStore.add(message("2"));
		countStore.add(message("3"));
		assert.deepEqual(
			countStore.snapshot().records.map((r) => r.event.id),
			["2", "3"],
		);
		assert.equal(countStore.snapshot().dropped, 1);

		const bytesStore = new CoordinationChatStore({ maxBytes: 1 });
		assert.equal(bytesStore.add(message("large")), true);
		assert.deepEqual(bytesStore.snapshot(), { records: [], dropped: 1 });
	});

	it("returns detached snapshots and isolates listeners", () => {
		const store = new CoordinationChatStore();
		let calls = 0;
		store.subscribe(() => {
			throw new Error("listener");
		});
		const unsubscribe = store.subscribe(() => {
			calls++;
		});
		store.add(task("task", "one"));
		const snapshot = store.snapshot() as any;
		snapshot.records[0].event.text = "changed";
		snapshot.records[0].event.task.owner = "intruder";
		assert.equal(store.snapshot().records[0].event.text.startsWith("Build"), true);
		assert.equal(store.snapshot().records[0].event.task?.owner, "a");
		unsubscribe();
		store.add(message("two"));
		assert.equal(calls, 1);
		store.clear();
		assert.deepEqual(store.snapshot(), { records: [], dropped: 0 });
	});
});

describe("buildCoordinationChatThreads", () => {
	it("builds exact-ID agent timelines only, ordered by latest activity", () => {
		const records = [
			message("ab", "a", "b", 1),
			message("ac", "a", "c", 3),
			message("ab2", "b", "a", 4),
		];
		const threads = buildCoordinationChatThreads(records);
		assert.deepEqual(
			threads.map((thread) => thread.id),
			["agent:a", "agent:b", "agent:c"],
		);
		assert.ok(threads.every((thread) => thread.kind === "agent"));
		assert.equal(threads[0].records.length, 3);
		assert.equal(threads[1].records.length, 2);
		assert.equal(threads[1].subtitle, "b");
		assert.equal(threads[1].preview, "body ab2");
		assert.equal(threads[1].latestTimestamp, 4);
	});

	it("puts task lifecycle events in each involved agent timeline, never synthetic task groups", () => {
		const assigned = task("t1", "assign", "assign", 1);
		const handed = task("t1", "handoff", "handoff", 2);
		handed.event.from = "a";
		handed.event.to = "b";
		handed.event.task!.owner = "b";
		handed.event.task!.revision = 2;
		const completed = task("t1", "complete", "complete", 3);
		completed.event.from = "b";
		completed.event.to = "parent";
		completed.event.task!.status = "completed";
		completed.event.task!.revision = 3;
		const threads = buildCoordinationChatThreads([assigned, handed, completed]);
		assert.deepEqual(
			threads.map((thread) => thread.id),
			["agent:b", "agent:parent", "agent:a"],
		);
		assert.equal(threads.find((thread) => thread.id === "agent:a")!.records.length, 2);
		assert.equal(threads.find((thread) => thread.id === "agent:b")!.records.length, 2);
		assert.ok(threads.every((thread) => !thread.id.startsWith("task:") && thread.id !== "all"));
	});

	it("builds explicit channel threads and includes channel traffic in member timelines", () => {
		const channel: CoordinationChatRecord = {
			timestamp: 5,
			fromName: "Alpha",
			toName: "Release room",
			goalId: "s:1",
			goalTitle: "Ship",
			event: {
				id: "channel-event",
				kind: "channel_message",
				from: "a",
				to: "room-1",
				text: "ready",
				channel: {
					id: "room-1",
					name: "Release room",
					members: ["a", "b", "unavailable"],
					status: "open",
				},
				recipients: ["b"],
			},
		};
		const threads = buildCoordinationChatThreads([channel]);
		assert.deepEqual(
			threads.map((thread) => thread.id),
			["agent:a", "agent:b", "channel:room-1"],
		);
		assert.equal(threads[2].kind, "channel");
		assert.equal(threads[2].title, "Release room");
		assert.equal(threads[2].subtitle, "3 members · open");
	});

	it("filters labels, IDs, bodies, goal and task metadata while retaining timeline context", () => {
		const records = [
			message("first", "a", "b", 1),
			message("needle", "b", "a", 2),
			task("ticket-7", "task", "assign", 3),
		];
		records[1].event.text = "unique phrase";
		records[1].goalTitle = "Important migration";
		const bodyMatch = buildCoordinationChatThreads(records, "UNIQUE PHRASE");
		assert.deepEqual(
			bodyMatch.map((thread) => thread.id),
			["agent:a", "agent:b"],
		);
		assert.equal(bodyMatch[0].records.length, 3);
		assert.deepEqual(bodyMatch[0].matchIds, ["needle"]);
		assert.equal(bodyMatch[0].matchCount, 1);
		assert.deepEqual(
			buildCoordinationChatThreads(records, "ticket-7").map((thread) => thread.id),
			["agent:a", "agent:parent"],
		);
		assert.deepEqual(
			buildCoordinationChatThreads(records, "migration").map((thread) => thread.id),
			["agent:a", "agent:b"],
		);
		assert.deepEqual(buildCoordinationChatThreads(records, "All activity"), []);
		assert.deepEqual(buildCoordinationChatThreads(records, "not present"), []);
	});
});
