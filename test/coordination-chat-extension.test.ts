import assert from "node:assert/strict";
import { it } from "node:test";
import { CHAT_ENTRY_TYPE, installCoordinationChat } from "../src/chat/extension.ts";
import type { CoordinationChatRecord } from "../src/chat/records.ts";
import type { CoordinationDashboardMember } from "../src/chat/server.ts";

function record(id = "event-1"): CoordinationChatRecord {
	return {
		timestamp: 1_700_000_000_000,
		fromName: "Worker",
		toName: "Reviewer",
		event: { id, kind: "message", from: "worker", to: "reviewer", text: `chat body ${id}` },
	};
}

function harness() {
	const events = new Map<string, (...args: any[]) => any>();
	const commands = new Map<string, any>();
	const entries: any[] = [];
	const notifications: string[] = [];
	const executions: any[] = [];
	const memberListeners = new Set<() => void>();
	let members: CoordinationDashboardMember[] = [];
	let sessionId = "session-1";
	const ctx: any = {
		mode: "tui",
		hasUI: true,
		isIdle: () => true,
		sessionManager: { getSessionId: () => sessionId, getEntries: () => entries },
		ui: { notify: (s: string) => notifications.push(s) },
	};
	const pi: any = {
		on: (event: string, handler: (...args: any[]) => any) => events.set(event, handler),
		registerCommand: (name: string, command: any) => commands.set(name, command),
		appendEntry: (customType: string, data: any) =>
			entries.push({ type: "custom", customType, data }),
		getSessionName: () => "Test team",
		async exec(command: string, args: string[]) {
			executions.push({ command, args });
			return { code: 0 };
		},
		sendMessage: () => assert.fail("Observer must not send messages"),
		sendUserMessage: () => assert.fail("Observer must not start model turns"),
	};
	const observe = installCoordinationChat(pi, {
		getMembers: () => members,
		subscribeMembers: (listener) => {
			memberListeners.add(listener);
			return () => {
				memberListeners.delete(listener);
			};
		},
	});
	return {
		ctx,
		pi,
		entries,
		notifications,
		observe,
		executions,
		memberListeners,
		async start(id = sessionId) {
			sessionId = id;
			await events.get("session_start")!({}, ctx);
		},
		async shutdown() {
			await events.get("session_shutdown")!({}, ctx);
		},
		async trigger(event: string) {
			await events.get(event)!({}, ctx);
		},
		addGoal(goal: string, status: string, startedAt: number) {
			entries.push({
				type: "custom",
				customType: "goal-state",
				data: { goal, status, startedAt, turns: 0 },
			});
		},
		open(args = "") {
			return commands.get("auto-bots").handler(args, ctx);
		},
		setMembers(value: CoordinationDashboardMember[]) {
			members = value;
			for (const listener of memberListeners) listener();
		},
		url() {
			return notifications
				.findLast((line) => line.startsWith("Auto-bots chats: "))!
				.slice("Auto-bots chats: ".length);
		},
	};
}

async function snapshot(url: string) {
	const parsed = new URL(url);
	const result = await fetch(`${parsed.origin}/api/snapshot`, {
		headers: { Authorization: `Bearer ${parsed.hash.slice(1).split("&")[0]}` },
		signal: AbortSignal.timeout(2000),
	});
	assert.equal(result.status, 200);
	return result.json() as Promise<any>;
}

async function archive(url: string, body: object) {
	const parsed = new URL(url);
	const result = await fetch(`${parsed.origin}/api/archive`, {
		method: "POST",
		body: JSON.stringify(body),
		headers: {
			Authorization: `Bearer ${parsed.hash.slice(1).split("&")[0]}`,
			"Content-Type": "application/json",
			Origin: parsed.origin,
			"Sec-Fetch-Site": "same-origin",
		},
		signal: AbortSignal.timeout(2000),
	});
	assert.equal(result.status, 200);
}

it("persists peer-only observations as custom entries, dedupes and restores on reload", async () => {
	const h = harness();
	await h.start();
	h.observe(record());
	h.observe(record());
	assert.equal(h.entries.length, 1);
	assert.equal(h.entries[0].customType, CHAT_ENTRY_TYPE);
	assert.equal(h.entries[0].data.sessionId, "session-1");
	await h.shutdown();
	await h.start();
	await h.open("");
	const data = await snapshot(h.url());
	assert.equal(data.records.filter((r: any) => r.event.text === "chat body event-1").length, 1);
	assert.equal(h.entries.length, 1);
	await h.shutdown();
});

it("switches/forks isolate copied history; malformed entries do not break startup", async () => {
	const h = harness();
	await h.start();
	h.observe(record());
	await h.shutdown();
	h.entries.push({ type: "custom", customType: CHAT_ENTRY_TYPE, data: null });
	h.entries.push({
		type: "custom",
		customType: CHAT_ENTRY_TYPE,
		data: { sessionId: "session-2", record: {} },
	});
	h.entries.push({
		type: "custom",
		customType: CHAT_ENTRY_TYPE,
		data: {
			sessionId: "session-2",
			record: {
				timestamp: 1,
				fromName: "Bad",
				toName: "Room",
				event: {
					id: "bad-channel",
					kind: "channel_message",
					from: "bad",
					to: "room",
					text: "bad",
					channel: { id: "room", name: "Room", members: null, status: "open" },
				},
			},
		},
	});
	await h.start("session-2");
	await h.open("");
	const url = h.url();
	const before = await snapshot(url);
	assert.equal(
		before.records.some((r: any) => r.event.text === "chat body event-1"),
		false,
	);
	h.observe(record("event-2"));
	const after = await snapshot(url);
	assert.equal(
		after.records.some((r: any) => r.event.text === "chat body event-2"),
		true,
	);
	await h.shutdown();
});

it("persistence failure preserves live observation and warns only once", async () => {
	const h = harness();
	await h.start();
	h.pi.appendEntry = () => {
		throw new Error("disk unavailable");
	};
	h.observe(record());
	h.observe(record("event-2"));
	assert.equal(h.notifications.length, 1);
	await h.open("");
	const data = await snapshot(h.url());
	assert.equal(
		data.records.some((r: any) => r.event.text === "chat body event-2"),
		true,
	);
	await h.shutdown();
});

it("optional command argument seeds the dashboard query without mutating history", async () => {
	const h = harness();
	await h.start();
	h.observe(record());
	await h.open("no-matching-thread");
	assert.match(h.url(), /&q=no-matching-thread/);
	assert.equal(h.entries.length, 1);
	await h.shutdown();
});

it("default command opens a reusable localhost dashboard with silent agents and peer messages", async () => {
	const h = harness();
	await h.start();
	h.setMembers([
		{ id: "silent", name: "Silent agent", status: "waiting", backend: "claude", role: "reviewer" },
	]);
	h.observe(record());
	try {
		await h.open("");
		const url = h.url();
		assert.match(url, /^http:\/\/127\.0\.0\.1:\d+\/#/);
		assert.equal(h.executions.length, 1);
		assert.ok(h.executions[0].args.includes(url));
		const data = await snapshot(url);
		assert.equal(data.sessionName, "Test team");
		assert.equal(data.members.find((member: any) => member.id === "silent").name, "Silent agent");
		assert.equal(data.records[0].event.text, "chat body event-1");
		assert.equal(data.members.find((member: any) => member.id === "worker").status, "archived");
		await h.open("url");
		assert.equal(h.url(), url);
		assert.equal(h.executions.length, 1);
		h.setMembers([]);
		assert.equal(
			(await snapshot(url)).members.find((member: any) => member.id === "silent").status,
			"finished",
		);
	} finally {
		await h.shutdown();
	}
});

it("browser opener failure leaves a working URL and stop invalidates the server", async () => {
	const h = harness();
	await h.start();
	h.pi.exec = async () => ({ code: 1 });
	await h.open("");
	const url = h.url();
	assert.match(h.notifications.join("\n"), /Open the local URL/);
	await snapshot(url);
	await h.open("stop");
	await assert.rejects(() => snapshot(url));
	await h.shutdown();
});

it("server shutdown/switch invalidates old capability and never leaks previous history", async () => {
	const h = harness();
	await h.start();
	h.observe(record());
	await h.open("url");
	const old = h.url();
	await h.shutdown();
	await assert.rejects(() => snapshot(old));
	await h.start("other-session");
	try {
		await h.open("url");
		const next = h.url();
		assert.notEqual(next, old);
		const data = await snapshot(next);
		assert.equal(data.records.length, 0);
		assert.equal(data.members.length, 1);
	} finally {
		await h.shutdown();
	}
});

it("server opening raced with shutdown cannot open a browser into a replacement session", async () => {
	const h = harness();
	await h.start();
	const opening = h.open("");
	await h.shutdown();
	await opening;
	assert.equal(h.executions.length, 0);
});

it("tags live records by goal identity, archives completion, and keeps late traffic bound", async () => {
	const h = harness();
	await h.start();
	h.addGoal("Ship release", "active", 100);
	await h.trigger("before_agent_start");
	h.observe(record("during"));
	h.addGoal("Ship release", "paused", 100);
	await h.trigger("before_agent_start");
	h.observe(record("paused"));
	h.addGoal("Ship release", "completed", 100);
	await h.trigger("tool_execution_end");
	h.observe(record("late"));
	await h.open("url");
	let data = await snapshot(h.url());
	assert.deepEqual(
		data.records.map((item: any) => item.goalId),
		["session-1:100", "session-1:100", "session-1:100"],
	);
	assert.deepEqual(data.archive.goalIds, ["session-1:100"]);
	assert.equal(data.goals.find((goal: any) => goal.id === "session-1:100").status, "completed");

	h.addGoal("Ship release", "active", 200);
	await h.trigger("before_agent_start");
	h.observe({
		timestamp: 1_700_000_000_001,
		fromName: "Coordinator",
		toName: "Worker",
		event: {
			id: "new-start",
			kind: "message",
			from: "parent",
			to: "worker",
			text: "new goal retask",
		},
	});
	data = await snapshot(h.url());
	assert.equal(data.records.at(-1).goalId, "session-1:200");
	assert.deepEqual(data.archive.goalIds, ["session-1:100"]);
	await h.shutdown();
});

it("does not persist redundant metadata while historical goal states remain in the branch", async () => {
	const h = harness();
	await h.start();
	h.addGoal("Stable", "active", 7);
	await h.trigger("before_agent_start");
	h.observe(record("stable-event"));
	h.addGoal("Stable", "completed", 7);
	await h.trigger("tool_execution_end");
	const count = h.entries.filter((entry) => entry.customType === "subagent-chat-archive").length;
	await h.trigger("before_agent_start");
	await h.trigger("tool_execution_end");
	assert.equal(
		h.entries.filter((entry) => entry.customType === "subagent-chat-archive").length,
		count,
	);
	await h.shutdown();
});

it("binds silent live members to an open goal and archives them when it completes", async () => {
	const h = harness();
	await h.start();
	h.setMembers([{ id: "silent", name: "Silent", status: "waiting", backend: "pi" }]);
	h.addGoal("Silent work", "active", 8);
	await h.trigger("before_agent_start");
	h.addGoal("Silent work", "completed", 8);
	await h.trigger("tool_execution_end");
	await h.open("url");
	const data = await snapshot(h.url());
	assert.equal(data.records.length, 0);
	assert.equal(data.members.find((member: any) => member.id === "silent").goalId, "session-1:8");
	assert.ok(data.archive.goalIds.includes("session-1:8"));
	assert.ok(data.archive.memberIds.includes("silent"));
	await h.shutdown();
});

it("preserves peer and channel goal affinity until an explicit coordinator retask", async () => {
	const h = harness();
	await h.start();
	h.setMembers([
		{ id: "old", name: "Old", status: "waiting", backend: "pi" },
		{ id: "peer", name: "Peer", status: "waiting", backend: "pi" },
	]);
	h.addGoal("Goal A", "active", 10);
	await h.trigger("before_agent_start");
	h.observe({
		timestamp: 1,
		fromName: "Coordinator",
		toName: "Old room",
		event: {
			id: "channel-create",
			kind: "channel_create",
			from: "parent",
			to: "old-room",
			text: "Old room",
			channel: { id: "old-room", name: "Old room", members: ["old", "peer"], status: "open" },
		},
	});
	h.addGoal("Goal A", "completed", 10);
	await h.trigger("tool_execution_end");

	h.addGoal("Goal B", "active", 20);
	await h.trigger("before_agent_start");
	h.setMembers([
		{ id: "old", name: "Old", status: "waiting", backend: "pi" },
		{ id: "peer", name: "Peer", status: "waiting", backend: "pi" },
		{ id: "new", name: "New", status: "waiting", backend: "pi" },
	]);
	h.observe({
		timestamp: 2,
		fromName: "Old",
		toName: "New",
		event: {
			id: "old-late",
			kind: "message",
			from: "old",
			to: "new",
			text: "late A",
		},
	});
	h.observe({
		timestamp: 3,
		fromName: "New",
		toName: "Old",
		event: {
			id: "new-work",
			kind: "message",
			from: "new",
			to: "old",
			text: "work B",
		},
	});
	h.observe({
		timestamp: 4,
		fromName: "Coordinator",
		toName: "Old",
		event: {
			id: "retask",
			kind: "message",
			from: "parent",
			to: "old",
			text: "join B",
		},
	});
	h.observe({
		timestamp: 5,
		fromName: "Old",
		toName: "Old room",
		event: {
			id: "old-channel-late",
			kind: "channel_message",
			from: "old",
			to: "old-room",
			text: "late channel A",
			channel: { id: "old-room", name: "Old room", members: ["old", "peer"], status: "open" },
			recipients: ["peer"],
		},
	});

	await h.open("url");
	const data = await snapshot(h.url());
	const goalFor = (id: string) => data.records.find((item: any) => item.event.id === id).goalId;
	assert.equal(goalFor("old-late"), "session-1:10");
	assert.equal(goalFor("new-work"), "session-1:20");
	assert.equal(goalFor("retask"), "session-1:20");
	assert.equal(goalFor("old-channel-late"), "session-1:10");
	assert.equal(data.members.find((member: any) => member.id === "old").goalId, "session-1:20");
	assert.equal(data.members.find((member: any) => member.id === "new").goalId, "session-1:20");
	assert.equal(data.members.find((member: any) => member.id === "peer").goalId, "session-1:10");
	await h.shutdown();
});

it("keeps late task completion and handoff on the task's originating goal", async () => {
	const h = harness();
	await h.start();
	h.setMembers([{ id: "old", name: "Old", status: "waiting", backend: "pi" }]);
	h.addGoal("Task A", "active", 30);
	await h.trigger("before_agent_start");
	const task = (id: string, owner: string, status: "assigned" | "completed", revision: number) => ({
		id,
		title: id,
		context: "ctx",
		nextSteps: "next",
		owner,
		createdBy: "parent",
		status,
		revision,
	});
	h.observe({
		timestamp: 1,
		fromName: "Coordinator",
		toName: "Coordinator",
		event: {
			id: "self-assign",
			kind: "assign",
			from: "parent",
			to: "parent",
			text: "self task",
			task: task("self-task", "parent", "assigned", 1),
		},
	});
	h.observe({
		timestamp: 2,
		fromName: "Coordinator",
		toName: "Old",
		event: {
			id: "handoff-assign",
			kind: "assign",
			from: "parent",
			to: "old",
			text: "handoff task",
			task: task("handoff-task", "old", "assigned", 1),
		},
	});
	h.addGoal("Task A", "completed", 30);
	await h.trigger("tool_execution_end");
	h.addGoal("Task B", "active", 40);
	await h.trigger("before_agent_start");
	h.setMembers([
		{ id: "old", name: "Old", status: "waiting", backend: "pi" },
		{ id: "fresh", name: "Fresh", status: "waiting", backend: "pi" },
	]);
	h.observe({
		timestamp: 3,
		fromName: "Coordinator",
		toName: "Coordinator",
		event: {
			id: "self-complete",
			kind: "complete",
			from: "parent",
			to: "parent",
			text: "self done",
			task: task("self-task", "parent", "completed", 2),
		},
	});
	h.observe({
		timestamp: 4,
		fromName: "Old",
		toName: "Fresh",
		event: {
			id: "late-handoff",
			kind: "handoff",
			from: "old",
			to: "fresh",
			text: "handoff",
			task: task("handoff-task", "fresh", "assigned", 2),
		},
	});
	await h.open("url");
	const data = await snapshot(h.url());
	const goalFor = (id: string) => data.records.find((item: any) => item.event.id === id).goalId;
	assert.equal(goalFor("self-complete"), "session-1:30");
	assert.equal(goalFor("late-handoff"), "session-1:30");
	assert.equal(data.members.find((member: any) => member.id === "fresh").goalId, "session-1:40");
	await h.shutdown();
});

it("replays legacy untagged records against ordered goal-state entries", async () => {
	const h = harness();
	h.addGoal("Legacy goal", "active", 9);
	h.entries.push({
		type: "custom",
		customType: CHAT_ENTRY_TYPE,
		data: { sessionId: "session-1", record: record("legacy") },
	});
	h.addGoal("Legacy goal", "completed", 9);
	await h.start();
	await h.open("url");
	const data = await snapshot(h.url());
	assert.equal(data.records[0].goalId, "session-1:9");
	assert.equal(data.records[0].goalTitle, "Legacy goal");
	assert.deepEqual(data.archive.goalIds, ["session-1:9"]);
	assert.equal(h.entries.at(-1).customType, "subagent-chat-archive");
	await h.shutdown();
});

it("restores goal/archive metadata in-session but isolates it from a forked session id", async () => {
	const h = harness();
	await h.start();
	h.addGoal("Finish", "active", 10);
	await h.trigger("before_agent_start");
	h.observe(record("goal-event"));
	h.addGoal("Finish", "completed", 10);
	await h.trigger("tool_execution_end");
	await h.shutdown();

	await h.start("session-1");
	await h.open("url");
	let data = await snapshot(h.url());
	assert.equal(data.records[0].goalId, "session-1:10");
	assert.deepEqual(data.archive.goalIds, ["session-1:10"]);
	await h.shutdown();

	await h.start("fork-session");
	await h.open("url");
	data = await snapshot(h.url());
	assert.equal(data.records.length, 0);
	assert.deepEqual(data.archive.goalIds, []);
	await h.shutdown();
});

it("manual agent, channel, and goal scopes archive exact current targets", async () => {
	const h = harness();
	await h.start();
	h.addGoal("Organize", "active", 50);
	await h.trigger("before_agent_start");
	h.observe(record("direct"));
	h.observe({
		timestamp: 2,
		fromName: "Worker",
		toName: "Room",
		event: {
			id: "channel",
			kind: "channel_message",
			from: "worker",
			to: "room",
			text: "broadcast",
			channel: { id: "room", name: "Room", members: ["worker", "reviewer"], status: "open" },
			recipients: ["reviewer"],
		},
	});
	await h.open("url");
	const url = h.url();
	await archive(url, { scope: "agent", id: "worker" });
	await archive(url, { scope: "channel", id: "room" });
	await archive(url, { scope: "goal", id: "session-1:50" });
	const data = await snapshot(url);
	assert.ok(data.archive.memberIds.includes("worker"));
	assert.ok(data.archive.channelIds.includes("room"));
	assert.deepEqual(data.archive.eventIds.sort(), ["channel", "direct"]);
	assert.deepEqual(data.archive.goalIds, ["session-1:50"]);
	assert.equal(data.records.length, 2);
	await h.shutdown();
});

it("manual clear archives current events, channels, and inactive members without deleting history", async () => {
	const h = harness();
	await h.start();
	h.setMembers([{ id: "worker", name: "Worker", status: "waiting", backend: "pi" }]);
	h.observe(record("direct"));
	h.observe({
		timestamp: 2,
		fromName: "Worker",
		toName: "Room",
		event: {
			id: "channel",
			kind: "channel_message",
			from: "worker",
			to: "room",
			text: "broadcast",
			channel: { id: "room", name: "Room", members: ["worker", "reviewer"], status: "open" },
			recipients: ["reviewer"],
		},
	});
	h.setMembers([]);
	await h.open("clear");
	assert.match(h.notifications.at(-1)!, /archived.*not deleted/i);
	await h.open("url");
	let data = await snapshot(h.url());
	assert.deepEqual(
		data.records.map((item: any) => item.event.id),
		["direct", "channel"],
	);
	assert.deepEqual(data.archive.eventIds, ["direct", "channel"]);
	assert.deepEqual(data.archive.channelIds, ["room"]);
	assert.ok(data.archive.memberIds.includes("worker"));
	assert.equal(data.members.find((member: any) => member.id === "worker").status, "finished");

	h.observe(record("new-direct"));
	h.observe({
		timestamp: 3,
		fromName: "Worker",
		toName: "Room",
		event: {
			id: "new-channel",
			kind: "channel_message",
			from: "worker",
			to: "room",
			text: "new broadcast",
			channel: { id: "room", name: "Room", members: ["worker", "reviewer"], status: "open" },
			recipients: ["reviewer"],
		},
	});
	data = await snapshot(h.url());
	assert.equal(data.archive.memberIds.includes("worker"), false);
	assert.equal(data.archive.channelIds.includes("room"), false);
	assert.deepEqual(data.archive.eventIds, ["direct", "channel"]);
	await h.shutdown();
});
