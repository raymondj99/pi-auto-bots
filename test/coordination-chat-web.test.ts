import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import {
	applyDashboardUpdates,
	avatarHue,
	initials,
	normalizeDashboardSnapshot,
	projectDashboard,
} from "../src/dashboard/app.js";

const member = (id: string, name: string, status = "running", goalId?: string) => ({
	id,
	name,
	status,
	backend: "pi",
	...(goalId ? { goalId } : {}),
});
const message = (
	id: string,
	from: string,
	to: string,
	text: string,
	timestamp: number,
	extra: any = {},
) => ({
	event: { id, kind: "message", from, to, text, ...extra.event },
	timestamp,
	fromName: from === "parent" ? "Coordinator" : from.toUpperCase(),
	toName: to === "parent" ? "Coordinator" : to.toUpperCase(),
	...extra.record,
});
const channel = (
	id: string,
	name: string,
	members: string[],
	status: "open" | "closed" = "open",
) => ({ id, name, members, status });
const channelMessage = (
	id: string,
	from: string,
	room: ReturnType<typeof channel>,
	text: string,
	timestamp: number,
	recipients = room.members.filter((entry) => entry !== from),
	record: any = {},
) => ({
	event: { id, kind: "channel_message", from, to: room.id, text, channel: room, recipients },
	timestamp,
	fromName: from === "parent" ? "Coordinator" : from.toUpperCase(),
	toName: room.name,
	...record,
});

const blankArchive = { goalIds: [], eventIds: [], memberIds: [], channelIds: [] };

describe("coordination chat web model", () => {
	it("projects named-agent timelines and explicit channels only", () => {
		const room = channel("launch", "Launch room", ["a", "b"]);
		const snapshot = {
			sessionName: "Build session",
			dropped: 0,
			members: [
				member("parent", "Coordinator", "active"),
				member("a", "Alice"),
				member("b", "Bob"),
				member("silent", "Silent", "idle"),
			],
			records: [
				message("dm", "parent", "a", "direct note", 1),
				{
					...message("task", "a", "b", "task", 2),
					event: {
						id: "task",
						kind: "assign",
						from: "a",
						to: "b",
						text: "task",
						task: {
							id: "t",
							title: "Ship",
							context: "ctx",
							nextSteps: "next",
							owner: "b",
							createdBy: "a",
							status: "assigned",
							revision: 1,
						},
					},
				},
				channelMessage("room", "a", room, "shared update", 3),
			],
			archive: blankArchive,
			goals: [],
		};
		const view = projectDashboard(snapshot);
		assert.deepEqual(
			view.members.map((entry: any) => entry.name),
			["Coordinator", "Alice", "Bob", "Silent"],
		);
		assert.equal(view.names.get("a"), "Alice", "roster names override stale event labels");
		assert.equal(view.names.has("launch"), false, "channel IDs are not agent identities");
		assert.deepEqual(
			view.threads.filter((entry: any) => entry.kind === "channel").map((entry: any) => entry.id),
			["channel:launch"],
		);
		assert.deepEqual(
			view.threads
				.filter((entry: any) => entry.kind === "agent")
				.map((entry: any) => entry.id)
				.sort(),
			["agent:a", "agent:b", "agent:parent", "agent:silent"],
		);
		assert.ok(
			!view.threads.some(
				(entry: any) =>
					entry.id === "all" || entry.id.startsWith("dm:") || entry.id.startsWith("task:"),
			),
		);
		assert.deepEqual(
			view.threads
				.find((entry: any) => entry.id === "agent:a")
				.records.map((entry: any) => entry.event.id),
			["dm", "task", "room"],
		);
		assert.deepEqual(
			view.threads
				.find((entry: any) => entry.id === "agent:b")
				.records.map((entry: any) => entry.event.id),
			["task", "room"],
		);
		assert.equal(view.threads.find((entry: any) => entry.id === "agent:silent").records.length, 0);
	});

	it("does not invent channels from direct messages or tasks", () => {
		const view = projectDashboard({
			members: [member("a", "Alice"), member("b", "Bob")],
			records: [
				message("one", "a", "b", "peer message", 1),
				{
					...message("two", "a", "b", "handoff", 2),
					event: {
						id: "two",
						kind: "handoff",
						from: "a",
						to: "b",
						text: "handoff",
						task: {
							id: "t",
							title: "Work",
							context: "c",
							nextSteps: "n",
							owner: "b",
							createdBy: "a",
							status: "assigned",
							revision: 2,
						},
					},
				},
			],
		});
		assert.equal(view.threads.filter((entry: any) => entry.kind === "channel").length, 0);
		assert.equal(view.threads.find((entry: any) => entry.id === "agent:b").records.length, 2);
	});

	it("uses accepted recipients for partial channel fanout agent timelines", () => {
		const room = channel("ops", "Operations", ["a", "b", "c"]);
		const view = projectDashboard({
			members: [member("a", "Alice"), member("b", "Bob"), member("c", "Cara")],
			records: [channelMessage("partial", "a", room, "Only Bob accepted", 1, ["b"])],
		});
		assert.equal(view.threads.find((entry: any) => entry.id === "agent:a").records.length, 1);
		assert.equal(view.threads.find((entry: any) => entry.id === "agent:b").records.length, 1);
		assert.equal(view.threads.find((entry: any) => entry.id === "agent:c").records.length, 0);
	});

	it("hides archived history and finished agents by default, then reveals it with badges", () => {
		const active = channel("active", "Active", ["parent", "a"]);
		const done = channel("done", "Finished goal", ["parent", "old"]);
		const closed = channel("closed", "Closed room", ["a", "old"], "closed");
		const snapshot = {
			members: [
				member("parent", "Coordinator", "active"),
				member("a", "Alice"),
				member("old", "Old worker", "finished", "goal-old"),
			],
			records: [
				channelMessage("active-event", "a", active, "now", 1),
				channelMessage("done-event", "old", done, "historical", 2, ["parent"], {
					goalId: "goal-old",
					goalTitle: "Old goal",
				}),
				channelMessage("closed-event", "a", closed, "closed", 3),
				message("archived-direct", "old", "parent", "old direct", 4, {
					record: { goalId: "goal-old" },
				}),
			],
			archive: { goalIds: ["goal-old"], eventIds: [], memberIds: [], channelIds: [] },
			goals: [{ id: "goal-old", title: "Old goal", status: "completed", startedAt: 10 }],
		};
		const activeView = projectDashboard(snapshot);
		assert.deepEqual(
			activeView.members.map((entry: any) => entry.id),
			["parent", "a"],
		);
		assert.deepEqual(
			activeView.threads
				.filter((entry: any) => entry.kind === "channel")
				.map((entry: any) => entry.id),
			["channel:active"],
		);
		assert.equal(
			activeView.threads
				.find((entry: any) => entry.id === "agent:parent")
				.records.some((entry: any) => entry.event.id === "archived-direct"),
			false,
		);

		const history = projectDashboard(snapshot, { showArchived: true });
		assert.deepEqual(
			history.members.map((entry: any) => entry.id),
			["parent", "a", "old"],
		);
		assert.equal(history.members.find((entry: any) => entry.id === "old").archived, true);
		assert.deepEqual(
			history.threads
				.filter((entry: any) => entry.kind === "channel")
				.map((entry: any) => entry.id)
				.sort(),
			["channel:active", "channel:closed", "channel:done"],
		);
		assert.equal(history.threads.find((entry: any) => entry.id === "channel:done").archived, true);
		assert.equal(
			history.threads.find((entry: any) => entry.id === "channel:closed").archived,
			true,
		);
	});

	it("applies event, member, and channel manual archives without erasing source records", () => {
		const room = channel("room", "Room", ["a", "b"]);
		const snapshot = {
			members: [member("a", "Alice"), member("b", "Bob")],
			records: [
				message("private", "a", "b", "hidden event", 1),
				channelMessage("shared", "a", room, "room history", 2),
			],
			archive: { goalIds: [], eventIds: ["private"], memberIds: ["b"], channelIds: ["room"] },
		};
		const active = projectDashboard(snapshot);
		assert.equal(active.records.length, 2, "projection retains immutable source history");
		assert.deepEqual(
			active.members.map((entry: any) => entry.id),
			["a"],
		);
		assert.equal(
			active.threads.some((entry: any) => entry.kind === "channel"),
			false,
		);
		assert.equal(
			active.threads
				.find((entry: any) => entry.id === "agent:a")
				.records.some((entry: any) => entry.event.id === "private"),
			false,
		);
		const history = projectDashboard(snapshot, { showArchived: true });
		assert.equal(
			history.threads.find((entry: any) => entry.id === "channel:room").records.length,
			1,
		);
	});

	it("searches channel metadata and messages while preserving matched channel context", () => {
		const room = channel("research", "Research guild", ["a", "b"]);
		const snapshot = {
			members: [member("a", "Alice"), member("b", "Bob")],
			records: [
				channelMessage("one", "a", room, "ordinary", 1),
				channelMessage("two", "b", room, "needle phrase", 2),
			],
		};
		const searched = projectDashboard(snapshot, { query: "needle" });
		const thread = searched.threads.find((entry: any) => entry.id === "channel:research");
		assert.equal(thread.records.length, 2);
		assert.deepEqual(thread.matchIds, ["two"]);
		assert.equal(
			projectDashboard(snapshot, { query: "guild" }).threads.some(
				(entry: any) => entry.id === "channel:research",
			),
			true,
		);
		assert.equal(
			projectDashboard(snapshot, { query: "all activity" }).threads.some(
				(entry: any) => entry.kind === "channel",
			),
			false,
		);
	});

	it("applies removals, deduplicates records, and fully replaces optional archive and goal metadata", () => {
		const initial = normalizeDashboardSnapshot({
			sessionName: "one",
			members: [member("a", "A")],
			records: [message("1", "a", "b", "first", 1)],
			archive: blankArchive,
			goals: [],
		});
		const next = applyDashboardUpdates(initial, [
			{
				removedIds: ["1"],
				records: [message("2", "a", "b", "second", 2), message("2", "a", "b", "duplicate", 3)],
				dropped: 1,
				members: [member("b", "B", "finished")],
				sessionName: "two",
				archive: { goalIds: ["g"], eventIds: ["2"], memberIds: ["b"], channelIds: ["c"] },
				goals: [{ id: "g", title: "Goal", status: "completed", startedAt: 4 }],
			},
		]);
		assert.equal(next.sessionName, "two");
		assert.deepEqual(
			next.records.map((record: any) => record.event.id),
			["2"],
		);
		assert.deepEqual(next.archive.channelIds, ["c"]);
		assert.deepEqual(
			next.goals.map((goal: any) => goal.id),
			["g"],
		);
		const retained = applyDashboardUpdates(next, [{ records: [], removedIds: [], dropped: 1 }]);
		assert.deepEqual(retained.archive, next.archive);
		assert.deepEqual(retained.goals, next.goals);
	});

	it("keeps hostile markup inert and uses only safe DOM text APIs", () => {
		const hostile = '<img src=x onerror="globalThis.pwned=true"><script>bad()</script>';
		const room = channel("unsafe", hostile, ["a", "b"]);
		const view = projectDashboard({
			sessionName: hostile,
			members: [member("a", hostile)],
			records: [channelMessage("x", "a", room, hostile, 1)],
		});
		assert.equal(view.records[0].event.text, hostile);
		const source = readFileSync(new URL("../src/dashboard/app.js", import.meta.url), "utf8");
		assert.doesNotMatch(source, /\.innerHTML\s*=/);
		assert.match(source, /textContent/);
	});

	it("exposes accessible archive controls and uses the authenticated non-destructive archive API", () => {
		const source = readFileSync(new URL("../src/dashboard/app.js", import.meta.url), "utf8");
		const html = readFileSync(new URL("../src/dashboard/index.html", import.meta.url), "utf8");
		for (const id of [
			"show-archived",
			"clear-chats",
			"archive-selected",
			"mutation-status",
			"chat-options",
		]) {
			assert.match(html, new RegExp(`id=["']${id}["']`));
		}
		assert.match(source, /window\.confirm\(/);
		assert.match(source, /fetch\("\/api\/archive"/);
		assert.match(source, /Authorization:\s*`Bearer \$\{capability\}`/);
		assert.match(source, /"Content-Type":\s*"application\/json"/);
		assert.match(source, /archive\("all"\)/);
		assert.match(source, /archive\(thread\.kind, thread\.entityId\)/);
		assert.doesNotMatch(html, /send message|stop agent|delete/i);
	});

	it("keeps diagnostic controls behind disclosure and uses one shared responsive inbox", () => {
		const html = readFileSync(new URL("../src/dashboard/index.html", import.meta.url), "utf8");
		assert.match(html, /<details id="chat-options"/);
		assert.match(html, /<details id="operations-panel" hidden>/);
		assert.doesNotMatch(html, /class="agents-pane"|id="agent-picker"|scope-note/);
		assert.match(html, /Read-only · Local history/);
		assert.match(html, /aria-label="Search chats and messages"/);
	});

	it("disambiguates duplicate names and ignores invalid dates", () => {
		const first = {
			...message("one", "worker-prefix-1", "parent", "first", 1),
			fromName: "Worker",
		};
		const second = {
			...message("two", "worker-prefix-2", "parent", "second", 2),
			fromName: "Worker",
		};
		const view = projectDashboard({
			records: [
				first,
				second,
				{ ...first, event: { ...first.event, id: "bad-date" }, timestamp: Number.MAX_SAFE_INTEGER },
			],
			members: [],
		});
		assert.equal(view.records.length, 2);
		assert.notEqual(view.names.get("worker-prefix-1"), view.names.get("worker-prefix-2"));
	});

	it("retains live bounded coordination operational projections", () => {
		const operational = {
			cursor: 9,
			roles: [],
			tasks: [
				{ id: "t", owner: "worker", state: "ready", revision: 1, dependencies: [], blockers: [] },
			],
			blockers: [],
			artifacts: [],
			budgets: [],
			deliveries: [],
			audit: [],
		};
		const snapshot = normalizeDashboardSnapshot({ records: [], members: [], operational });
		assert.deepEqual(snapshot.operational, operational);
		const updated = applyDashboardUpdates(snapshot, [
			{ records: [], removedIds: [], dropped: 0, operational: { ...operational, cursor: 10 } },
		]);
		assert.equal(updated.operational.cursor, 10);
	});

	it("creates stable compact avatar labels and colors", () => {
		assert.equal(initials("Ada Lovelace"), "AL");
		assert.equal(initials("worker"), "WO");
		assert.equal(avatarHue("worker-1"), avatarHue("worker-1"));
		assert.ok(avatarHue("worker-1") >= 0 && avatarHue("worker-1") < 360);
	});
});
