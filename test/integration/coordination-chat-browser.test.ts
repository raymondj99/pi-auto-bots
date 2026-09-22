/** Real headless Chrome + local HTTP/SSE; no provider calls or browser dependency. */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import type { CoordinationChatRecord } from "../../src/chat/records.ts";
import {
	type CoordinationArchiveRequest,
	type CoordinationDashboardSnapshot,
	startCoordinationDashboard,
} from "../../src/chat/server.ts";
import { CoordinationBroker } from "../../src/coordination/broker.ts";
import { coordinationChatMembers, coordinationChatRecords } from "../../src/coordination/chat.ts";

const chrome =
	process.env.PI_TEST_CHROME ??
	(process.platform === "darwin"
		? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
		: "/usr/bin/google-chrome");

class DevTools {
	private socket: WebSocket;
	private nextId = 0;
	private pending = new Map<
		number,
		{ resolve: (value: any) => void; reject: (error: Error) => void }
	>();
	errors: string[] = [];
	dialogs: string[] = [];
	ready: Promise<void>;
	constructor(url: string) {
		this.socket = new WebSocket(url);
		this.ready = new Promise((resolve, reject) => {
			this.socket.addEventListener("open", () => resolve(), { once: true });
			this.socket.addEventListener(
				"error",
				() => reject(new Error("Chrome DevTools connection failed")),
				{ once: true },
			);
		});
		this.socket.addEventListener("message", (event) => {
			const message = JSON.parse(String(event.data));
			const request = this.pending.get(message.id);
			if (request) {
				this.pending.delete(message.id);
				if (message.error) request.reject(new Error(JSON.stringify(message.error)));
				else request.resolve(message.result);
			}
			if (message.method === "Runtime.exceptionThrown")
				this.errors.push(JSON.stringify(message.params.exceptionDetails));
			if (message.method === "Page.javascriptDialogOpening") {
				this.dialogs.push(message.params.message);
				void this.send("Page.handleJavaScriptDialog", { accept: true }, message.sessionId);
			}
		});
		this.socket.addEventListener("close", () => {
			for (const request of this.pending.values())
				request.reject(new Error("Chrome DevTools disconnected"));
			this.pending.clear();
		});
	}
	async send(method: string, params: object = {}, sessionId?: string): Promise<any> {
		await this.ready;
		const id = ++this.nextId;
		return new Promise((resolve, reject) => {
			this.pending.set(id, { resolve, reject });
			this.socket.send(JSON.stringify({ id, method, params, sessionId }));
		});
	}
	close() {
		this.socket.close();
	}
}

it("browser agent timelines, coordinator channels, archives, safe text and live bounded history", {
	timeout: 45_000,
	skip: !existsSync(chrome),
}, async () => {
	const directory = mkdtempSync(join(tmpdir(), "pi-chat-chrome-"));
	const time = Date.now();
	const names: Record<string, string> = {
		parent: "Coordinator",
		researcher: "Researcher",
		writer: "Writer",
		retired: "Old worker",
	};
	const message = (
		id: string,
		from: string,
		to: string,
		text: string,
		offset = 0,
	): CoordinationChatRecord => ({
		event: { id, kind: "message", from, to, text },
		timestamp: time + offset,
		fromName: names[from] ?? from,
		toName: names[to] ?? to,
		goalId: "launch",
		goalTitle: "Launch briefing",
	});
	const channel = {
		id: "launch-room",
		name: "Launch coordination",
		members: ["researcher", "writer"],
		status: "open" as const,
	};
	const created: CoordinationChatRecord = {
		...message("channel-created", "parent", channel.id, "Launch coordination"),
		event: {
			id: "channel-created",
			kind: "channel_create",
			from: "parent",
			to: channel.id,
			text: channel.name,
			channel,
		},
	};
	const channelPost: CoordinationChatRecord = {
		...message(
			"channel-post",
			"researcher",
			channel.id,
			"Shared channel finding: three verified customer themes.",
			4,
		),
		event: {
			id: "channel-post",
			kind: "channel_message",
			from: "researcher",
			to: channel.id,
			text: "Shared channel finding: three verified customer themes.",
			channel,
			recipients: ["writer"],
		},
	};
	let value: CoordinationDashboardSnapshot = {
		sessionName: "Launch briefing team",
		dropped: 0,
		archive: { goalIds: ["old"], eventIds: [], memberIds: [], channelIds: [] },
		goals: [
			{ id: "launch", title: "Launch briefing", startedAt: time, status: "active" },
			{ id: "old", title: "Old work", startedAt: time - 1000, status: "completed" },
		],
		members: [
			{ id: "parent", name: "Coordinator", backend: "coordinator", status: "active" },
			{
				id: "researcher",
				name: "Researcher",
				role: "Research",
				backend: "pi",
				status: "active",
				goalId: "launch",
			},
			{
				id: "writer",
				name: "Writer",
				role: "Drafting",
				backend: "pi",
				status: "waiting",
				goalId: "launch",
			},
			{
				id: "silent-reviewer",
				name: "Silent reviewer",
				role: "Review",
				backend: "claude",
				status: "starting",
				goalId: "launch",
			},
			{ id: "retired", name: "Old worker", backend: "pi", status: "finished", goalId: "old" },
		],
		records: [
			created,
			message(
				"one",
				"researcher",
				"writer",
				"The launch notes are ready. Three customer themes have source links.",
				1,
			),
			message(
				"two",
				"writer",
				"researcher",
				"Thanks — I will draft the briefing and separate questions from verified facts.",
				2,
			),
			message(
				"hostile",
				"researcher",
				"writer",
				'<img src=x onerror="window.__chatXss=1"> is untrusted text.',
				3,
			),
			channelPost,
			{
				...message("old-event", "retired", "parent", "Archived previous goal message", -1000),
				goalId: "old",
			},
		],
	};
	const listeners = new Set<() => void>();
	const archiveCalls: CoordinationArchiveRequest[] = [];
	const publish = () => {
		for (const listener of listeners) listener();
	};
	const server = await startCoordinationDashboard({
		snapshot: () => value,
		subscribe: (listener) => {
			listeners.add(listener);
			return () => {
				listeners.delete(listener);
			};
		},
		archive: (request) => {
			archiveCalls.push(request);
			value = {
				...value,
				archive: {
					goalIds: ["old"],
					eventIds: value.records.map((record) => record.event.id),
					memberIds: value.members
						.filter((member) => member.id !== "parent")
						.map((member) => member.id),
					channelIds: [channel.id],
				},
			};
			publish();
		},
	});
	const child = spawn(
		chrome,
		[
			"--headless=new",
			"--remote-debugging-port=0",
			`--user-data-dir=${directory}`,
			"--no-first-run",
			"--no-default-browser-check",
			"--disable-background-networking",
			"about:blank",
		],
		{ stdio: ["ignore", "ignore", "pipe"] },
	);
	const exited = once(child, "exit");
	let diagnostics = "";
	const endpoint = new Promise<string>((resolve, reject) => {
		child.stderr.on("data", (chunk) => {
			diagnostics += chunk;
			const match = diagnostics.match(/DevTools listening on (ws:\/\/[^\s]+)/);
			if (match) resolve(match[1]);
		});
		child.once("error", reject);
		child.once("exit", () => reject(new Error(diagnostics || "Chrome exited before startup")));
	});
	let cdp: DevTools | undefined;
	try {
		cdp = new DevTools(await endpoint);
		const target = await cdp.send("Target.createTarget", { url: "about:blank" });
		const { sessionId } = await cdp.send("Target.attachToTarget", {
			targetId: target.targetId,
			flatten: true,
		});
		await cdp.send("Runtime.enable", {}, sessionId);
		await cdp.send("Page.enable", {}, sessionId);
		const evaluate = async (expression: string) => {
			const result = await cdp!.send(
				"Runtime.evaluate",
				{ expression, returnByValue: true, awaitPromise: true },
				sessionId,
			);
			if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
			return result.result.value;
		};
		const waitFor = async (expression: string) => {
			const deadline = Date.now() + 8000;
			while (!(await evaluate(expression)) && Date.now() < deadline)
				await new Promise((resolve) => setTimeout(resolve, 30));
			assert.ok(
				await evaluate(expression),
				`${expression}\n${await evaluate("document.body.innerText")}`,
			);
		};
		const toggleDetails = async () => {
			await evaluate("document.querySelector('#chat-options > summary').click()");
			assert.equal(
				await evaluate("document.querySelector('#details').getBoundingClientRect().height > 0"),
				true,
			);
			await evaluate("document.querySelector('#details').click()");
			assert.equal(await evaluate("document.querySelector('#chat-options').open"), false);
		};
		const agent = "document.querySelector('[data-agent-id=researcher]')";
		const room = "document.querySelector('[data-thread-id=\"channel:launch-room\"]')";
		await cdp.send(
			"Emulation.setDeviceMetricsOverride",
			{ width: 1440, height: 900, deviceScaleFactor: 1, mobile: false },
			sessionId,
		);
		await cdp.send(
			"Emulation.setEmulatedMedia",
			{ features: [{ name: "prefers-color-scheme", value: "light" }] },
			sessionId,
		);
		await cdp.send("Page.navigate", { url: server.url }, sessionId);
		await waitFor(`${agent} !== null`);
		await evaluate(`${agent}.click()`);
		await waitFor(
			"document.querySelector('#messages').innerText.includes('The launch notes are ready')",
		);
		assert.ok(await evaluate("document.body.innerText.includes('Silent reviewer')"));
		assert.equal(
			await evaluate("document.querySelector('[data-agent-id=retired]') === null"),
			true,
		);
		assert.equal(
			await evaluate("document.querySelectorAll('#thread-list [data-thread-id]').length"),
			1,
		);
		assert.equal(
			await evaluate("document.querySelector('#thread-list').innerText.includes('All activity')"),
			false,
		);
		assert.ok(
			await evaluate(
				"document.querySelector('[data-event-id=two]').getAttribute('aria-label').includes('Message from Writer')",
			),
		);
		assert.ok(
			await evaluate(
				"document.querySelector('[data-event-id=one]').getAttribute('aria-label').includes('Messaged Writer')",
			),
		);
		assert.equal(
			await evaluate(
				"document.querySelector('[data-event-id=one]').classList.contains('outgoing')",
			),
			true,
		);
		assert.equal(
			await evaluate(
				"document.querySelector('[data-event-id=two]').classList.contains('incoming')",
			),
			true,
		);
		assert.equal(
			await evaluate(
				"document.querySelector('[data-event-id=one]').getBoundingClientRect().right > document.querySelector('[data-event-id=two]').getBoundingClientRect().right",
			),
			true,
		);
		assert.equal(
			await evaluate(
				"getComputedStyle(document.querySelector('[data-event-id=one] .message-body')).backgroundColor !== getComputedStyle(document.querySelector('[data-event-id=two] .message-body')).backgroundColor",
			),
			true,
		);
		assert.ok(
			await evaluate(
				"document.querySelector('[data-agent-id=researcher] .agent-meta').innerText.includes('Shared channel finding')",
			),
		);
		assert.equal(await evaluate("window.__chatXss"), undefined);
		assert.equal(await evaluate("document.querySelectorAll('img').length"), 0);
		assert.ok(await evaluate("document.body.innerText.includes('<img src=x')"));
		assert.equal(
			await evaluate("getComputedStyle(document.querySelector('#load-older')).display"),
			"none",
		);
		assert.ok(
			await evaluate(
				"Math.abs(document.querySelector('.privacy-footer').getBoundingClientRect().bottom - innerHeight) < 2",
			),
		);
		assert.equal(await evaluate("document.querySelector('#chat-options').open"), false);
		assert.equal(await evaluate("document.querySelector('#messages .goal-tag') === null"), true);
		assert.equal(
			await evaluate("document.querySelector('.agent-meta').innerText.includes('coordinator')"),
			false,
		);
		await toggleDetails();
		await waitFor("document.querySelector('#messages').innerText.includes('hostile')");
		await toggleDetails();
		await evaluate(`${room}.click()`);
		await waitFor(
			"document.querySelector('#messages').innerText.includes('Shared channel finding')",
		);
		assert.equal(
			await evaluate(
				"document.querySelector('#messages').innerText.includes('The launch notes are ready')",
			),
			false,
		);
		await toggleDetails();
		await waitFor("document.querySelector('#messages').innerText.includes('Accepted recipients')");
		await toggleDetails();
		await evaluate("document.querySelector('#show-archived').click()");
		await waitFor("document.querySelector('[data-agent-id=retired]') !== null");
		await evaluate("document.querySelector('[data-agent-id=retired]').click()");
		await waitFor(
			"document.querySelector('#messages').innerText.includes('Archived previous goal message')",
		);
		await evaluate("document.querySelector('#show-archived').click()");
		await evaluate(`${agent}.click()`);
		await evaluate(
			"(() => { const input = document.querySelector('#search'); input.value = 'no-such-message'; input.dispatchEvent(new Event('input', {bubbles:true})); })()",
		);
		await waitFor(
			"!document.querySelector('#messages').innerText.includes('The launch notes are ready')",
		);
		assert.equal(await evaluate("document.querySelectorAll('[data-agent-id]').length"), 0);
		assert.equal(
			await evaluate("document.querySelector('#empty-inbox').innerText"),
			"No chats match your search.",
		);
		await evaluate(
			"(() => { const input = document.querySelector('#search'); input.value = ''; input.dispatchEvent(new Event('input', {bubbles:true})); })()",
		);
		await evaluate(`${agent}.click()`);
		value = {
			...value,
			records: [
				...value.records,
				message("live", "writer", "researcher", "Live peer update without a model call.", 5),
			],
		};
		publish();
		await waitFor("document.querySelector('#messages').innerText.includes('Live peer update')");
		assert.equal(
			await evaluate("document.querySelector('[data-agent-id=writer] .unread-dot') !== null"),
			true,
		);
		value = {
			...value,
			members: value.members.map((member) =>
				member.id === "writer" ? { ...member, name: "Lead writer" } : member,
			),
		};
		publish();
		await waitFor(
			"document.querySelector('[data-event-id=live]').getAttribute('aria-label').includes('Message from Lead writer')",
		);
		assert.equal(
			await evaluate("document.querySelector('[data-event-id=live] .direction-label').innerText"),
			"Lead writer",
		);
		const desktop = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
		writeFileSync(join(tmpdir(), "pi-auto-bots-desktop.png"), Buffer.from(desktop.data, "base64"));
		await cdp.send(
			"Emulation.setEmulatedMedia",
			{ features: [{ name: "prefers-color-scheme", value: "dark" }] },
			sessionId,
		);
		assert.equal(
			await evaluate(
				"getComputedStyle(document.documentElement).getPropertyValue('--panel').trim()",
			),
			"#171717",
		);
		const dark = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
		writeFileSync(join(tmpdir(), "pi-auto-bots-dark.png"), Buffer.from(dark.data, "base64"));
		await cdp.send(
			"Emulation.setEmulatedMedia",
			{ features: [{ name: "prefers-color-scheme", value: "light" }] },
			sessionId,
		);
		await cdp.send("Page.reload", {}, sessionId);
		await waitFor(`${agent} !== null`);
		await evaluate(`${agent}.click()`);
		await waitFor("document.querySelector('#messages').innerText.includes('Live peer update')");
		assert.equal(await evaluate("document.querySelectorAll('[data-event-id=live]').length"), 1);
		await cdp.send(
			"Emulation.setDeviceMetricsOverride",
			{ width: 390, height: 844, deviceScaleFactor: 1, mobile: true },
			sessionId,
		);
		await waitFor("document.documentElement.scrollWidth <= innerWidth");
		const mobile = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
		writeFileSync(
			join(tmpdir(), "pi-auto-bots-mobile-conversation.png"),
			Buffer.from(mobile.data, "base64"),
		);
		await evaluate("document.querySelector('#back').click()");
		await waitFor("!document.querySelector('#app').classList.contains('mobile-conversation')");
		assert.equal(await evaluate(`${agent}.getBoundingClientRect().width > 0`), true);
		const mobileInbox = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
		writeFileSync(
			join(tmpdir(), "pi-auto-bots-mobile-inbox.png"),
			Buffer.from(mobileInbox.data, "base64"),
		);
		await evaluate(`${agent}.click()`);
		await waitFor("document.querySelector('#app').classList.contains('mobile-conversation')");
		value = {
			...value,
			records: [
				created,
				...Array.from({ length: 999 }, (_, i) =>
					message(`bulk-${i}`, "researcher", "writer", `Bulk message ${i}`, i + 10),
				),
			],
		};
		publish();
		await waitFor("document.querySelector('#messages').innerText.includes('Bulk message 998')");
		assert.ok(await evaluate("document.querySelectorAll('[data-event-id]').length <= 100"));
		await evaluate(
			"(() => { document.querySelector('#back').click(); const input = document.querySelector('#search'); input.value = 'Bulk message 3'; input.dispatchEvent(new Event('input', {bubbles:true})); })()",
		);
		await evaluate(`${agent}.click()`);
		await waitFor("document.querySelector('[data-event-id=\"bulk-3\"] mark') !== null");
		assert.equal(await evaluate("document.querySelector('#search').value"), "Bulk message 3");
		await evaluate(
			"(() => { document.querySelector('#back').click(); const input = document.querySelector('#search'); input.value = ''; input.dispatchEvent(new Event('input', {bubbles:true})); })()",
		);
		await evaluate(`${agent}.click()`);
		// Let the render's follow-to-bottom frame finish before simulating a user scroll.
		await evaluate(
			"new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))",
		);
		await evaluate("document.querySelector('#transcript').scrollTop = 0");
		await waitFor("document.querySelector('#transcript').scrollTop === 0");
		const anchor = await evaluate(
			"document.querySelector('#messages').firstElementChild.dataset.eventId",
		);
		value = {
			...value,
			records: [
				created,
				...value.records.slice(2),
				message("bulk-999", "researcher", "writer", "New message beyond paused window", 1010),
			],
			dropped: 1,
		};
		publish();
		await waitFor("document.querySelector('#jump-latest').hidden === false");
		assert.equal(
			await evaluate("document.querySelector('#messages').firstElementChild.dataset.eventId"),
			anchor,
		);
		assert.ok(await evaluate("document.querySelector('#transcript').scrollTop < 2"));
		await evaluate("document.querySelector('#jump-latest').click()");
		await waitFor(
			"document.querySelector('#messages').innerText.includes('New message beyond paused window')",
		);
		// Clear is a confirmed, authenticated archive operation; archived work is retrievable.
		await cdp.send(
			"Emulation.setDeviceMetricsOverride",
			{ width: 1440, height: 900, deviceScaleFactor: 1, mobile: false },
			sessionId,
		);
		await evaluate("document.querySelector('#chat-options > summary').click()");
		assert.equal(
			await evaluate("document.querySelector('#clear-chats').getBoundingClientRect().height > 0"),
			true,
		);
		await evaluate("document.querySelector('#clear-chats').click()");
		await waitFor("document.querySelectorAll('#thread-list [data-thread-id]').length === 0");
		assert.deepEqual(archiveCalls, [{ scope: "all" }]);
		assert.equal(cdp.dialogs.length, 1);
		await evaluate("document.querySelector('#show-archived').click()");
		await waitFor(`${room} !== null`);
		await evaluate(`${agent}.click()`);
		await waitFor("document.querySelector('#messages').innerText.includes('Bulk message')");
		// Metadata-only changes refresh badges even while archived history stays visible.
		value = {
			...value,
			archive: { goalIds: ["old"], eventIds: [], memberIds: [], channelIds: [] },
		};
		publish();
		await waitFor("document.querySelector('#messages .archive-badge') === null");
		value = {
			...value,
			archive: { goalIds: ["old", "launch"], eventIds: [], memberIds: [], channelIds: [] },
			goals: value.goals!.map((goal) => ({ ...goal, status: "completed" })),
		};
		publish();
		await waitFor("document.querySelector('#messages .archive-badge') !== null");
		// A completed-goal metadata delta hides only that goal, without deleting records.
		await evaluate("document.querySelector('#show-archived').click()");
		await waitFor("document.querySelector('[data-agent-id=researcher]') === null");
		assert.equal(
			await evaluate("document.querySelectorAll('#thread-list [data-thread-id]').length"),
			0,
		);
		value = {
			...value,
			operational: {
				cursor: 42,
				roles: [],
				tasks: [
					{
						id: "protected-task",
						owner: "worker",
						state: "blocked",
						revision: 3,
						dependencies: ["input-task"],
						dependencyStates: [{ id: "input-task", state: "review" }],
						blockers: ["hold"],
					},
				],
				blockers: [
					{
						id: "hold",
						severity: "critical",
						summary: '<img src=x onerror="window.__opsXss=1">',
						pendingAcknowledgements: ["worker"],
					},
				],
				artifacts: [
					{
						artifactId: "pack",
						version: 2,
						digest: "sha256:abc",
						state: "frozen",
						consumers: ["protected-task"],
					},
				],
				budgets: [{ id: "gpu", limit: 4, used: 2, dispatchedUnknown: 1 }],
				deliveries: [{ id: "delivery", targetRoleId: "worker", state: "delivered", wakeCount: 1 }],
				audit: [{ seq: 42, type: "permit.dispatched", actorRoleId: "worker", timestamp: time }],
			},
		};
		publish();
		await waitFor("!document.querySelector('#operational-summary').hidden");
		assert.equal(await evaluate("document.querySelector('#operations-panel').open"), false);
		assert.equal(
			await evaluate(
				"document.querySelector('#operational-summary').getBoundingClientRect().height",
			),
			0,
		);
		await evaluate(
			"document.querySelector('#chat-options > summary').click(); document.querySelector('#operations-panel > summary').click()",
		);
		await waitFor("document.querySelectorAll('#operational-summary details').length === 6");
		await evaluate(
			"document.querySelectorAll('#operational-summary details').forEach(node => node.open = true)",
		);
		await waitFor(
			"document.querySelector('#operational-summary').innerText.includes('input-task=review')",
		);
		assert.ok(
			await evaluate("document.querySelector('#operational-summary').innerText.includes('pack@2')"),
		);
		assert.ok(
			await evaluate(
				"document.querySelector('#operational-summary').innerText.includes('1 outcome_unknown')",
			),
		);
		assert.ok(
			await evaluate(
				"document.querySelector('#operational-summary').innerText.includes('permit.dispatched')",
			),
		);
		assert.equal(
			await evaluate(
				"document.querySelectorAll('#operational-summary button, #operational-summary input, #operational-summary img').length",
			),
			0,
		);
		assert.equal(await evaluate("window.__opsXss"), undefined);
		value = { ...value, operational: { ...value.operational!, cursor: 43 } };
		publish();
		await waitFor("document.querySelector('#operational-summary').innerText.includes('audit 43')");
		assert.equal(
			await evaluate("document.querySelectorAll('#operational-summary details[open]').length"),
			6,
		);
		value = { ...value, operational: undefined };
		publish();
		await waitFor("document.querySelector('#operational-summary').hidden");
		// Exercise actual ledger-to-conversation records, not only static fixtures.
		const broker = new CoordinationBroker("browser", "branch");
		const coordinator = { roleId: "coordinator", runId: "coordinator-run", generation: 1 };
		const worker = { roleId: "review-worker", runId: "worker-run", generation: 1 };
		broker.admitRole({
			roleId: "coordinator",
			capabilities: ["coordinator"],
			idempotencyKey: "admit",
		});
		broker.bindRole({ roleId: "coordinator", runId: coordinator.runId, idempotencyKey: "bind" });
		broker.admitRole({
			actor: coordinator,
			roleId: worker.roleId,
			capabilities: ["worker"],
			idempotencyKey: "worker",
		});
		broker.bindRole({ roleId: worker.roleId, runId: worker.runId, idempotencyKey: "worker-bind" });
		const reviewChannel = broker.createChannel({
			actor: coordinator,
			name: "Live review",
			purpose: "Verify conversation rendering",
			members: [
				{ roleId: "coordinator", mode: "participate" },
				{ roleId: worker.roleId, mode: "participate" },
			],
			idempotencyKey: "room",
		}).event;
		const channelMessage = broker.sendChannel({
			actor: worker,
			channelId: reviewChannel.payload.id,
			expectedRevision: 1,
			text: "Worker channel message",
			idempotencyKey: "post",
		}).event;
		value = {
			sessionName: "Coordination team",
			dropped: 0,
			members: [
				{ id: "parent", name: "Coordinator", status: "active" },
				...coordinationChatMembers(broker.snapshot(), [
					{ id: worker.runId, name: "Live review worker", status: "active" },
				]),
			],
			records: [reviewChannel, channelMessage].flatMap((event) =>
				coordinationChatRecords(event, broker.snapshot()),
			),
		};
		publish();
		await waitFor("document.querySelector('[data-agent-id=\"review-worker\"]') !== null");
		await evaluate("document.querySelector('[data-agent-id=\"review-worker\"]').click()");
		await waitFor(
			"document.querySelector('#messages').innerText.includes('Worker channel message')",
		);
		assert.ok(await evaluate("document.body.innerText.includes('Live review worker')"));
		const reviewRoom = `document.querySelector('[data-thread-id="channel:${reviewChannel.payload.id}"]')`;
		await evaluate(`${reviewRoom}.click()`);
		await waitFor(
			"document.querySelector('#messages').innerText.includes('Worker channel message')",
		);
		const followUp = broker.sendChannel({
			actor: coordinator,
			channelId: reviewChannel.payload.id,
			expectedRevision: 1,
			text: "Coordinator live reply",
			idempotencyKey: "reply",
		}).event;
		value = {
			...value,
			records: [...value.records, ...coordinationChatRecords(followUp, broker.snapshot())],
		};
		publish();
		await waitFor(
			"document.querySelector('#messages').innerText.includes('Coordinator live reply')",
		);
		await server.close();
		await waitFor(
			"document.querySelector('#connection-label').innerText.toLowerCase() === 'reconnecting'",
		);
		assert.deepEqual(cdp.errors, []);
	} finally {
		cdp?.close();
		child.kill("SIGTERM");
		const kill = setTimeout(() => child.kill("SIGKILL"), 2000);
		await exited;
		clearTimeout(kill);
		await server.close();
		rmSync(directory, { recursive: true, force: true });
	}
});
