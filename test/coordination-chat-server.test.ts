import assert from "node:assert/strict";
import { type ClientRequest, request as httpRequest, type IncomingMessage } from "node:http";
import { it } from "node:test";
import type { CoordinationChatRecord } from "../src/chat/records.ts";
import {
	type CoordinationArchiveRequest,
	type CoordinationDashboardSnapshot,
	type CoordinationDashboardSource,
	startCoordinationDashboard,
} from "../src/chat/server.ts";

function record(id: string): CoordinationChatRecord {
	return {
		timestamp: Date.now(),
		fromName: "Alpha",
		toName: "Beta",
		event: { id, kind: "message", from: "alpha", to: "beta", text: `message ${id}` },
	};
}

class MutableSource implements CoordinationDashboardSource {
	value: CoordinationDashboardSnapshot = {
		sessionName: "Test session",
		records: [],
		dropped: 0,
		members: [{ id: "alpha", name: "Alpha", status: "running" }],
	};
	listeners = new Set<() => void>();
	subscriptions = 0;
	archive?: (request: CoordinationArchiveRequest) => void;
	unsubscriptions = 0;

	snapshot(): CoordinationDashboardSnapshot {
		return this.value;
	}
	subscribe(listener: () => void): () => void {
		this.subscriptions++;
		this.listeners.add(listener);
		return () => {
			this.unsubscriptions++;
			this.listeners.delete(listener);
		};
	}
	publish(value: CoordinationDashboardSnapshot): void {
		this.value = value;
		for (const listener of this.listeners) listener();
	}
}

function parsed(serverUrl: string): { origin: string; secret: string } {
	const url = new URL(serverUrl);
	return { origin: url.origin, secret: url.hash.slice(1).split("&", 1)[0] };
}

async function request(
	origin: string,
	path: string,
	options: { method?: string; headers?: Record<string, string>; body?: string } = {},
): Promise<{ status: number; headers: IncomingMessage["headers"]; body: string }> {
	const url = new URL(path, origin);
	return await new Promise((resolve, reject) => {
		const req = httpRequest(url, options, (response) => {
			const chunks: Buffer[] = [];
			response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
			response.on("end", () =>
				resolve({
					status: response.statusCode ?? 0,
					headers: response.headers,
					body: Buffer.concat(chunks).toString("utf8"),
				}),
			);
		});
		req.once("error", reject);
		req.end(options.body);
	});
}

function openEvents(
	origin: string,
	secret: string,
): {
	request: ClientRequest;
	response: Promise<IncomingMessage>;
	events: Promise<Array<{ event: string; data: unknown }>>;
} {
	let resolveResponse!: (response: IncomingMessage) => void;
	let resolveEvents!: (events: Array<{ event: string; data: unknown }>) => void;
	const responsePromise = new Promise<IncomingMessage>((resolve) => {
		resolveResponse = resolve;
	});
	const eventsPromise = new Promise<Array<{ event: string; data: unknown }>>((resolve) => {
		resolveEvents = resolve;
	});
	const events: Array<{ event: string; data: unknown }> = [];
	const req = httpRequest(
		`${origin}/api/events?token=${encodeURIComponent(secret)}`,
		(response) => {
			resolveResponse(response);
			response.setEncoding("utf8");
			let buffered = "";
			response.on("data", (chunk: string) => {
				buffered += chunk;
				for (;;) {
					const boundary = buffered.indexOf("\n\n");
					if (boundary < 0) break;
					const block = buffered.slice(0, boundary);
					buffered = buffered.slice(boundary + 2);
					if (block.startsWith(":")) continue;
					const name = block.match(/^event: (.+)$/m)?.[1];
					const data = block.match(/^data: (.+)$/m)?.[1];
					if (name && data) events.push({ event: name, data: JSON.parse(data) });
					if (events.length >= 2) resolveEvents(events);
				}
			});
		},
	);
	req.end();
	return { request: req, response: responsePromise, events: eventsPromise };
}

it("binds a capability URL and protects API requests", async (t) => {
	const source = new MutableSource();
	const server = await startCoordinationDashboard(source);
	t.after(() => server.close());
	const { origin, secret } = parsed(server.url);
	const url = new URL(origin);

	assert.equal(url.hostname, "127.0.0.1");
	assert.notEqual(url.port, "");
	assert.match(secret, /^[a-f0-9]{64}$/);
	assert.equal(source.subscriptions, 1);

	const unauthorized = await request(origin, "/api/snapshot");
	assert.equal(unauthorized.status, 401);
	assert.equal(unauthorized.headers["access-control-allow-origin"], undefined);
	assert.equal(unauthorized.headers["cache-control"], "no-store");

	const authorized = await request(origin, "/api/snapshot", {
		headers: { Authorization: `Bearer ${secret}`, Origin: origin, "Sec-Fetch-Site": "same-origin" },
	});
	assert.equal(authorized.status, 200);
	assert.deepEqual(JSON.parse(authorized.body), source.value);
	assert.equal(authorized.headers["x-content-type-options"], "nosniff");
	assert.equal(authorized.headers["referrer-policy"], "no-referrer");
	assert.match(String(authorized.headers["content-security-policy"] ?? ""), /default-src 'none'/);

	assert.equal(
		(
			await request(origin, "/api/snapshot", {
				headers: { Authorization: `Bearer ${secret}`, Origin: "https://attacker.invalid" },
			})
		).status,
		403,
	);
	assert.equal(
		(
			await request(origin, "/api/snapshot", {
				headers: { Authorization: `Bearer ${secret}`, "Sec-Fetch-Site": "cross-site" },
			})
		).status,
		403,
	);
	assert.equal(
		(
			await request(origin, "/api/snapshot", {
				headers: { Authorization: `Bearer ${secret}`, Host: "attacker.invalid" },
			})
		).status,
		400,
	);
	assert.equal(
		(
			await request(origin, "/api/snapshot", {
				method: "POST",
				headers: { Authorization: `Bearer ${secret}` },
			})
		).status,
		405,
	);
	assert.equal((await request(origin, "/dashboard/../coordination-chat.ts")).status, 404);
	assert.equal((await request(origin, "/package.json")).status, 404);
	for (const path of ["/", "/app.js", "/style.css"]) {
		const asset = await request(origin, path);
		assert.equal(asset.status, 200);
		assert.equal(asset.headers["cache-control"], "no-store");
		assert.ok(
			!asset.body.includes(secret),
			"static assets must never embed the capability or session snapshot",
		);
	}
});

it("streams an initial snapshot and coalesced record, removal, and roster deltas", async (t) => {
	const source = new MutableSource();
	source.value = { ...source.value, records: [record("old")] };
	const server = await startCoordinationDashboard(source);
	t.after(() => server.close());
	const { origin, secret } = parsed(server.url);
	const stream = openEvents(origin, secret);
	t.after(() => stream.request.destroy());
	const response = await stream.response;
	assert.equal(response.statusCode, 200);
	assert.match(response.headers["content-type"] ?? "", /^text\/event-stream/);

	const next = record("next");
	source.publish({
		sessionName: "Renamed",
		records: [next],
		dropped: 1,
		members: [{ id: "beta", name: "Beta", status: "idle", backend: "pi" }],
	});
	// A burst is one update and reflects the final source state.
	source.publish({ ...source.value, dropped: 2 });

	const events = await Promise.race([
		stream.events,
		new Promise<never>((_, reject) =>
			setTimeout(() => reject(new Error("SSE update timed out")), 1_000),
		),
	]);
	assert.equal(events[0].event, "snapshot");
	assert.deepEqual(
		(events[0].data as CoordinationDashboardSnapshot).records.map((item) => item.event.id),
		["old"],
	);
	assert.equal(events[1].event, "update");
	assert.deepEqual(events[1].data, {
		records: [next],
		removedIds: ["old"],
		dropped: 2,
		members: source.value.members,
		sessionName: "Renamed",
	});
});

it("isolates failing update sources and cleans up failed startup", { timeout: 3000 }, async (t) => {
	const source = new MutableSource();
	const server = await startCoordinationDashboard(source);
	t.after(() => server.close());
	const { origin, secret } = parsed(server.url);
	const stream = openEvents(origin, secret);
	t.after(() => stream.request.destroy());
	const response = await stream.response;
	const closed = new Promise<void>((resolve) => response.once("close", resolve));
	source.snapshot = () => {
		throw new Error("observer source failed");
	};
	source.publish(source.value);
	await closed;
	assert.equal(response.destroyed, true);
	await assert.rejects(
		() =>
			startCoordinationDashboard({
				snapshot: () => new MutableSource().value,
				subscribe: (listener) => {
					listener();
					throw new Error("subscription failed");
				},
			}),
		/subscription failed/,
	);
});

it("does not emit empty deltas for unchanged lifecycle ticks", { timeout: 3000 }, async (t) => {
	const source = new MutableSource();
	const server = await startCoordinationDashboard(source);
	t.after(() => server.close());
	const { origin, secret } = parsed(server.url);
	const stream = openEvents(origin, secret);
	t.after(() => stream.request.destroy());
	const response = await stream.response;
	let body = "";
	response.on("data", (chunk) => {
		body += chunk;
	});
	source.publish(source.value);
	await new Promise((resolve) => setTimeout(resolve, 40));
	assert.doesNotMatch(body, /event: update/);
	source.publish({ ...source.value, records: [record("new")] });
	const events = await stream.events;
	assert.equal(events[1].event, "update");
});

it("archives only via authenticated same-origin bounded JSON and emits metadata deltas", {
	timeout: 4000,
}, async (t) => {
	const source = new MutableSource();
	const calls: CoordinationArchiveRequest[] = [];
	source.archive = (action) => {
		calls.push(action);
		source.publish({
			...source.value,
			archive: { goalIds: [action.id ?? "goal-1"], eventIds: [], memberIds: [], channelIds: [] },
			goals: [{ id: "goal-1", title: "Example goal", status: "completed", startedAt: 1 }],
		});
	};
	const server = await startCoordinationDashboard(source);
	t.after(() => server.close());
	const { origin, secret } = parsed(server.url);
	const headers = {
		Origin: origin,
		Authorization: `Bearer ${secret}`,
		"Content-Type": "application/json",
		"Sec-Fetch-Site": "same-origin",
	};
	const post = (body: string, override = {}) =>
		request(origin, "/api/archive", { method: "POST", headers: { ...headers, ...override }, body });
	assert.equal((await request(origin, "/api/archive")).status, 405);
	assert.equal((await post('{"scope":"all"}', { Authorization: "Bearer wrong" })).status, 401);
	assert.equal((await post('{"scope":"all"}', { Origin: "https://evil.invalid" })).status, 403);
	assert.equal((await post('{"scope":"all"}', { "Sec-Fetch-Site": "cross-site" })).status, 403);
	assert.equal(
		(
			await request(origin, "/api/archive", {
				method: "POST",
				headers: { Authorization: `Bearer ${secret}`, "Content-Type": "application/json" },
				body: '{"scope":"all"}',
			})
		).status,
		403,
	);
	assert.equal((await post('{"scope":"all"}', { "Content-Type": "text/plain" })).status, 415);
	for (const body of [
		"{",
		"null",
		"[]",
		'{"scope":"agent"}',
		'{"scope":"delete"}',
		'{"scope":"all","id":"x"}',
		'{"scope":"all","command":"send"}',
	]) {
		assert.equal((await post(body)).status, 400);
	}
	assert.equal((await post(JSON.stringify({ scope: "agent", id: "x".repeat(9000) }))).status, 413);
	assert.equal(calls.length, 0);
	const stream = openEvents(origin, secret);
	t.after(() => stream.request.destroy());
	await stream.response;
	const archived = await post('{"scope":"goal","id":"goal-1"}');
	assert.equal(archived.status, 200);
	assert.deepEqual(JSON.parse(archived.body), { ok: true });
	assert.deepEqual(calls, [{ scope: "goal", id: "goal-1" }]);
	const events = await stream.events;
	assert.deepEqual((events[1].data as any).archive.goalIds, ["goal-1"]);
	assert.equal((events[1].data as any).goals[0].status, "completed");
});

it("caps simultaneous event streams", async (t) => {
	const source = new MutableSource();
	const server = await startCoordinationDashboard(source);
	t.after(() => server.close());
	const { origin, secret } = parsed(server.url);
	const streams = Array.from({ length: 9 }, () => openEvents(origin, secret));
	t.after(() => {
		for (const stream of streams) stream.request.destroy();
	});
	const responses = await Promise.all(streams.map((stream) => stream.response));
	assert.deepEqual(
		responses.map((response) => response.statusCode),
		[200, 200, 200, 200, 200, 200, 200, 200, 503],
	);
});

it("rejects bad event credentials and releases subscriptions and sockets on idempotent close", async () => {
	const source = new MutableSource();
	const server = await startCoordinationDashboard(source);
	const { origin, secret } = parsed(server.url);
	assert.equal((await request(origin, "/api/events?token=wrong")).status, 401);

	const stream = openEvents(origin, secret);
	const response = await stream.response;
	const closed = new Promise<void>((resolve) => response.once("close", resolve));
	const first = server.close();
	assert.equal(server.close(), first);
	await first;
	await closed;
	assert.equal(response.destroyed, true);
	assert.equal(source.listeners.size, 0);
	assert.equal(source.unsubscriptions, 1);
	source.publish({ ...source.value, records: [record("late")] });
	stream.request.destroy();
	await assert.rejects(
		request(origin, "/api/snapshot", {
			headers: { Authorization: `Bearer ${secret}` },
		}),
	);
});
