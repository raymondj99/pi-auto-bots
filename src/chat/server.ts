import { randomBytes, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Socket } from "node:net";
import type { CoordinationOperationalView } from "../coordination/operational.ts";
import type { CoordinationChatRecord } from "./records.ts";

export interface CoordinationDashboardMember {
	id: string;
	name: string;
	status: string;
	role?: string;
	backend?: string;
	interactive?: boolean;
	goalId?: string;
}

export interface CoordinationChatArchive {
	goalIds: string[];
	eventIds: string[];
	memberIds: string[];
	channelIds: string[];
}
export interface CoordinationChatGoal {
	id: string;
	title: string;
	status: string;
	startedAt: number;
}
export interface CoordinationArchiveRequest {
	scope: "all" | "agent" | "channel" | "goal";
	id?: string;
}

export interface CoordinationDashboardSnapshot {
	sessionName: string;
	records: readonly CoordinationChatRecord[];
	dropped: number;
	members: readonly CoordinationDashboardMember[];
	archive?: CoordinationChatArchive;
	goals?: CoordinationChatGoal[];
	operational?: CoordinationOperationalView;
}

export interface CoordinationDashboardSource {
	snapshot(): CoordinationDashboardSnapshot;
	subscribe(listener: () => void): () => void;
	archive?(request: CoordinationArchiveRequest): void | Promise<void>;
}

export interface CoordinationDashboardServer {
	url: string;
	close(): Promise<void>;
}

const HOST = "127.0.0.1";
const MAX_SSE_CLIENTS = 8;
// A retained snapshot can legitimately approach 2 MiB. Keep enough room for one
// complete snapshot while still bounding a client that stops reading.
const MAX_SSE_BUFFER_BYTES = 4 * 1024 * 1024;
const UPDATE_DELAY_MS = 10;
const HEARTBEAT_MS = 15_000;
const STATIC_ASSETS = new Map<string, { url: URL; type: string }>([
	[
		"/",
		{ url: new URL("../dashboard/index.html", import.meta.url), type: "text/html; charset=utf-8" },
	],
	[
		"/app.js",
		{
			url: new URL("../dashboard/app.js", import.meta.url),
			type: "text/javascript; charset=utf-8",
		},
	],
	[
		"/style.css",
		{ url: new URL("../dashboard/style.css", import.meta.url), type: "text/css; charset=utf-8" },
	],
]);

const COMMON_HEADERS = {
	"Content-Security-Policy":
		"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'none'; font-src 'none'; object-src 'none'; frame-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
	"Referrer-Policy": "no-referrer",
	"X-Content-Type-Options": "nosniff",
} as const;

function sameSecret(candidate: string | null, secret: string): boolean {
	if (candidate === null) return false;
	const actual = Buffer.from(candidate);
	const expected = Buffer.from(secret);
	return actual.length === expected.length && timingSafeEqual(actual, expected);
}

function writeJson(response: ServerResponse, status: number, value: unknown): void {
	response.writeHead(status, {
		...COMMON_HEADERS,
		"Cache-Control": "no-store",
		"Content-Type": "application/json; charset=utf-8",
	});
	response.end(JSON.stringify(value));
}

function reject(response: ServerResponse, status: number, message: string, extra = {}): void {
	writeJson(response, status, { error: message, ...extra });
}

function apiRequestAllowed(
	request: { headers: Record<string, string | string[] | undefined> },
	origin: string,
): boolean {
	const requestOrigin = request.headers.origin;
	if (Array.isArray(requestOrigin) || (requestOrigin !== undefined && requestOrigin !== origin))
		return false;
	const fetchSite = request.headers["sec-fetch-site"];
	if (Array.isArray(fetchSite)) return false;
	return fetchSite === undefined || fetchSite === "same-origin" || fetchSite === "none";
}

function recordIds(snapshot: CoordinationDashboardSnapshot): Set<string> {
	return new Set(snapshot.records.map((record) => record.event.id));
}

function sameJson(left: unknown, right: unknown): boolean {
	return JSON.stringify(left) === JSON.stringify(right);
}

function sseEvent(name: string, value: unknown): string {
	return `event: ${name}\ndata: ${JSON.stringify(value)}\n\n`;
}

async function archiveBody(request: IncomingMessage): Promise<CoordinationArchiveRequest> {
	const body = await new Promise<string>((resolve, reject) => {
		let size = 0;
		const chunks: Buffer[] = [];
		const timer = setTimeout(() => finish(new Error("Archive request timed out")), 3000);
		const cleanup = () => {
			clearTimeout(timer);
			request.off("data", onData);
			request.off("end", onEnd);
			request.off("error", onError);
			request.off("aborted", onAbort);
		};
		const finish = (error?: Error) => {
			cleanup();
			if (error) {
				request.resume();
				reject(error);
			} else resolve(Buffer.concat(chunks).toString("utf8"));
		};
		const onData = (chunk: Buffer) => {
			size += chunk.length;
			if (size > 8192) finish(new Error("Archive request exceeds 8KB"));
			else chunks.push(chunk);
		};
		const onEnd = () => finish();
		const onError = (error: Error) => finish(error);
		const onAbort = () => finish(new Error("Archive request aborted"));
		request.on("data", onData);
		request.once("end", onEnd);
		request.once("error", onError);
		request.once("aborted", onAbort);
	});
	const value = JSON.parse(body) as CoordinationArchiveRequest;
	if (
		!value ||
		typeof value !== "object" ||
		Array.isArray(value) ||
		!["all", "agent", "channel", "goal"].includes(value.scope) ||
		Object.keys(value).some((key) => key !== "scope" && key !== "id") ||
		(value.scope !== "all" &&
			(typeof value.id !== "string" || !value.id.trim() || value.id.length > 200)) ||
		(value.scope === "all" && value.id !== undefined)
	)
		throw new Error("Invalid archive scope or ID");
	return value;
}

/** Start a loopback-only observer; its sole mutation organizes archived history. */
export async function startCoordinationDashboard(
	source: CoordinationDashboardSource,
): Promise<CoordinationDashboardServer> {
	const secret = randomBytes(32).toString("hex");
	const clients = new Set<ServerResponse>();
	const sockets = new Set<Socket>();
	let previous = source.snapshot();
	let updateTimer: NodeJS.Timeout | undefined;
	let heartbeatTimer: NodeJS.Timeout | undefined;
	let unsubscribe: (() => void) | undefined;
	let closePromise: Promise<void> | undefined;
	let expectedHost = "";
	let origin = "";

	const send = (client: ServerResponse, body: string): void => {
		if (
			client.destroyed ||
			client.writableEnded ||
			client.writableLength + Buffer.byteLength(body) > MAX_SSE_BUFFER_BYTES
		) {
			clients.delete(client);
			client.destroy();
			return;
		}
		// A false return only says Node crossed its small stream high-water mark;
		// writableLength above is the hard per-client cap used to identify a slow peer.
		try {
			client.write(body);
		} catch {
			clients.delete(client);
			client.destroy();
		}
	};

	const flushUpdate = (): void => {
		updateTimer = undefined;
		const next = source.snapshot();
		const oldIds = recordIds(previous);
		const nextIds = recordIds(next);
		const records = next.records.filter((record) => !oldIds.has(record.event.id));
		const removedIds = previous.records
			.filter((record) => !nextIds.has(record.event.id))
			.map((record) => record.event.id);
		const update: {
			records: CoordinationChatRecord[];
			removedIds: string[];
			dropped: number;
			members?: CoordinationDashboardMember[];
			sessionName?: string;
			archive?: CoordinationChatArchive;
			goals?: CoordinationChatGoal[];
			operational?: CoordinationOperationalView | null;
		} = { records, removedIds, dropped: next.dropped };
		if (!sameJson(previous.members, next.members)) update.members = [...next.members];
		if (previous.sessionName !== next.sessionName) update.sessionName = next.sessionName;
		if (!sameJson(previous.archive, next.archive))
			update.archive = next.archive ?? { goalIds: [], eventIds: [], memberIds: [], channelIds: [] };
		if (!sameJson(previous.goals, next.goals)) update.goals = next.goals ?? [];
		if (!sameJson(previous.operational, next.operational))
			update.operational = next.operational ?? null;
		const unchanged =
			records.length === 0 &&
			removedIds.length === 0 &&
			next.dropped === previous.dropped &&
			update.members === undefined &&
			update.sessionName === undefined &&
			update.archive === undefined &&
			update.goals === undefined &&
			update.operational === undefined;
		previous = next;
		if (unchanged) return;
		const body = sseEvent("update", update);
		for (const client of [...clients]) send(client, body);
	};

	const scheduleUpdate = (): void => {
		if (updateTimer || closePromise) return;
		updateTimer = setTimeout(() => {
			try {
				flushUpdate();
			} catch {
				updateTimer = undefined;
				// A failed observer source must not crash Pi or leave clients showing a
				// falsely live feed. Reconnecting clients receive a fresh snapshot.
				for (const client of clients) client.destroy();
				clients.clear();
			}
		}, UPDATE_DELAY_MS);
		updateTimer.unref();
	};

	const server: Server = createServer(async (request, response) => {
		try {
			for (const [name, value] of Object.entries(COMMON_HEADERS)) response.setHeader(name, value);
			if (request.headers.host !== expectedHost) {
				reject(response, 400, "Invalid Host");
				return;
			}
			if (
				request.method !== "GET" &&
				!(request.method === "POST" && request.url === "/api/archive")
			) {
				response.setHeader("Allow", "GET");
				reject(response, 405, "Method not allowed");
				return;
			}

			const requestUrl = new URL(request.url ?? "/", origin);
			if (requestUrl.origin !== origin) {
				reject(response, 400, "Invalid request target");
				return;
			}
			if (requestUrl.pathname.startsWith("/api/") && !apiRequestAllowed(request, origin)) {
				reject(response, 403, "Cross-site request denied");
				return;
			}

			if (requestUrl.pathname === "/api/archive") {
				if (request.method !== "POST") {
					response.setHeader("Allow", "POST");
					reject(response, 405, "Use POST for history archiving");
					return;
				}
				if (request.headers.origin !== origin) {
					reject(response, 403, "Same-origin archive request required");
					return;
				}
				const authorization = request.headers.authorization;
				if (
					!sameSecret(authorization?.startsWith("Bearer ") ? authorization.slice(7) : null, secret)
				) {
					reject(response, 401, "Unauthorized");
					return;
				}
				if (request.headers["content-type"]?.split(";", 1)[0].trim() !== "application/json") {
					reject(response, 415, "Archive requests require application/json");
					return;
				}
				if (!source.archive) {
					reject(response, 409, "History archiving unavailable");
					return;
				}
				try {
					const action = await archiveBody(request);
					await source.archive(action);
					scheduleUpdate();
					writeJson(response, 200, { ok: true });
				} catch (error) {
					const message = error instanceof Error ? error.message : "Archive failed";
					reject(response, message.includes("8KB") ? 413 : 400, message);
				}
				return;
			}

			if (requestUrl.pathname === "/api/snapshot") {
				const authorization = request.headers.authorization;
				const candidate = authorization?.startsWith("Bearer ") ? authorization.slice(7) : null;
				if (!sameSecret(candidate, secret)) {
					reject(response, 401, "Unauthorized");
					return;
				}
				writeJson(response, 200, source.snapshot());
				return;
			}

			if (requestUrl.pathname === "/api/events") {
				if (!sameSecret(requestUrl.searchParams.get("token"), secret)) {
					reject(response, 401, "Unauthorized");
					return;
				}
				if (clients.size >= MAX_SSE_CLIENTS) {
					reject(response, 503, "Too many event streams");
					return;
				}
				response.writeHead(200, {
					...COMMON_HEADERS,
					"Cache-Control": "no-store",
					Connection: "keep-alive",
					"Content-Type": "text/event-stream; charset=utf-8",
					"X-Accel-Buffering": "no",
				});
				clients.add(response);
				const remove = (): void => {
					clients.delete(response);
				};
				request.once("close", remove);
				response.once("close", remove);
				response.once("error", remove);
				send(response, sseEvent("snapshot", source.snapshot()));
				return;
			}

			const asset = STATIC_ASSETS.get(requestUrl.pathname);
			if (!asset || requestUrl.search) {
				reject(response, 404, "Not found");
				return;
			}
			let contents: Buffer;
			try {
				contents = await readFile(asset.url);
			} catch {
				reject(response, 503, "Dashboard asset unavailable");
				return;
			}
			response.writeHead(200, {
				...COMMON_HEADERS,
				"Cache-Control": "no-store",
				"Content-Type": asset.type,
				"Content-Length": contents.byteLength,
			});
			response.end(contents);
		} catch {
			if (!response.headersSent) reject(response, 400, "Bad request");
			else response.destroy();
		}
	});

	server.on("connection", (socket) => {
		sockets.add(socket);
		socket.once("close", () => sockets.delete(socket));
	});

	await new Promise<void>((resolve, rejectListen) => {
		server.once("error", rejectListen);
		server.listen(0, HOST, () => {
			server.off("error", rejectListen);
			resolve();
		});
	});

	const address = server.address();
	if (!address || typeof address === "string") {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		throw new Error("Dashboard did not bind a TCP port");
	}
	expectedHost = `${HOST}:${address.port}`;
	origin = `http://${expectedHost}`;
	try {
		unsubscribe = source.subscribe(scheduleUpdate);
	} catch (error) {
		if (updateTimer) clearTimeout(updateTimer);
		for (const socket of sockets) socket.destroy();
		await new Promise<void>((resolve) => server.close(() => resolve()));
		throw error;
	}
	heartbeatTimer = setInterval(() => {
		for (const client of [...clients]) send(client, ": heartbeat\n\n");
	}, HEARTBEAT_MS);
	heartbeatTimer.unref();

	return {
		url: `${origin}/#${secret}`,
		close(): Promise<void> {
			if (closePromise) return closePromise;
			let resolveClose!: () => void;
			closePromise = new Promise<void>((resolve) => {
				resolveClose = resolve;
			});
			if (updateTimer) clearTimeout(updateTimer);
			updateTimer = undefined;
			if (heartbeatTimer) clearInterval(heartbeatTimer);
			heartbeatTimer = undefined;
			const stop = unsubscribe;
			unsubscribe = undefined;
			try {
				stop?.();
			} catch {
				/* shutdown remains best-effort and idempotent */
			}
			for (const client of [...clients]) client.destroy();
			clients.clear();
			for (const socket of [...sockets]) socket.destroy();
			sockets.clear();
			server.close(() => resolveClose());
			return closePromise;
		},
	};
}
