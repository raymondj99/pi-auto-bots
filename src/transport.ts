/**
 * Authenticated local socket transport between one coordinator session and its
 * spawned child runs.
 *
 * The transport is deliberately thin: it authenticates members, delivers
 * coordination events, and tunnels coordination requests to the coordinator's
 * broker. Workflow state — tasks, channels, signals, artifacts — lives in the
 * event-sourced broker under `coordination/`, never here.
 */

import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CoordinationChatRecord } from "./chat/records.ts";
import { publicCoordinationError, publicMutationResult } from "./coordination/results.ts";
import { type CoordinationEvent, cloneEvent } from "./events.ts";

export interface TransportRequest {
	action: string;
	to?: string;
	text?: string;
	deliveryId?: string;
	priority?: "critical" | "blocking" | "action" | "info";
	/** Coordination tunnel: broker method and its parameters. */
	method?: string;
	params?: Record<string, unknown>;
}

export interface TransportCredentials {
	socket: string;
	id: string;
	token: string;
	sessionFile: string;
}

interface Member {
	id: string;
	name: string;
	interactive: boolean;
	token: string;
	sessionFile: string;
	socket?: Socket;
}

const MAX_FRAME = 128 * 1024;

function text(value: unknown, field: string): string {
	if (typeof value !== "string" || !value.trim() || value.length > 8000) {
		throw new Error(`${field} must be non-empty text (maximum 8000 characters).`);
	}
	return value;
}

function send(socket: Socket, data: unknown) {
	if (socket.destroyed || !socket.writable) {
		throw Object.assign(
			new Error(
				"Coordination transport disconnected; reconnect before sending another request. Prior mutation outcomes may be unknown.",
			),
			{ code: "TRANSPORT_DISCONNECTED" },
		);
	}
	const frame = `${JSON.stringify(data)}\n`;
	const bytes = Buffer.byteLength(frame);
	if (bytes > MAX_FRAME)
		throw Object.assign(new Error("Coordination payload exceeds 128KB."), {
			code: "BOUNDS_EXCEEDED",
		});
	if (socket.writableLength + bytes > MAX_FRAME)
		throw Object.assign(new Error("Coordination transport backpressure; request was not queued."), {
			code: "DELIVERY_BACKPRESSURE",
		});
	socket.write(frame);
}

function frames(socket: Socket, receive: (value: any) => void) {
	let buffer = "";
	socket.setEncoding("utf8");
	socket.on("data", (chunk: string) => {
		buffer += chunk;
		for (;;) {
			const newline = buffer.indexOf("\n");
			if (newline < 0) break;
			const line = buffer.slice(0, newline);
			buffer = buffer.slice(newline + 1);
			if (Buffer.byteLength(line) > MAX_FRAME) {
				socket.destroy();
				return;
			}
			try {
				receive(JSON.parse(line));
			} catch {
				socket.destroy();
				return;
			}
		}
		if (Buffer.byteLength(buffer) > MAX_FRAME) socket.destroy();
	});
}

/** One live coordinator session owns the transport and serializes all requests. */
export class TransportBroker {
	private members = new Map<string, Member>();
	private connections = new Set<Socket>();
	private server?: Server;
	private directory?: string;
	private closed = false;
	private receive: (event: CoordinationEvent) => void;
	private observe?: (record: CoordinationChatRecord) => void;
	private deliveryReceipt?: (from: string, deliveryId: string) => unknown;
	private coordinationRequest?: (
		from: string,
		method: string,
		params: Record<string, unknown>,
	) => unknown;
	private memberAvailable?: (id: string) => void;

	constructor(
		receive: (event: CoordinationEvent) => void,
		observe?: (record: CoordinationChatRecord) => void,
		deliveryReceipt?: (from: string, deliveryId: string) => unknown,
		coordinationRequest?: (
			from: string,
			method: string,
			params: Record<string, unknown>,
		) => unknown,
		memberAvailable?: (id: string) => void,
	) {
		this.receive = receive;
		this.observe = observe;
		this.deliveryReceipt = deliveryReceipt;
		this.coordinationRequest = coordinationRequest;
		this.memberAvailable = memberAvailable;
	}

	async start(): Promise<void> {
		if (this.closed) throw new Error("Coordination session closed.");
		this.directory = mkdtempSync(join(tmpdir(), "pi-coordination-"));
		this.server = createServer((socket) => {
			this.connections.add(socket);
			let member: Member | undefined;
			const timeout = setTimeout(() => socket.destroy(), 5000);
			socket.on("error", () => {});
			socket.on("close", () => {
				clearTimeout(timeout);
				this.connections.delete(socket);
				if (member?.socket === socket) member.socket = undefined;
			});
			frames(socket, (frame) => {
				try {
					if (!member) {
						const candidate = this.members.get(frame.id);
						if (
							frame.action !== "hello" ||
							!candidate?.token ||
							candidate.token !== frame.token ||
							candidate.sessionFile !== frame.sessionFile ||
							candidate.socket
						) {
							throw new Error("Invalid, duplicate, or out-of-session coordination identity.");
						}
						member = candidate;
						member.socket = socket;
						clearTimeout(timeout);
						send(socket, { requestId: frame.requestId, result: { self: member.id } });
						this.memberAvailable?.(member.id);
						return;
					}
					const result = this.request(member.id, frame.request);
					send(socket, { requestId: frame.requestId, result });
				} catch (error) {
					send(socket, { requestId: frame?.requestId, ...publicCoordinationError(error) });
					if (!member) socket.end();
				}
			});
		});
		await new Promise<void>((resolve, reject) => {
			this.server!.once("error", reject);
			this.server!.listen(join(this.directory!, "bus"), resolve);
		});
	}

	add(id: string, name: string, sessionFile: string, interactive: boolean): TransportCredentials {
		if (this.closed || !this.directory) throw new Error("Coordination session unavailable.");
		if (id === "parent" || this.members.has(id))
			throw new Error("Duplicate coordination identity.");
		if (this.members.size >= 100)
			throw new Error("Coordination member limit reached (100 per session).");
		const token = randomUUID();
		this.members.set(id, { id, name, sessionFile, interactive, token });
		return { socket: join(this.directory, "bus"), id, token, sessionFile };
	}

	remove(id: string) {
		const member = this.members.get(id);
		member?.socket?.destroy();
		if (member) {
			member.socket = undefined;
			member.token = "";
		}
	}

	private available(id: string): boolean {
		const socket = this.members.get(id)?.socket;
		return id === "parent" || (!!socket && !socket.destroyed && socket.writable);
	}

	/** Whether a member currently holds a writable authenticated connection. */
	isAvailable(id: string): boolean {
		return !this.closed && this.available(id);
	}

	private deliver(event: CoordinationEvent) {
		if (!this.available(event.to))
			throw new Error(
				`Recipient "${event.to}" unavailable. Use an exact live run ID; completed runs cannot receive messages.`,
			);
		if (event.to === "parent") this.receive(cloneEvent(event));
		else send(this.members.get(event.to)!.socket!, { event });
	}

	private displayName(id: string): string {
		if (id === "parent") return "Coordinator";
		const name = this.members.get(id)?.name.slice(0, 80);
		return name?.trim() ? name : id;
	}

	private notifyObserver(event: CoordinationEvent, fromName: string, toName: string) {
		if (!this.observe) return;
		const observedEvent = cloneEvent(event);
		if (observedEvent.task) Object.freeze(observedEvent.task);
		if (observedEvent.channel) {
			Object.freeze(observedEvent.channel.members);
			Object.freeze(observedEvent.channel);
		}
		if (observedEvent.recipients) Object.freeze(observedEvent.recipients);
		Object.freeze(observedEvent);
		const record = Object.freeze({ event: observedEvent, timestamp: Date.now(), fromName, toName });
		try {
			this.observe(record);
		} catch {
			/* Observation must never affect accepted work. */
		}
	}

	request(from: string, request: TransportRequest): unknown {
		if (this.closed) throw new Error("Coordination session closed.");
		if (from !== "parent" && !this.available(from)) throw new Error("Sender unavailable.");
		if (!request || typeof request !== "object") throw new Error("Invalid coordination request.");

		if (request.action === "coordination") {
			if (from === "parent") throw new Error("Coordinator requests use the local broker directly.");
			if (
				!this.coordinationRequest ||
				typeof request.method !== "string" ||
				!request.params ||
				typeof request.params !== "object"
			)
				throw new Error("Invalid coordination request.");
			return publicMutationResult(this.coordinationRequest(from, request.method, request.params));
		}

		if (request.action === "delivery_receipt") {
			const deliveryId = text(request.deliveryId, "deliveryId");
			if (from === "parent")
				throw new Error(
					"Child-confirmed delivery receipts must come from an authenticated child transport.",
				);
			// A receipt must never serialize the append result's full projection (including
			// private state and retry ledger). Parallel receipts otherwise flood the socket.
			return publicMutationResult(this.deliveryReceipt?.(from, deliveryId) ?? { delivered: false });
		}

		if (request.action === "message") {
			const event: CoordinationEvent = {
				id: randomUUID(),
				kind: "message",
				from,
				to: request.to ?? "",
				text: text(request.text, "text"),
				...(request.deliveryId ? { deliveryId: request.deliveryId } : {}),
				...(from === "parent" && request.priority ? { priority: request.priority } : {}),
			};
			if (event.to === from) throw new Error("Cannot message yourself.");
			const fromName = this.displayName(event.from);
			const toName = this.displayName(event.to);
			this.deliver(event);
			this.notifyObserver(event, fromName, toName);
			return { queued: true, eventId: event.id, to: event.to };
		}

		throw new Error("Unknown coordination action.");
	}

	close() {
		if (this.closed) return;
		this.closed = true;
		for (const socket of this.connections) socket.destroy();
		this.connections.clear();
		this.server?.close();
		if (this.directory) rmSync(this.directory, { recursive: true, force: true });
		this.members.clear();
	}
}

export class TransportClient {
	private socket: Socket;
	private pending = new Map<
		string,
		{
			resolve: (value: any) => void;
			reject: (error: Error) => void;
			timer: ReturnType<typeof setTimeout>;
		}
	>();
	readonly ready: Promise<unknown>;

	constructor(credentials: TransportCredentials, receive: (event: CoordinationEvent) => void) {
		this.socket = createConnection(credentials.socket);
		this.socket.on("error", () => {});
		this.socket.on("close", () => {
			for (const p of this.pending.values()) {
				clearTimeout(p.timer);
				p.reject(
					new Error(
						"Coordination disconnected; outcome may be unknown. Do not blindly retry mutations.",
					),
				);
			}
			this.pending.clear();
		});
		frames(this.socket, (frame) => {
			if (frame.event) {
				receive(frame.event);
				return;
			}
			const pending = this.pending.get(frame.requestId);
			if (!pending) return;
			this.pending.delete(frame.requestId);
			clearTimeout(pending.timer);
			if (frame.error)
				pending.reject(
					Object.assign(new Error(frame.error), { code: frame.code, current: frame.current }),
				);
			else pending.resolve(frame.result);
		});
		this.ready = this.exchange({ action: "hello", ...credentials });
	}

	private exchange(payload: object): Promise<any> {
		return new Promise((resolve, reject) => {
			const requestId = randomUUID();
			const timer = setTimeout(() => {
				this.pending.delete(requestId);
				reject(
					new Error("Coordination timeout; outcome unknown. Inspect task state before retrying."),
				);
			}, 5000);
			this.pending.set(requestId, { resolve, reject, timer });
			try {
				send(this.socket, { ...payload, requestId });
			} catch (error) {
				clearTimeout(timer);
				this.pending.delete(requestId);
				reject(error);
			}
		});
	}

	async request(request: TransportRequest): Promise<any> {
		await this.ready;
		return this.exchange({ request });
	}

	close() {
		this.socket.destroy();
	}
}
