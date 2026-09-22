import type { CoordinationEvent, CoordinationTask } from "../events.ts";

export interface CoordinationChatRecord {
	event: CoordinationEvent;
	timestamp: number;
	fromName: string;
	toName: string;
	goalId?: string;
	goalTitle?: string;
}

export interface CoordinationChatSnapshot {
	records: readonly CoordinationChatRecord[];
	dropped: number;
}

export interface CoordinationChatThread {
	id: string;
	title: string;
	subtitle: string;
	records: readonly CoordinationChatRecord[];
	kind?: "agent" | "channel";
	preview?: string;
	latestTimestamp?: number;
	matchIds?: readonly string[];
	matchCount?: number;
}

const DEFAULT_MAX_RECORDS = 1_000;
const DEFAULT_MAX_BYTES = 2 * 1024 * 1024;
const EVENT_KINDS = new Set([
	"message",
	"assign",
	"handoff",
	"complete",
	"channel_create",
	"channel_message",
	"channel_close",
]);

function boundedString(value: unknown, max: number, allowEmpty = false): string | undefined {
	if (typeof value !== "string" || value.length > max || (!allowEmpty && !value.trim()))
		return undefined;
	return value;
}

function validatedStringArray(value: unknown, maxItems: number): string[] | undefined {
	if (!Array.isArray(value) || value.length > maxItems) return undefined;
	const result: string[] = [];
	for (const item of value) {
		const validated = boundedString(item, 200);
		if (!validated || result.includes(validated)) return undefined;
		result.push(validated);
	}
	return result;
}

function validatedTask(value: unknown): CoordinationTask | undefined {
	if (!value || typeof value !== "object") return undefined;
	const input = value as Record<string, unknown>;
	const id = boundedString(input.id, 200);
	const title = boundedString(input.title, 8_000);
	const context = boundedString(input.context, 8_000);
	const nextSteps = boundedString(input.nextSteps, 8_000);
	const owner = boundedString(input.owner, 200);
	const createdBy = boundedString(input.createdBy, 200);
	const status = input.status;
	const revision = input.revision;
	if (
		!id ||
		!title ||
		!context ||
		!nextSteps ||
		!owner ||
		!createdBy ||
		(status !== "assigned" && status !== "completed") ||
		!Number.isSafeInteger(revision) ||
		(revision as number) < 1
	)
		return undefined;
	return { id, title, context, nextSteps, owner, createdBy, status, revision: revision as number };
}

/** Validate untrusted restored data and copy only the public observation fields. */
export function validateCoordinationChatRecord(value: unknown): CoordinationChatRecord | undefined {
	if (!value || typeof value !== "object") return undefined;
	const input = value as Record<string, unknown>;
	if (!input.event || typeof input.event !== "object") return undefined;
	const source = input.event as Record<string, unknown>;
	const id = boundedString(source.id, 200);
	const kind = boundedString(source.kind, 20);
	const from = boundedString(source.from, 200);
	const to = boundedString(source.to, 200);
	const text = boundedString(source.text, 25_000);
	const fromName = boundedString(input.fromName, 80);
	const toName = boundedString(input.toName, 80);
	const timestamp = input.timestamp;
	if (
		!id ||
		!kind ||
		!EVENT_KINDS.has(kind) ||
		!from ||
		!to ||
		!text ||
		!fromName ||
		!toName ||
		!Number.isSafeInteger(timestamp) ||
		(timestamp as number) < 0
	)
		return undefined;

	let task: CoordinationTask | undefined;
	if (kind === "message") {
		if (from === to || source.task !== undefined || source.channel !== undefined) return undefined;
	} else if (kind === "assign" || kind === "handoff" || kind === "complete") {
		task = validatedTask(source.task);
		if (!task || source.channel !== undefined) return undefined;
	}

	let channel: CoordinationEvent["channel"];
	if (kind.startsWith("channel_")) {
		if (!source.channel || typeof source.channel !== "object" || source.task !== undefined)
			return undefined;
		const inputChannel = source.channel as Record<string, unknown>;
		const channelId = boundedString(inputChannel.id, 200);
		const name = boundedString(inputChannel.name, 200);
		const members = validatedStringArray(inputChannel.members, 100);
		const status = inputChannel.status;
		if (!channelId || !name || !members || (status !== "open" && status !== "closed"))
			return undefined;
		channel = { id: channelId, name, members, status };
	}
	const recipients =
		source.recipients === undefined ? undefined : validatedStringArray(source.recipients, 100);
	if (source.recipients !== undefined && !recipients) return undefined;
	const goalId = input.goalId === undefined ? undefined : boundedString(input.goalId, 300);
	const goalTitle =
		input.goalTitle === undefined ? undefined : boundedString(input.goalTitle, 8_000);
	if ((input.goalId !== undefined && !goalId) || (input.goalTitle !== undefined && !goalTitle))
		return undefined;

	const event: CoordinationEvent = { id, kind, from, to, text };
	if (task) event.task = task;
	if (channel) event.channel = channel;
	if (recipients) event.recipients = recipients;
	return {
		event,
		timestamp: timestamp as number,
		fromName,
		toName,
		...(goalId ? { goalId } : {}),
		...(goalTitle ? { goalTitle } : {}),
	};
}

function cloneRecord(record: CoordinationChatRecord): CoordinationChatRecord {
	return {
		event: {
			...record.event,
			...(record.event.task ? { task: { ...record.event.task } } : {}),
			...(record.event.channel
				? { channel: { ...record.event.channel, members: [...record.event.channel.members] } }
				: {}),
			...(record.event.recipients ? { recipients: [...record.event.recipients] } : {}),
		},
		timestamp: record.timestamp,
		fromName: record.fromName,
		toName: record.toName,
		...(record.goalId ? { goalId: record.goalId } : {}),
		...(record.goalTitle ? { goalTitle: record.goalTitle } : {}),
	};
}

function serializedBytes(record: CoordinationChatRecord): number {
	return Buffer.byteLength(JSON.stringify(record), "utf8");
}

function limit(value: number | undefined, fallback: number): number {
	return value === undefined || !Number.isFinite(value) ? fallback : Math.max(0, Math.floor(value));
}

export class CoordinationChatStore {
	private readonly maxRecords: number;
	private readonly maxBytes: number;
	private records: CoordinationChatRecord[] = [];
	private ids = new Set<string>();
	private bytes = 0;
	private dropped = 0;
	private listeners = new Set<() => void>();

	constructor(options: { maxRecords?: number; maxBytes?: number } = {}) {
		this.maxRecords = limit(options.maxRecords, DEFAULT_MAX_RECORDS);
		this.maxBytes = limit(options.maxBytes, DEFAULT_MAX_BYTES);
	}

	add(record: CoordinationChatRecord): boolean {
		const copy = validateCoordinationChatRecord(record);
		if (!copy || this.ids.has(copy.event.id)) return false;
		const bytes = serializedBytes(copy);
		this.records.push(copy);
		this.ids.add(copy.event.id);
		this.bytes += bytes;
		while (
			this.records.length > 0 &&
			(this.records.length > this.maxRecords || this.bytes > this.maxBytes)
		) {
			const removed = this.records.shift()!;
			this.ids.delete(removed.event.id);
			this.bytes -= serializedBytes(removed);
			this.dropped++;
		}
		this.emit();
		return true;
	}

	snapshot(): CoordinationChatSnapshot {
		return { records: this.records.map(cloneRecord), dropped: this.dropped };
	}

	subscribe(listener: () => void): () => void {
		this.listeners.add(listener);
		return () => {
			this.listeners.delete(listener);
		};
	}

	clear(): void {
		if (this.records.length === 0 && this.dropped === 0) return;
		this.records = [];
		this.ids.clear();
		this.bytes = 0;
		this.dropped = 0;
		this.emit();
	}

	private emit(): void {
		for (const listener of [...this.listeners]) {
			try {
				listener();
			} catch {
				/* Observers cannot disrupt coordination. */
			}
		}
	}
}

function searchable(record: CoordinationChatRecord): string {
	const { event } = record;
	const values: unknown[] = [
		event.id,
		event.kind,
		event.from,
		event.to,
		event.text,
		record.fromName,
		record.toName,
		record.goalId,
		record.goalTitle,
	];
	if (event.task)
		values.push(
			event.task.id,
			event.task.title,
			event.task.context,
			event.task.nextSteps,
			event.task.owner,
			event.task.createdBy,
			event.task.status,
			event.task.revision,
		);
	if (event.channel)
		values.push(
			event.channel.id,
			event.channel.name,
			event.channel.status,
			...event.channel.members,
			...(event.recipients ?? []),
		);
	return values.join(" ").toLocaleLowerCase();
}

export function buildCoordinationChatThreads(
	records: readonly CoordinationChatRecord[],
	query = "",
): CoordinationChatThread[] {
	const valid = records
		.map(validateCoordinationChatRecord)
		.filter((record): record is CoordinationChatRecord => !!record);
	const groups = new Map<
		string,
		{ thread: CoordinationChatThread; latest: number; index: number }
	>();
	const names = new Map<string, string>();
	for (const record of valid) {
		names.set(record.event.from, record.fromName);
		if (!record.event.channel) names.set(record.event.to, record.toName);
	}

	const add = (
		id: string,
		kind: "agent" | "channel",
		title: string,
		subtitle: string,
		record: CoordinationChatRecord,
		index: number,
	) => {
		const existing = groups.get(id);
		if (existing) {
			(existing.thread.records as CoordinationChatRecord[]).push(record);
			if (record.timestamp >= existing.latest) {
				existing.latest = record.timestamp;
				existing.index = index;
				existing.thread.title = title;
				existing.thread.subtitle = subtitle;
			}
		} else {
			groups.set(id, {
				thread: { id, kind, title, subtitle, records: [record] },
				latest: record.timestamp,
				index,
			});
		}
	};

	for (let index = 0; index < valid.length; index++) {
		const record = valid[index];
		const { event } = record;
		const participants = new Set<string>();
		participants.add(event.from);
		if (event.channel) {
			const channelParticipants =
				event.kind === "channel_message" ? (event.recipients ?? []) : event.channel.members;
			for (const participant of channelParticipants) participants.add(participant);
			add(
				`channel:${event.channel.id}`,
				"channel",
				event.channel.name,
				`${event.channel.members.length} members · ${event.channel.status}`,
				record,
				index,
			);
		} else {
			participants.add(event.to);
		}
		for (const participant of participants) {
			add(
				`agent:${participant}`,
				"agent",
				names.get(participant) ?? participant,
				participant,
				record,
				index,
			);
		}
	}

	const needle = query.trim().toLocaleLowerCase();
	return [...groups.values()]
		.sort(
			(a, b) => b.latest - a.latest || b.index - a.index || a.thread.id.localeCompare(b.thread.id),
		)
		.map(({ thread }) => {
			const last = thread.records.at(-1);
			const labelMatches =
				!!needle &&
				`${thread.id} ${thread.title} ${thread.subtitle}`.toLocaleLowerCase().includes(needle);
			const matchIds = needle
				? thread.records
						.filter((record) => labelMatches || searchable(record).includes(needle))
						.map((record) => record.event.id)
				: [];
			return {
				...thread,
				preview: last?.event.task?.context ?? last?.event.text ?? "",
				latestTimestamp: last?.timestamp,
				matchIds,
				matchCount: matchIds.length,
			};
		})
		.filter((thread) => !needle || !!thread.matchCount);
}
