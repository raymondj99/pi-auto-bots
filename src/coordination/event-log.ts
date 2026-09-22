import { createHash } from "node:crypto";
import {
	validCheckpointProjection,
	validEventEnvelope,
	validEventPayload,
} from "./event-validation.ts";
import { redactProjection } from "./policy.ts";
import { makeEnvelope, reduceCoordinationEvent, restoreOffline } from "./projection.ts";
import type {
	CoordinationActor,
	CoordinationEnvelope,
	CoordinationEventType,
	CoordinationProjection,
	RoleState,
	StableErrorCode,
} from "./protocol.ts";
import { stableStringify } from "./protocol.ts";

export interface AppendResult<T = unknown> {
	event: CoordinationEnvelope<T>;
	projection: CoordinationProjection;
	replayed?: boolean;
}
export interface PersistedEntrySink {
	appendEntry?(customType: string, data: unknown): void;
}

function appendResult<T>(
	event: CoordinationEnvelope<T>,
	projection: CoordinationProjection,
	replayed = false,
): AppendResult<T> {
	return {
		event: structuredClone(event),
		get projection() {
			return structuredClone(projection);
		},
		...(replayed ? { replayed: true } : {}),
	};
}

function requestPayload(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(requestPayload);
	if (!value || typeof value !== "object") return value;
	const ignored = new Set(["at", "queuedAt", "deliveredAt", "acknowledgedAt", "resolvedAt"]);
	return Object.fromEntries(
		Object.entries(value as Record<string, unknown>)
			.filter(([key]) => !ignored.has(key))
			.map(([key, child]) => [key, requestPayload(child)]),
	);
}
function digest(type: string, payload: unknown, actor: CoordinationActor, refs: unknown): string {
	return createHash("sha256")
		.update(stableStringify({ type, payload: requestPayload(payload), actor, refs }))
		.digest("hex");
}
function scopeKey(actor: CoordinationActor, key: string): string {
	return JSON.stringify([actor.roleId, actor.runId, actor.generation, key]);
}
function eventRefs(event: CoordinationEnvelope) {
	return Object.fromEntries(
		["taskId", "channelId", "goalId", "causationId", "correlationId"]
			.filter((key) => Object.hasOwn(event, key))
			.map((key) => [key, event[key as keyof CoordinationEnvelope]]),
	);
}
function checkpointHash(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}
function validEnvelope(event: any, maxPayloadBytes = 128 * 1024): event is CoordinationEnvelope {
	return (
		validEventEnvelope(event) &&
		validEventPayload(event.type, event.payload) &&
		Buffer.byteLength(JSON.stringify(event)) <= maxPayloadBytes
	);
}

export class CoordinationEventLog {
	private projection: CoordinationProjection;
	private readonly events: CoordinationEnvelope[] = [];
	private sink?: PersistedEntrySink;
	private maxEvents: number;
	private maxPayloadBytes: number;
	private requestContext?: { key: string; digest: string };
	constructor(
		initial: CoordinationProjection,
		sink?: PersistedEntrySink,
		maxEvents = 1000,
		maxPayloadBytes = 128 * 1024,
	) {
		this.projection = structuredClone(initial);
		this.sink = sink;
		this.maxEvents = maxEvents;
		this.maxPayloadBytes = maxPayloadBytes;
	}
	snapshot(viewerRoleId?: string): CoordinationProjection {
		return viewerRoleId
			? redactProjection(this.projection, viewerRoleId)
			: structuredClone(this.projection);
	}
	/** Isolated working state for mutations that do not inspect event history. */
	stateSnapshot(): CoordinationProjection {
		return structuredClone({ ...this.projection, audit: [], idempotency: {} });
	}
	hasInbox(roleId: string): boolean {
		for (const delivery of Object.values(this.projection.deliveries)) {
			if (
				delivery.targetRoleId === roleId &&
				delivery.state === "queued" &&
				delivery.wakeCount === 0 &&
				this.projection.signals[delivery.signalId ?? ""]?.state === "open"
			)
				return true;
		}
		return false;
	}
	roleSnapshot(roleId: string): RoleState | undefined {
		if (!Object.hasOwn(this.projection.roles, roleId)) return undefined;
		const { checkpoint: _checkpoint, ...role } = this.projection.roles[roleId];
		return structuredClone(role);
	}
	allEvents(): CoordinationEnvelope[] {
		return structuredClone(this.events) as CoordinationEnvelope[];
	}
	request<T>(
		method: string,
		actor: CoordinationActor,
		params: Record<string, unknown>,
		apply: () => T,
	): T {
		const key = params.idempotencyKey;
		if (typeof key !== "string" || !key.trim() || key.length > 200)
			throw Object.assign(new Error("idempotencyKey is mandatory and bounded."), {
				code: "IDEMPOTENCY_CONFLICT",
			});
		const canonical = Object.fromEntries(
			Object.entries(params).filter(
				([field, value]) =>
					!["actor", "idempotencyKey", "projection"].includes(field) && value !== undefined,
			),
		);
		const requestDigest = createHash("sha256")
			.update(stableStringify({ method, params: canonical }))
			.digest("hex");
		const existing = this.projection.idempotency[scopeKey(actor, key)];
		if (existing) {
			if (existing.digest !== requestDigest)
				throw Object.assign(new Error("Idempotency key was reused with different arguments."), {
					code: "IDEMPOTENCY_CONFLICT",
				});
			return appendResult(existing.result as CoordinationEnvelope, this.projection, true) as T;
		}
		if (this.requestContext) throw new Error("Nested coordination mutations are not supported.");
		this.requestContext = { key, digest: requestDigest };
		try {
			return apply();
		} finally {
			this.requestContext = undefined;
		}
	}
	append<T>(
		type: CoordinationEventType,
		actor: CoordinationActor,
		payload: T,
		idempotencyKey: string,
		refs: Partial<
			Pick<
				CoordinationEnvelope,
				"taskId" | "channelId" | "goalId" | "causationId" | "correlationId"
			>
		> = {},
	): AppendResult<T> {
		if (!idempotencyKey || idempotencyKey.length > 200)
			throw Object.assign(new Error("idempotencyKey is mandatory and bounded."), {
				code: "IDEMPOTENCY_CONFLICT" satisfies StableErrorCode,
			});
		const requestDigest =
			this.requestContext?.key === idempotencyKey
				? this.requestContext.digest
				: digest(type, payload, actor, refs);
		const scopedKey = scopeKey(actor, idempotencyKey);
		const existing = this.projection.idempotency[scopedKey];
		if (existing) {
			if (existing.digest !== requestDigest)
				throw Object.assign(new Error("Idempotency key was reused with different arguments."), {
					code: "IDEMPOTENCY_CONFLICT" satisfies StableErrorCode,
					current: structuredClone(existing.result),
				});
			return appendResult(existing.result as CoordinationEnvelope<T>, this.projection, true);
		}
		const event = makeEnvelope(this.projection, type, actor, payload, idempotencyKey, refs);
		if (this.requestContext?.key === idempotencyKey) event.requestDigest = requestDigest;
		if (!validEnvelope(event, this.maxPayloadBytes))
			throw Object.assign(new Error("Coordination event exceeds bounds or is invalid."), {
				code: "BOUNDS_EXCEEDED" satisfies StableErrorCode,
			});
		const next = reduceCoordinationEvent(this.projection, event, true);
		next.idempotency[scopedKey] = { digest: requestDigest, result: event };
		const idempotencyKeys = Object.keys(next.idempotency);
		if (idempotencyKeys.length > this.maxEvents)
			for (const key of idempotencyKeys.slice(0, idempotencyKeys.length - this.maxEvents))
				delete next.idempotency[key];
		this.sink?.appendEntry?.(
			"subagent-coordination-event",
			structuredClone({ sessionId: next.sessionId, branchId: next.branchId, event }),
		);
		this.projection = next;
		this.events.push(event);
		if (this.events.length > this.maxEvents)
			this.events.splice(0, this.events.length - this.maxEvents);
		return appendResult(event, this.projection);
	}
	checkpoint(idempotencyKey: string, actor: CoordinationActor): AppendResult {
		const compact = this.snapshot();
		compact.audit = [];
		compact.idempotency = {};
		for (const role of Object.values(compact.roles)) delete role.checkpoint;
		const serialized = JSON.stringify(compact);
		if (Buffer.byteLength(serialized) > 120_000)
			throw Object.assign(new Error("Checkpoint exceeds bounded payload."), {
				code: "BOUNDS_EXCEEDED" satisfies StableErrorCode,
			});
		return this.append(
			"checkpoint.saved",
			actor,
			{ roleId: actor.roleId, checkpoint: serialized, checksum: checkpointHash(serialized) },
			idempotencyKey,
		);
	}
	static restore(
		entries: Array<{ customType?: string; data?: any }>,
		sessionId: string,
		branchId: string,
	): CoordinationProjection {
		const events: CoordinationEnvelope[] = [];
		let lastCorruptionIndex = -1;
		const positions = new Map<CoordinationEnvelope, number>();
		for (const [index, entry] of entries.entries()) {
			// Callers must pass the active branch (SessionManager.getBranch), never a flat merged entry list.
			if (entry.customType !== "subagent-coordination-event" || entry.data?.sessionId !== sessionId)
				continue;
			if (validEnvelope(entry.data.event)) {
				events.push(entry.data.event);
				positions.set(entry.data.event, index);
			} else lastCorruptionIndex = index;
		}
		for (let index = 1; index < events.length; index++) {
			if (events[index].seq <= events[index - 1].seq)
				throw Object.assign(new Error("Coordination branch events are out of order."), {
					code: "CHECKPOINT_CORRUPT" satisfies StableErrorCode,
				});
		}
		let base: CoordinationProjection | undefined;
		let startSeq = 0;
		let checkpointIndex = -1;
		// Prefer the newest self-consistent checkpoint, but never accept its live identities.
		for (const event of events)
			if (event.type === "checkpoint.saved") {
				try {
					const parsed = JSON.parse((event.payload as any).checkpoint) as CoordinationProjection;
					if (
						(event.payload as any).checksum === checkpointHash((event.payload as any).checkpoint) &&
						validCheckpointProjection(parsed) &&
						parsed.sessionId === sessionId &&
						parsed.seq === event.seq - 1
					) {
						base = parsed;
						startSeq = event.seq - 1;
						checkpointIndex = positions.get(event)!;
					}
				} catch {
					/* try older valid checkpoint */
				}
			}
		if (lastCorruptionIndex > checkpointIndex)
			throw Object.assign(
				new Error(
					"Coordination checkpoint/event replay failed closed: invalid envelope after the latest valid checkpoint.",
				),
				{ code: "CHECKPOINT_CORRUPT" satisfies StableErrorCode },
			);
		try {
			let projection = base ? { ...base, branchId } : restoreOffline([], sessionId, branchId);
			// Rebuild the retry ledger from the retained branch events, including those
			// before the checkpoint. A checkpoint must not erase accepted request keys.
			projection.idempotency = {};
			for (const event of events) {
				if (event.seq > startSeq) projection = reduceCoordinationEvent(projection, event, true);
				projection.idempotency[scopeKey(event.actor, event.idempotencyKey)] = {
					digest:
						event.requestDigest ?? digest(event.type, event.payload, event.actor, eventRefs(event)),
					result: structuredClone(event),
				};
				const keys = Object.keys(projection.idempotency);
				if (keys.length > 1000) delete projection.idempotency[keys[0]];
			}
			projection.online = false;
			for (const role of Object.values(projection.roles)) {
				role.active = false;
				role.runId = undefined;
				role.unavailable = true;
			}
			// Permits and leases are restored for audit/accounting only; they cannot be consumed offline.
			return projection;
		} catch (error) {
			throw Object.assign(
				new Error(
					`Coordination checkpoint/event replay failed closed: ${(error as Error).message}`,
				),
				{ code: "CHECKPOINT_CORRUPT" satisfies StableErrorCode },
			);
		}
	}
}
