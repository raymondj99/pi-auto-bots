import { randomUUID } from "node:crypto";
import { CoordinationPolicyError } from "./policy.ts";
import type {
	CoordinationProjection,
	CoordinationSeverity,
	DeliveryRecord,
	SignalStateRecord,
} from "./protocol.ts";

export function queueDelivery(
	targetRoleId: string,
	revision: number,
	refs: { signalId?: string; channelId?: string } = {},
	id: string = randomUUID(),
): DeliveryRecord {
	return {
		id,
		targetRoleId,
		revision,
		state: "queued",
		queuedAt: Date.now(),
		wakeCount: 0,
		...refs,
	};
}

export function markDelivered(delivery: DeliveryRecord): DeliveryRecord {
	return delivery.state === "queued"
		? { ...delivery, state: "delivered", deliveredAt: Date.now() }
		: delivery;
}
export function markAcknowledged(delivery: DeliveryRecord): DeliveryRecord {
	if (delivery.state === "queued")
		throw new CoordinationPolicyError(
			"INVALID_TRANSITION",
			"Queued delivery has not been confirmed injected.",
		);
	return delivery.state === "delivered"
		? { ...delivery, state: "acknowledged", acknowledgedAt: Date.now() }
		: delivery;
}
export function markResolved(delivery: DeliveryRecord): DeliveryRecord {
	if (delivery.state === "queued")
		throw new CoordinationPolicyError(
			"INVALID_TRANSITION",
			"Queued delivery has not been confirmed injected.",
		);
	return delivery.state === "resolved"
		? delivery
		: { ...delivery, state: "resolved", resolvedAt: Date.now() };
}

export function shouldWake(
	delivery: DeliveryRecord,
	severity: CoordinationSeverity,
	interactive = false,
): boolean {
	if (interactive) return false;
	if (delivery.wakeCount > 0) return false;
	return severity === "critical" || severity === "blocking" || severity === "action";
}

export function recordWake(delivery: DeliveryRecord): DeliveryRecord {
	return { ...delivery, wakeCount: delivery.wakeCount + 1 };
}

export function formatSignalDelivery(signal: SignalStateRecord, targetRoleId: string): string {
	const lines = [
		`Coordination signal ${signal.id}@${signal.revision}: ${signal.kind}/${signal.severity}; state=${signal.state}.`,
	];
	if (signal.supersedes)
		lines.push(`Supersedes signal ${signal.supersedes}; use this successor's current state.`);
	if (signal.state === "open" && signal.requiresAck.includes(targetRoleId))
		lines.push(
			`Explicit acknowledgement required: subagent_signal acknowledge, signalId=${signal.id}, expectedRevision=${signal.revision}. Query current state on revision conflict.`,
		);
	for (const ref of signal.artifactRefs.slice(0, 8))
		lines.push(`Artifact: ${ref.artifactId}@${ref.version} ${ref.digest}`);
	if (signal.artifactRefs.length > 8)
		lines.push("Additional artifact refs omitted; query scoped state with subagent_team.");
	lines.push(signal.details || signal.summary);
	const text = lines.join("\n").replace(/[\u0000-\u0009\u000b-\u001f\u007f-\u009f]/g, " ");
	return text.length <= 8000
		? text
		: `${text.slice(0, 7880)}\n[Details omitted; query this signal with subagent_team.]`;
}

export interface CoordinationDigest {
	cursor: number;
	lines: string[];
	eventIds: string[];
	truncated: number;
}
export function buildDigest(
	projection: CoordinationProjection,
	roleId: string,
	sinceSeq = 0,
	maxLines = 20,
): CoordinationDigest {
	if (!Number.isSafeInteger(sinceSeq) || sinceSeq < 0 || sinceSeq > projection.seq)
		throw new Error("Invalid digest cursor.");
	if (!Number.isSafeInteger(maxLines) || maxLines < 1 || maxLines > 100)
		throw new Error("Invalid digest page size.");
	const lines: string[] = [];
	const eventIds: string[] = [];
	let truncated = 0;
	let bytes = 0;
	let cursor = sinceSeq;
	for (const event of projection.audit.filter((entry) => entry.seq > sinceSeq)) {
		const signal = event.payload as Partial<SignalStateRecord>;
		const targets = signal.targets ?? [];
		const targeted = targets.some(
			(target) =>
				target.roleId === roleId ||
				(target.taskId && projection.tasks[target.taskId]?.owner === roleId) ||
				(target.channelId &&
					projection.channels[target.channelId]?.members.some(
						(member) => member.roleId === roleId,
					)),
		);
		if (!targeted && event.actor.roleId !== roleId) {
			if (!truncated) cursor = event.seq;
			continue;
		}
		const identity = signal.id
			? ` ${signal.id}${signal.revision ? `@${signal.revision}` : ""}`
			: "";
		const line = `${event.seq} ${event.type}${identity}: ${signal.summary ?? event.type}`
			.replace(/[\u0000-\u001f\u007f-\u009f]/g, " ")
			.replace(/\s+/g, " ")
			.slice(0, 240);
		const lineBytes = Buffer.byteLength(line);
		if (!truncated && lines.length < maxLines && bytes + lineBytes <= 20 * 1024) {
			lines.push(line);
			bytes += lineBytes;
			eventIds.push(event.eventId);
			cursor = event.seq;
		} else truncated++;
	}
	return { cursor: truncated ? cursor : projection.seq, lines, eventIds, truncated };
}
