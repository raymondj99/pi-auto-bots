import type { CoordinationChatRecord } from "../chat/records.ts";
import type { CoordinationDashboardMember } from "../chat/server.ts";
import type {
	ChannelState,
	CoordinationEnvelope,
	CoordinationProjection,
	SignalStateRecord,
	TaskStateRecord,
} from "./protocol.ts";
import type { ReviewMessage, ReviewReadinessEvent } from "./review.ts";

const chatId = (roleId: string) => (roleId === "coordinator" ? "parent" : roleId);

/** Stable role IDs keep conversations together across run replacement/recovery. */
export function coordinationChatMembers(
	projection: CoordinationProjection,
	live: readonly CoordinationDashboardMember[],
	roleNames: ReadonlyMap<string, string> = new Map(),
): CoordinationDashboardMember[] {
	const mapped = new Set<string>();
	const roles = Object.values(projection.roles)
		.filter((role) => role.roleId !== "coordinator")
		.map((role) => {
			const member = live.find((candidate) => candidate.id === role.runId);
			if (member) mapped.add(member.id);
			return {
				...member,
				id: chatId(role.roleId),
				name: member?.name ?? roleNames.get(role.roleId) ?? role.roleId,
				role: role.roleId,
				status: member?.status ?? (role.active ? "running" : "archived"),
				backend: member?.backend ?? "pi",
			};
		});
	return [...roles, ...live.filter((member) => !mapped.has(member.id))];
}

/** Public conversation fields only: never copy credentials, paths or checkpoints. */
export function coordinationChatRecords(
	event: CoordinationEnvelope,
	projection: CoordinationProjection,
): CoordinationChatRecord[] {
	const from = chatId(event.actor.roleId);
	const name = (id: string) => (id === "parent" ? "Coordinator" : id).slice(0, 80);
	const record = (to: string, text: string, suffix = ""): CoordinationChatRecord => ({
		event: {
			id: `coordination:${event.eventId}${suffix}`,
			kind: "message",
			from,
			to,
			text: text.slice(0, 25000),
		},
		timestamp: event.timestamp,
		fromName: name(from),
		toName: name(to),
	});
	const channelRecord = (channel: ChannelState, text: string, kind = "channel_message") => {
		const result = record(channel.id, text, `:${channel.id}`);
		result.event.kind = kind;
		result.event.channel = {
			id: channel.id,
			name: channel.name,
			status: channel.status,
			members: channel.members.map((member) => chatId(member.roleId)),
		};
		result.event.recipients = result.event.channel.members.filter((id: string) => id !== from);
		result.toName = channel.name.slice(0, 80);
		return result;
	};
	if (["channel.created", "channel.membership_changed", "channel.closed"].includes(event.type)) {
		const payload = event.payload as ChannelState;
		const channel = { ...projection.channels[payload.id], ...payload };
		if (!channel.members) return [];
		return [
			channelRecord(
				channel,
				`${event.type}: #${channel.name}${channel.purpose ? ` — ${channel.purpose}` : ""}`,
				event.type === "channel.created"
					? "channel_create"
					: event.type === "channel.closed"
						? "channel_close"
						: "channel_message",
			),
		];
	}
	if (event.type === "channel.message") {
		const payload = event.payload as { id: string; summary: string };
		const channel = projection.channels[payload.id];
		return channel ? [channelRecord(channel, payload.summary)] : [];
	}
	if (event.type === "review.readiness")
		return coordinationChatRecords(
			{ ...event, type: "signal.emitted", payload: (event.payload as ReviewReadinessEvent).signal },
			projection,
		);
	if (event.type === "task.transitioned" && (event.payload as any).reviewMessage) {
		const notice = (event.payload as { reviewMessage: ReviewMessage }).reviewMessage;
		const channel = projection.channels[notice.id];
		return channel ? [channelRecord(channel, notice.summary)] : [];
	}
	if (event.type.startsWith("signal.")) {
		const signal = event.payload as SignalStateRecord;
		const text = `${signal.kind}/${signal.severity} ${signal.state} (${signal.id}@${signal.revision}): ${signal.details ?? signal.summary}`;
		const channels = signal.targets.flatMap((target) =>
			target.channelId && projection.channels[target.channelId]
				? [projection.channels[target.channelId]]
				: [],
		);
		const records = channels.map((channel) => channelRecord(channel, text));
		const recipients = new Set(
			signal.targets.flatMap((target) => {
				const role =
					target.roleId ?? (target.taskId ? projection.tasks[target.taskId]?.owner : undefined);
				return role ? [role] : [];
			}),
		);
		for (const roleId of recipients) {
			const to = chatId(roleId);
			if (
				to !== from &&
				!channels.some((channel) => channel.members.some((member) => member.roleId === roleId))
			)
				records.push(record(to, text, `:${to}`));
		}
		return records;
	}
	if (event.type.startsWith("task.")) {
		const task = event.payload as TaskStateRecord;
		const to = chatId(task.owner);
		// Self-owned coordinator housekeeping is operational state, not a conversation.
		const peer = to === from ? "parent" : to;
		if (peer === from) return [];
		return [
			record(peer, `${event.type}: ${task.title} (${task.id}@${task.revision}, ${task.state})`),
		];
	}
	return [];
}
