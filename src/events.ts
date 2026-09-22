/**
 * The coordination event vocabulary shared by the transport, the chat archive
 * and the dashboard.
 *
 * Events are the display and audit model: the transport delivers them to live
 * members, and `coordination/chat.ts` projects ledger envelopes into the same
 * shape so the chat UI renders coordinator and peer traffic identically.
 */

export interface CoordinationChannel {
	id: string;
	name: string;
	members: string[];
	status: "open" | "closed";
}

export interface CoordinationTask {
	id: string;
	title: string;
	context: string;
	nextSteps: string;
	owner: string;
	createdBy: string;
	status: "assigned" | "completed";
	revision: number;
}

export interface CoordinationEvent {
	id: string;
	kind: string;
	from: string;
	to: string;
	text: string;
	task?: CoordinationTask;
	channel?: CoordinationChannel;
	/** Recipient IDs for which a channel broadcast was accepted. */
	recipients?: string[];
	deliveryId?: string;
	priority?: "critical" | "blocking" | "action" | "info";
}

export function cloneChannel(channel: CoordinationChannel): CoordinationChannel {
	return { ...channel, members: [...channel.members] };
}

export function cloneEvent(event: CoordinationEvent): CoordinationEvent {
	return {
		...event,
		...(event.task ? { task: { ...event.task } } : {}),
		...(event.channel ? { channel: cloneChannel(event.channel) } : {}),
		...(event.recipients ? { recipients: [...event.recipients] } : {}),
	};
}
