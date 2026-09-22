import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CoordinationToolContext } from "./tools.ts";

/** Routine traffic rides useful tool results or user turns, never a polling turn. */
export function installCoordinationInbox(
	pi: ExtensionAPI,
	getContext: () => CoordinationToolContext,
	enabled: () => boolean,
) {
	const pending = new Set<string>();
	const take = async () => {
		if (!enabled()) return;
		const { broker, actor } = getContext();
		const inbox = await broker.inbox({ actor });
		const indexes = inbox.deliveryIds.flatMap((id, i) => (pending.has(id) ? [] : [i]));
		if (!indexes.length) return;
		const deliveryIds = indexes.map((i) => inbox.deliveryIds[i]);
		for (const id of deliveryIds) pending.add(id);
		return {
			text: `Coordination inbox (agent context):\n${indexes.map((i) => inbox.lines[i]).join("\n\n")}`,
			deliveryIds,
		};
	};
	pi.on("before_agent_start", async () => {
		try {
			const inbox = await take();
			if (inbox)
				return {
					message: {
						customType: "subagent_coordination_inbox",
						display: false,
						content: inbox.text,
						details: { coordinationInbox: { deliveryIds: inbox.deliveryIds } },
					},
				};
		} catch {
			/* Retained by the broker for the next natural turn. */
		}
	});
	pi.on("tool_result", async (event) => {
		// Preserve foreign result shapes and the normal 50 KiB text envelope.
		if (
			event.details != null &&
			(typeof event.details !== "object" || Array.isArray(event.details))
		)
			return;
		if (
			event.content.reduce(
				(bytes, item) => bytes + (item.type === "text" ? Buffer.byteLength(item.text) : 0),
				0,
			) >
			29 * 1024
		)
			return;
		try {
			const inbox = await take();
			if (inbox)
				return {
					content: [...event.content, { type: "text" as const, text: inbox.text }],
					details: {
						...(event.details && typeof event.details === "object" ? event.details : {}),
						coordinationInbox: { deliveryIds: inbox.deliveryIds },
					},
				};
		} catch {
			/* Transport failure must not replace a useful tool result. */
		}
	});
	pi.on("message_end", async (event) => {
		if (!enabled()) return;
		const message = event.message as any;
		if (message.role !== "toolResult" && message.customType !== "subagent_coordination_inbox")
			return;
		const ids = message.details?.coordinationInbox?.deliveryIds;
		if (!Array.isArray(ids)) return;
		const { broker, actor } = getContext();
		for (const id of ids)
			if (typeof id === "string" && pending.has(id)) {
				try {
					await broker.receipt({
						actor,
						deliveryId: id,
						state: "delivered",
						idempotencyKey: `inbox:${id}:${actor.generation}`,
					});
				} catch {
					/* Retry on the next natural result, never acknowledge unseen data. */
				}
				pending.delete(id);
			}
	});
	pi.on("agent_end", () => pending.clear());
	pi.on("session_shutdown", () => pending.clear());
}
