import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { deliverCoordination } from "../child-coordination.ts";
import type { TransportRequest } from "../transport.ts";
import type { CoordinationBroker } from "./broker.ts";
import { formatSignalDelivery } from "./delivery.ts";

/** The coordinator is local to Pi, not a member socket named by its run ID. */
export function installCoordinationDeliveryRouting(
	pi: ExtensionAPI,
	broker: CoordinationBroker,
	sendToRun: (request: TransportRequest) => Promise<unknown>,
	interactive = false,
): () => void {
	let active = true;
	const pending = new Set<string>();
	broker.setDeliverySink((delivery, signal) => {
		const target = broker.role(delivery.targetRoleId);
		if (!active || !target?.active || !target.runId || pending.has(delivery.id)) return;
		const request: TransportRequest = {
			action: "message",
			to: target.runId,
			deliveryId: delivery.id,
			priority: delivery.wakeCount > 0 && signal.state === "open" ? signal.severity : "info",
			text: formatSignalDelivery(signal, delivery.targetRoleId),
		};
		if (target.roleId === "coordinator") {
			// Queue acceptance is not a receipt. message_end below confirms injection.
			pending.add(delivery.id);
			try {
				deliverCoordination(
					pi,
					{
						id: delivery.id,
						from: "broker",
						to: "coordinator",
						kind: "signal",
						text: request.text!,
						deliveryId: delivery.id,
						priority: request.priority,
					},
					interactive,
				);
			} catch (error) {
				pending.delete(delivery.id);
				throw error;
			}
		} else {
			pending.add(delivery.id);
			void sendToRun(request)
				.catch(() => {
					/* Durable queue retries on reconnect. */
				})
				.finally(() => pending.delete(delivery.id));
		}
	});
	pi.on("message_end", (event) => {
		if (!active) return;
		const message = event.message as any;
		const deliveryIds: unknown[] =
			message?.customType === "subagent_coordination" && message.details?.to === "coordinator"
				? [message.details?.deliveryId]
				: message?.role === "toolResult" && message.toolName === "subagent_team" && !message.isError
					? (message.details?.digest?.deliveryIds ?? [])
					: [];
		const role = broker.role("coordinator");
		if (!role?.active || !role.runId) return;
		for (const deliveryId of new Set(deliveryIds))
			if (typeof deliveryId === "string") {
				pending.delete(deliveryId);
				broker.receipt({
					actor: { roleId: role.roleId, runId: role.runId, generation: role.generation },
					deliveryId,
					state: "delivered",
					idempotencyKey: `injected:${deliveryId}:${role.generation}`,
				});
			}
	});
	return () => {
		active = false;
		pending.clear();
	};
}
