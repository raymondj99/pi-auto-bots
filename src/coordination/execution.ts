import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CoordinationToolContext } from "./tools.ts";

/** A permit is selected explicitly, then consumed by the actual tool-call hook.
 * This avoids adding unsupported fields to built-in tool argument schemas.
 */
export function installProtectedExecution(
	pi: ExtensionAPI,
	getContext: () => CoordinationToolContext,
	operations: () => readonly string[],
	enabled: () => boolean,
): (
	permitId: string,
	operation: string,
) => Promise<{ armed: true; permitId: string; operation: string }> {
	const armed = new Map<string, string>();
	const attempts = new Map<string, string>();
	pi.on("tool_call", async (event) => {
		if (!enabled()) return;
		const operation =
			event.toolName === "bash" || event.toolName === "powershell" ? "shell" : event.toolName;
		if (!operations().includes(operation)) return;
		const selected = [...armed].find(([, candidate]) => candidate === operation);
		if (!selected)
			return {
				block: true,
				reason: `Arm a single-use ${operation} permit with subagent_permit before this protected call.`,
			};
		const [permitId] = selected;
		armed.delete(permitId);
		try {
			const { broker, actor } = getContext();
			const result = await broker.dispatchPermit({
				actor,
				permitId,
				operation,
				idempotencyKey: `tool-dispatch:${event.toolCallId}`,
			});
			if (result?.replayed)
				return {
					block: true,
					reason:
						"This protected attempt was already dispatched; reconcile its outcome instead of repeating the side effect.",
				};
			attempts.set(event.toolCallId, permitId);
		} catch (error) {
			return { block: true, reason: `Coordination permit rejected: ${(error as Error).message}` };
		}
	});
	pi.on("tool_execution_end", async (event) => {
		const permitId = attempts.get(event.toolCallId);
		if (!permitId) return;
		attempts.delete(event.toolCallId);
		try {
			const { broker, actor } = getContext();
			await broker.settlePermit({
				actor,
				permitId,
				outcome: event.isError ? "outcome_unknown" : "settled",
				idempotencyKey: `tool-settle:${event.toolCallId}`,
			});
		} catch {
			// Never release a dispatched reservation when transport or settlement fails.
		}
	});
	pi.on("session_shutdown", () => {
		armed.clear();
		attempts.clear();
	});
	return async (permitId, operation) => {
		if (!enabled()) throw new Error("Coordination is unavailable in this session.");
		if (!operations().includes(operation))
			throw new Error("Operation is not protected by this session.");
		if (armed.size >= 100 && !armed.has(permitId))
			throw new Error("Pending permit limit was reached.");
		const { broker, actor } = getContext();
		const permit = await broker.getPermit({ actor, permitId });
		if (
			permit?.state !== "reserved" ||
			permit.operation !== operation ||
			permit.expiresAt <= Date.now()
		)
			throw new Error("Permit is unavailable, expired, or bound to another operation.");
		armed.set(permitId, operation);
		return { armed: true, permitId, operation };
	};
}
