/** Only actionable receipt fields enter model context; details retain the event. */
export function compactMutationResult(value: unknown): unknown {
	if (!value || typeof value !== "object" || !("event" in value)) return value;
	const { event, replayed, warning } = value as {
		event?: { seq: number; type: string; payload: Record<string, unknown> };
		replayed?: boolean;
		warning?: string;
	};
	if (!event) return value;
	const p = event.payload;
	const result: Record<string, unknown> = { seq: event.seq, type: event.type };
	for (const key of [
		"id",
		"roleId",
		"runId",
		"generation",
		"revision",
		"state",
		"owner",
		"artifactId",
		"version",
		"digest",
		"bytes",
		"taskId",
		"taskRevision",
		"budgetId",
		"reservationId",
		"operation",
		"expiresAt",
		"used",
		"limit",
		"dispatchedUnknown",
		"queued",
		"offline",
	]) {
		if (p[key] !== undefined) result[key] = p[key];
	}
	if (event.type === "budget.reserved") {
		result.reservationId = Object.keys((p.reservations as object) ?? {}).at(-1);
		const task = p.task as { id: string; revision: number } | undefined;
		if (task) result.task = { id: task.id, revision: task.revision };
	}
	if (p.requiresAck) result.requiresAck = p.requiresAck;
	if (replayed) result.replayed = true;
	if (warning) result.warning = warning;
	const notice = p.reviewMessage as { id: string; deliveries: unknown[] } | undefined;
	if (notice) result.peerHandoff = { channelId: notice.id, queued: notice.deliveries.length };
	return result;
}

export function publicCoordinationError(error: unknown): {
	error: string;
	code?: string;
	current?: Record<string, unknown>;
} {
	const failure = error as { message?: string; code?: string; current?: unknown };
	const result: { error: string; code?: string; current?: Record<string, unknown> } = {
		error: String(failure?.message ?? error).slice(0, 1000),
	};
	if (typeof failure?.code === "string" && /^[A-Z][A-Z_]{1,60}$/.test(failure.code))
		result.code = failure.code;
	if (failure?.current && typeof failure.current === "object") {
		result.current = Object.fromEntries(
			Object.entries(failure.current).filter(
				([key, value]) =>
					["id", "revision", "state", "roleId", "taskId"].includes(key) &&
					((typeof value === "string" && value.length <= 200) ||
						(typeof value === "number" && Number.isSafeInteger(value))),
			),
		);
	}
	return result;
}

/** Mutation responses expose their accepted event, not the broker's full projection.
 * Full projections are available only through an authorization-filtered query.
 */
export function publicMutationResult(value: unknown): unknown {
	if (!value || typeof value !== "object" || !Object.hasOwn(value, "event")) return value;
	const result = value as Record<string, unknown>;
	const sanitize = (item: unknown): unknown => {
		if (Array.isArray(item)) return item.map(sanitize);
		if (!item || typeof item !== "object") return item;
		return Object.fromEntries(
			Object.entries(item)
				.filter(
					([key]) =>
						![
							"path",
							"checkpoint",
							"idempotency",
							"projection",
							"affectedTasks",
							"verification",
						].includes(key),
				)
				.map(([key, child]) => [key, sanitize(child)]),
		);
	};
	return {
		event: sanitize(result.event),
		...(result.replayed ? { replayed: true } : {}),
		...(typeof result.warning === "string" ? { warning: result.warning.slice(0, 1000) } : {}),
	};
}
