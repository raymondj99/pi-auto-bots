import type { CoordinationProjection } from "./protocol.ts";

/** Process completion and reviewed task completion are different facts. */
export function formatRunTaskOutcome(
	projection: CoordinationProjection,
	roleId: string,
	runId: string,
): string {
	if (projection.roles[roleId]?.runId !== runId)
		return "Workflow: this result is from an older run; it cannot close a replacement run's tasks.";
	const tasks = Object.values(projection.tasks).filter(
		(t) => t.owner === roleId && t.activeRunId === runId,
	);
	if (!tasks.length) return "";
	const lines = ["Workflow status (process exit is not review approval):"];
	for (const task of tasks.slice(0, 10)) {
		lines.push(
			`${task.id}@${task.revision}: ${task.state}${task.state === "review" && !task.reviewSatisfied ? " — awaiting coordinator review" : ""}.`,
		);
		if (task.state === "review") {
			if (!task.outputs.length || !task.verification.length)
				lines.push(
					"Attach current frozen outputs and verification before approval; the final message alone is not verified task evidence.",
				);
			else
				lines.push(
					`Coordinator: subagent_task approve or changes_requested; taskId=${task.id}, expectedRevision=${task.revision}, reason required. Fast-mode approval also requires explicit nextActions ([] if none) and completes the task.`,
				);
		} else if (task.state === "blocked")
			lines.push("Resume or hand off the unfinished task; do not report it as complete.");
	}
	if (tasks.length > 10) lines.push("Additional tasks omitted; query scoped task state.");
	return lines.join("\n");
}
