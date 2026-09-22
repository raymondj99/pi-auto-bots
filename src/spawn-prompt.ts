/** Accept a misfiled brief without weakening the public schema or inventing work. */
export function prepareSpawnArguments(args: unknown): unknown {
	if (!args || typeof args !== "object" || Array.isArray(args)) return args;
	const input = args as Record<string, unknown>;
	if (typeof input.task === "string" && input.task.trim()) return args;
	if (input.task !== undefined && typeof input.task !== "string") return args;
	if (typeof input.systemPrompt === "string" && input.systemPrompt.trim()) {
		// Move, do not duplicate: an agent preset's identity must not swallow the brief.
		const { systemPrompt, ...rest } = input;
		return { ...rest, task: systemPrompt };
	}
	throw new Error(
		'subagent needs a task brief. Supply task:"Outcome and owned files". Coordination instructions are automatic; taskId identifies an assignment, not its instructions.',
	);
}
