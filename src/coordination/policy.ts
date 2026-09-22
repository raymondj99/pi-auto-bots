import type {
	ArtifactRef,
	ArtifactStateRecord,
	CoordinationActor,
	CoordinationProjection,
	SignalStateRecord,
	StableErrorCode,
	TaskState,
	TaskStateRecord,
} from "./protocol.ts";

export class CoordinationPolicyError extends Error {
	code: StableErrorCode;
	current?: unknown;
	constructor(code: StableErrorCode, message: string, current?: unknown) {
		super(message);
		this.code = code;
		this.current = current;
	}
}

export const TASK_TRANSITIONS: Record<TaskState, TaskState[]> = Object.freeze({
	draft: ["ready", "blocked", "cancelled"],
	ready: ["in_progress", "blocked", "cancelled"],
	in_progress: ["blocked", "submitted", "failed", "cancelled"],
	blocked: ["ready", "cancelled", "failed"],
	submitted: ["review", "in_progress", "completed", "failed"],
	review: ["in_progress", "completed", "failed", "cancelled"],
	completed: [],
	failed: [],
	cancelled: [],
});

export const ACL_TABLE = Object.freeze({
	admitRole: ["coordinator"],
	rebindRole: ["coordinator"],
	channelMutate: ["coordinator"],
	emitSignal: ["coordinator", "worker", "reviewer"],
	resolveSignal: ["coordinator", "reviewer"],
	acknowledge: ["self"],
	freezeArtifact: ["coordinator", "reviewer"],
	approve: ["coordinator", "reviewer"],
	budget: ["coordinator"],
	permit: ["coordinator", "broker"],
	queryPrivate: ["coordinator", "evaluator"],
	override: ["coordinator"],
});

export const CONFIDENTIALITY_TABLE = Object.freeze({
	publicTaskFields: [
		"id",
		"title",
		"owner",
		"state",
		"revision",
		"dependsOn",
		"blockers",
		"requiredInputs",
		"outputs",
		"nextActions",
	],
	privateArtifactFields: ["path"],
	evaluatorOnlyFields: ["verification", "privateTo"],
});

export function requireCapability(
	projection: Pick<CoordinationProjection, "roles">,
	actor: CoordinationActor,
	capability: string,
): void {
	const role = projection.roles[actor.roleId];
	if (!role || role.generation !== actor.generation || role.runId !== actor.runId || !role.active) {
		throw new CoordinationPolicyError(
			"STALE_GENERATION",
			"Actor role generation is not active.",
			role,
		);
	}
	if (capability === "self") return;
	if (!role.capabilities.includes(capability) && !role.capabilities.includes("coordinator")) {
		throw new CoordinationPolicyError(
			"UNAUTHORIZED",
			`Role ${actor.roleId} lacks ${capability} permission.`,
		);
	}
}

export function assertExpectedRevision(
	current: { revision: number },
	expectedRevision: number | undefined,
): void {
	if (expectedRevision === undefined || current.revision !== expectedRevision) {
		throw new CoordinationPolicyError(
			"STALE_REVISION",
			"Mutation requires the current revision.",
			current,
		);
	}
}

export function requireArtifactAccess(
	projection: CoordinationProjection,
	roleId: string,
	artifact: ArtifactStateRecord,
): void {
	const privileged = projection.roles[roleId]?.capabilities.some(
		(capability) => capability === "coordinator" || capability === "evaluator",
	);
	if (artifact.privateTo && !artifact.privateTo.includes(roleId) && !privileged)
		throw new CoordinationPolicyError("UNAUTHORIZED", "Artifact is private to another role.");
}

function artifactVersion(projection: CoordinationProjection, ref: ArtifactRef) {
	return projection.artifacts[ref.artifactId]?.find(
		(artifact) => artifact.version === ref.version && artifact.digest === ref.digest,
	);
}

export function taskGateSignals(
	projection: CoordinationProjection,
	task: TaskStateRecord,
): SignalStateRecord[] {
	return Object.values(projection.signals).filter((signal) => {
		if (task.blockers.includes(signal.id) || task.dependsOn.includes(signal.id)) return true;
		if (
			signal.state === "superseded" ||
			!(
				signal.requiresAck.length > 0 ||
				signal.severity === "blocking" ||
				["hold", "blocked", "changes_requested"].includes(signal.kind)
			)
		)
			return false;
		// Explicit task targets define gate scope; role/channel targets also route the
		// notification but must not hold every unrelated task owned by an observer.
		const tasks = signal.targets.filter((target) => target.taskId);
		if (tasks.length) return tasks.some((target) => target.taskId === task.id);
		return signal.targets.some(
			(target) =>
				target.roleId === task.owner ||
				(target.channelId &&
					projection.channels[target.channelId]?.members.some(
						(member) => member.roleId === task.owner && member.mode === "participate",
					)),
		);
	});
}

export function assertTaskCanStart(
	projection: CoordinationProjection,
	task: TaskStateRecord,
	requireLease = true,
): void {
	for (const dependency of task.dependsOn) {
		const depTask = projection.tasks[dependency];
		const depSignal = projection.signals[dependency];
		if (
			(!depTask && !depSignal) ||
			(depTask && depTask.state !== "completed") ||
			(depSignal && depSignal.state !== "resolved")
		) {
			throw new CoordinationPolicyError(
				"UNRESOLVED_DEPENDENCY",
				"Task dependency is missing or unresolved.",
				depTask ?? depSignal ?? { id: dependency },
			);
		}
	}
	for (const signalId of task.blockers) {
		const signal = projection.signals[signalId];
		if (signal?.state !== "resolved")
			throw new CoordinationPolicyError(
				"UNRESOLVED_BLOCKER",
				"Task has a missing or open blocker.",
				signal ?? { id: signalId },
			);
	}
	const gates = taskGateSignals(projection, task);
	for (const gate of gates)
		if (
			gate.state !== "resolved" &&
			(gate.severity === "blocking" || ["hold", "blocked", "changes_requested"].includes(gate.kind))
		) {
			throw new CoordinationPolicyError(
				"UNRESOLVED_BLOCKER",
				"A typed task, role or channel hold remains unresolved.",
				{ id: gate.id },
			);
		}
	for (const roleId of new Set([
		...task.requiredAcknowledgements,
		...gates.flatMap((signal) => signal.requiresAck),
	])) {
		const obligations = gates.filter(
			(signal) => signal.requiresAck.includes(roleId) && signal.state !== "superseded",
		);
		if (
			obligations.length === 0 ||
			obligations.some(
				(signal) =>
					signal.state !== "resolved" &&
					!signal.acknowledgements.some(
						(ack) =>
							ack.roleId === roleId && projection.roles[roleId]?.generation === ack.generation,
					),
			)
		) {
			throw new CoordinationPolicyError(
				"ACK_REQUIRED",
				"A current-generation acknowledgement is missing.",
				{ roleId },
			);
		}
	}
	for (const input of task.requiredInputs) {
		const artifact = artifactVersion(projection, input);
		if (artifact) requireArtifactAccess(projection, task.owner, artifact);
		if (artifact?.state !== "frozen")
			throw new CoordinationPolicyError("INPUT_NOT_FROZEN", "Required input is not frozen.", input);
		const latest = projection.artifacts[input.artifactId]?.at(-1);
		if (latest && (latest.version !== input.version || latest.digest !== input.digest))
			throw new CoordinationPolicyError("INPUT_SUPERSEDED", "Required input has been superseded.", {
				required: input,
				latest,
			});
	}
	if (
		requireLease &&
		(task.leases.length === 0 ||
			!Object.values(projection.budgets).some((budget) =>
				task.leases.some((id) => budget.reservations[id]?.state === "reserved"),
			))
	) {
		throw new CoordinationPolicyError(
			"LEASE_REQUIRED",
			"A start transition requires a live reserved budget/resource lease.",
			task,
		);
	}
}

export function assertTaskCanComplete(
	projection: CoordinationProjection,
	task: TaskStateRecord,
): void {
	if (
		task.permits.some((id) =>
			["reserved", "dispatched", "outcome_unknown"].includes(projection.permits[id]?.state),
		)
	) {
		throw new CoordinationPolicyError(
			"PERMIT_NOT_USABLE",
			"Completion requires settling or reconciling every outstanding attempt.",
			task,
		);
	}
	if (
		task.outputs.length === 0 ||
		task.verification.length === 0 ||
		task.verification.some((evidence) => !evidence.trim()) ||
		!task.reviewSatisfied
	) {
		throw new CoordinationPolicyError(
			"INVALID_TRANSITION",
			"Completion requires outputs, verification evidence, and satisfied review.",
			task,
		);
	}
	for (const output of task.outputs) {
		const artifact = artifactVersion(projection, output);
		if (artifact) requireArtifactAccess(projection, task.owner, artifact);
		const latest = projection.artifacts[output.artifactId]?.at(-1);
		if (
			artifact?.state !== "frozen" ||
			latest?.version !== output.version ||
			latest.digest !== output.digest
		) {
			throw new CoordinationPolicyError(
				"INPUT_SUPERSEDED",
				"Completion outputs must reference current frozen artifacts.",
			);
		}
	}
}

export function redactProjection(
	projection: CoordinationProjection,
	viewerRoleId: string,
): CoordinationProjection {
	const viewer = projection.roles[viewerRoleId];
	if (!viewer) throw new CoordinationPolicyError("UNAUTHORIZED", "Unknown projection viewer.");
	const privileged = !!viewer?.capabilities.some(
		(cap) => cap === "coordinator" || cap === "evaluator",
	);
	// Do not copy the potentially large retry ledger just to discard it.
	const clone = structuredClone({ ...projection, idempotency: {} }) as CoordinationProjection;
	// Idempotency results and filesystem paths are broker-private, never query data.
	for (const role of Object.values(clone.roles)) delete role.checkpoint;
	for (const versions of Object.values(clone.artifacts))
		for (const artifact of versions) delete artifact.path;
	const visibleRef = (ref: ArtifactRef) =>
		clone.artifacts[ref.artifactId]?.some(
			(artifact) => artifact.version === ref.version && artifact.digest === ref.digest,
		);
	if (!privileged) {
		for (const [id, versions] of Object.entries(clone.artifacts)) {
			clone.artifacts[id] = versions.filter(
				(artifact) => !artifact.privateTo || artifact.privateTo.includes(viewerRoleId),
			);
			if (!clone.artifacts[id].length) delete clone.artifacts[id];
		}
		for (const [id, role] of Object.entries(clone.roles))
			if (role.private && id !== viewerRoleId) delete clone.roles[id];
		for (const [id, channel] of Object.entries(clone.channels))
			if (!channel.members.some((member) => member.roleId === viewerRoleId))
				delete clone.channels[id];
		const reviewTasks = new Set(
			Object.values(clone.channels)
				.filter((c) => c.review?.reviewerRoleId === viewerRoleId)
				.flatMap((c) => c.review!.taskIds),
		);
		for (const [id, task] of Object.entries(clone.tasks)) {
			if (task.owner !== viewerRoleId && !reviewTasks.has(id)) {
				delete clone.tasks[id];
				continue;
			}
			if (!reviewTasks.has(id)) task.verification = [];
			task.requiredInputs = task.requiredInputs.filter(visibleRef);
			if (task.reviewSourceInputs)
				task.reviewSourceInputs = task.reviewSourceInputs.filter(visibleRef);
			task.outputs = task.outputs.filter(visibleRef);
		}
		for (const [id, signal] of Object.entries(clone.signals)) {
			const visible =
				signal.requiresAck.includes(viewerRoleId) ||
				signal.targets.some(
					(target) =>
						target.roleId === viewerRoleId ||
						!!(target.channelId && clone.channels[target.channelId]) ||
						!!(target.taskId && clone.tasks[target.taskId]),
				);
			if (!visible || signal.artifactRefs.some((ref) => !visibleRef(ref))) {
				delete clone.signals[id];
				continue;
			}
			signal.targets = signal.targets.filter(
				(target) =>
					target.roleId === viewerRoleId ||
					!!(target.channelId && clone.channels[target.channelId]) ||
					!!(target.taskId && clone.tasks[target.taskId]),
			);
		}
		for (const task of Object.values(clone.tasks)) {
			task.dependsOn = task.dependsOn.filter((id) => !!clone.tasks[id] || !!clone.signals[id]);
			task.blockers = task.blockers.filter((id) => !!clone.signals[id]);
		}
		for (const [id, permit] of Object.entries(clone.permits))
			if (permit.roleId !== viewerRoleId) delete clone.permits[id];
		for (const [id, delivery] of Object.entries(clone.deliveries))
			if (delivery.targetRoleId !== viewerRoleId) delete clone.deliveries[id];
		const leases = new Set(Object.values(clone.tasks).flatMap((task) => task.leases));
		for (const [id, budget] of Object.entries(clone.budgets)) {
			budget.reservations = Object.fromEntries(
				Object.entries(budget.reservations).filter(([key]) => leases.has(key)),
			);
			if (!Object.keys(budget.reservations).length) delete clone.budgets[id];
		}
	}
	// Audit projections expose metadata, not raw persisted mutation payloads. The latter
	// can contain evaluator evidence, sealed paths and whole recovery snapshots.
	clone.audit = clone.audit.flatMap((event) => {
		const payload = event.payload as Record<string, unknown>;
		const id = typeof payload.id === "string" ? payload.id : undefined;
		const artifactId = typeof payload.artifactId === "string" ? payload.artifactId : undefined;
		const roleId = typeof payload.roleId === "string" ? payload.roleId : undefined;
		if (!privileged) {
			const visible =
				(id &&
					(clone.tasks[id] ||
						clone.channels[id] ||
						clone.signals[id] ||
						clone.permits[id] ||
						clone.deliveries[id] ||
						clone.budgets[id])) ||
				(artifactId &&
					clone.artifacts[artifactId]?.some(
						(artifact) =>
							artifact.version === payload.version && artifact.digest === payload.digest,
					)) ||
				roleId === viewerRoleId;
			if (
				!visible ||
				(event.channelId && !clone.channels[event.channelId]) ||
				(event.taskId && !clone.tasks[event.taskId])
			)
				return [];
		}
		const auditPayload: Record<string, unknown> = Object.fromEntries(
			Object.entries(payload).filter(
				([key, value]) =>
					["id", "artifactId", "roleId", "state", "revision", "version", "generation"].includes(
						key,
					) &&
					(typeof value === "string" || typeof value === "number"),
			),
		);
		if (event.type === "channel.message" && id && clone.channels[id]) {
			auditPayload.targets = [{ channelId: id }];
			auditPayload.summary = typeof payload.summary === "string" ? payload.summary : "";
			const signal = payload.signal as SignalStateRecord | undefined;
			if (signal) auditPayload.signal = { id: signal.id, revision: signal.revision };
		}
		if (event.type.startsWith("signal.") && id && clone.signals[id]) {
			auditPayload.targets = clone.signals[id].targets;
			if (typeof payload.summary === "string") auditPayload.summary = payload.summary;
		}
		return [{ ...event, payload: auditPayload }];
	});
	return clone;
}
