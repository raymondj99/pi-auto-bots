import { randomUUID } from "node:crypto";
import type {
	ArtifactStateRecord,
	BudgetState,
	ChannelState,
	CoordinationActor,
	CoordinationEnvelope,
	CoordinationProjection,
	DeliveryRecord,
	PermitStateRecord,
	SignalStateRecord,
	TaskExitUpdate,
	TaskStateRecord,
} from "./protocol.ts";
import type { ReviewMessage, ReviewReadinessEvent } from "./review.ts";

export function createProjection(
	sessionId = "session",
	branchId = "branch",
): CoordinationProjection {
	return {
		protocolVersion: 2,
		seq: 0,
		sessionId,
		branchId,
		online: true,
		roles: {},
		channels: {},
		tasks: {},
		signals: {},
		artifacts: {},
		budgets: {},
		permits: {},
		deliveries: {},
		audit: [],
		idempotency: {},
	};
}

export function applyTaskExitUpdate(
	task: TaskStateRecord,
	update: TaskExitUpdate,
): TaskStateRecord {
	const nextActions =
		update.nextAction &&
		task.nextActions.length < 100 &&
		!task.nextActions.includes(update.nextAction)
			? [update.nextAction, ...task.nextActions]
			: task.nextActions;
	return { ...task, state: update.state, revision: update.revision, nextActions };
}

function bindTaskProjection(next: CoordinationProjection, task: TaskStateRecord) {
	next.tasks[task.id] = structuredClone(task);
	for (const [id, versions] of Object.entries(next.artifacts)) {
		const updated = versions.map((artifact) => {
			const consumes = task.requiredInputs.some(
				(ref) =>
					ref.artifactId === artifact.artifactId &&
					ref.version === artifact.version &&
					ref.digest === artifact.digest,
			);
			if (consumes === artifact.consumers.includes(task.id)) return artifact;
			return {
				...artifact,
				consumers: consumes
					? [...artifact.consumers, task.id]
					: artifact.consumers.filter((id) => id !== task.id),
			};
		});
		if (updated.some((artifact, i) => artifact !== versions[i])) next.artifacts[id] = updated;
	}
}

export function systemActor(): CoordinationActor {
	return { roleId: "broker", runId: "broker", generation: 0 };
}

export function reduceCoordinationEvent(
	projection: CoordinationProjection,
	event: CoordinationEnvelope,
	copyOnWrite = false,
): CoordinationProjection {
	if (event.protocolVersion !== 2) throw new Error("Unsupported coordination event version.");
	if (event.seq !== projection.seq + 1)
		throw new Error(
			`Non-monotonic coordination sequence ${event.seq}; expected ${projection.seq + 1}.`,
		);
	event = structuredClone(event);
	// The event log owns immutable revisions. Copy maps, not the retained history
	// and every prior idempotency result, on each append. Public callers retain
	// the isolated reducer contract unless they explicitly opt into sharing.
	const next: CoordinationProjection = copyOnWrite
		? {
				...projection,
				roles: { ...projection.roles },
				channels: { ...projection.channels },
				tasks: { ...projection.tasks },
				signals: { ...projection.signals },
				artifacts: { ...projection.artifacts },
				budgets: { ...projection.budgets },
				permits: { ...projection.permits },
				deliveries: { ...projection.deliveries },
				audit: [...projection.audit],
				idempotency: { ...projection.idempotency },
			}
		: structuredClone(projection);
	next.seq = event.seq;
	next.audit.push(event);
	if (next.audit.length > 1000) next.audit.splice(0, next.audit.length - 1000);
	switch (event.type) {
		case "role.admitted": {
			const p = event.payload as { roleId: string; capabilities?: string[]; private?: boolean };
			next.roles[p.roleId] = {
				roleId: p.roleId,
				generation: 0,
				active: false,
				capabilities: p.capabilities ?? [],
				private: p.private,
			};
			break;
		}
		case "role.bound": {
			const p = event.payload as {
				roleId: string;
				runId: string;
				generation: number;
				capabilities?: string[];
			};
			const previous = next.roles[p.roleId] ?? {
				roleId: p.roleId,
				generation: 0,
				active: false,
				capabilities: [],
			};
			next.roles[p.roleId] = {
				...previous,
				runId: p.runId,
				generation: p.generation,
				active: true,
				unavailable: false,
				capabilities: p.capabilities ?? previous.capabilities,
			};
			for (const task of Object.values(next.tasks))
				if (
					task.owner === p.roleId &&
					!task.permits.some((permitId) =>
						["dispatched", "outcome_unknown"].includes(next.permits[permitId]?.state ?? ""),
					)
				)
					next.tasks[task.id] = { ...task, activeRunId: p.runId };
			break;
		}
		case "role.rebound": {
			const p = event.payload as {
				roleId: string;
				runId: string;
				generation: number;
				checkpoint?: string;
			};
			const previous = next.roles[p.roleId];
			next.roles[p.roleId] = {
				...(previous ?? { roleId: p.roleId, capabilities: [] }),
				runId: p.runId,
				generation: p.generation,
				active: true,
				unavailable: false,
				checkpoint: p.checkpoint,
			};
			for (const task of Object.values(next.tasks))
				if (
					task.owner === p.roleId &&
					!task.permits.some((permitId) =>
						["dispatched", "outcome_unknown"].includes(next.permits[permitId]?.state ?? ""),
					)
				)
					next.tasks[task.id] = { ...task, activeRunId: p.runId };
			break;
		}
		case "role.detached": {
			const p = event.payload as { roleId: string; affectedTasks?: TaskExitUpdate[] };
			if (next.roles[p.roleId])
				next.roles[p.roleId] = { ...next.roles[p.roleId], active: false, unavailable: true };
			for (const update of p.affectedTasks ?? [])
				next.tasks[update.id] = applyTaskExitUpdate(next.tasks[update.id], update);
			break;
		}
		case "channel.created": {
			const channel = event.payload as ChannelState;
			next.channels[channel.id] = structuredClone(channel);
			break;
		}
		case "channel.membership_changed": {
			const p = event.payload as ChannelState;
			next.channels[p.id] = structuredClone(p);
			break;
		}
		case "channel.closed": {
			const p = event.payload as { id: string; revision: number };
			if (next.channels[p.id])
				next.channels[p.id] = { ...next.channels[p.id], status: "closed", revision: p.revision };
			break;
		}
		case "review.readiness": {
			const { channelId, readiness, signal, deliveries, task } =
				event.payload as ReviewReadinessEvent;
			if (task) bindTaskProjection(next, task);
			const channel = next.channels[channelId];
			const previous = channel.readiness?.signalId;
			if (previous && next.signals[previous]) {
				next.signals[previous] = { ...next.signals[previous], state: "superseded" };
				for (const d of Object.values(next.deliveries))
					if (d.signalId === previous && d.state !== "resolved")
						next.deliveries[d.id] = { ...d, state: "resolved", resolvedAt: event.timestamp };
			}
			next.channels[channelId] = { ...channel, readiness };
			next.signals[signal.id] = signal;
			for (const d of deliveries) next.deliveries[d.id] = d;
			break;
		}
		case "channel.message": {
			const payload = event.payload as { signal: SignalStateRecord; deliveries: DeliveryRecord[] };
			next.signals[payload.signal.id] = structuredClone(payload.signal);
			for (const delivery of payload.deliveries)
				next.deliveries[delivery.id] = structuredClone(delivery);
			break;
		}
		case "task.created":
		case "task.transitioned":
		case "task.handed_off":
		case "task.inputs_bound": {
			const { reviewMessage, ...task } = event.payload as TaskStateRecord & {
				reviewMessage?: ReviewMessage;
			};
			next.tasks[task.id] = structuredClone(task);
			if (reviewMessage) {
				next.signals[reviewMessage.signal.id] = reviewMessage.signal;
				for (const delivery of reviewMessage.deliveries) next.deliveries[delivery.id] = delivery;
			}
			bindTaskProjection(next, task);
			break;
		}
		case "signal.emitted":
		case "signal.acknowledged":
		case "signal.resolved":
		case "signal.superseded": {
			const { deliveries, previous, affectedTasks, ...signal } =
				event.payload as SignalStateRecord & {
					deliveries?: DeliveryRecord[];
					previous?: SignalStateRecord;
					affectedTasks?: TaskStateRecord[];
				};
			if (previous) {
				if (!next.signals[previous.id] || signal.supersedes !== previous.id)
					throw new Error("Invalid signal predecessor.");
				next.signals[previous.id] = structuredClone(previous);
			}
			for (const task of affectedTasks ?? []) next.tasks[task.id] = structuredClone(task);
			next.signals[signal.id] = structuredClone(signal);
			for (const delivery of deliveries ?? [])
				next.deliveries[delivery.id] = structuredClone(delivery);
			break;
		}
		case "artifact.sealed":
		case "artifact.frozen":
		case "artifact.superseded":
		case "artifact.invalidated":
		case "artifact.verified": {
			const {
				previous,
				affectedTasks,
				reason: _reason,
				...artifact
			} = event.payload as ArtifactStateRecord & {
				previous?: ArtifactStateRecord;
				affectedTasks?: TaskStateRecord[];
				reason?: string;
			};
			const versions = [...(next.artifacts[artifact.artifactId] ?? [])];
			if (previous) {
				const previousIndex = versions.findIndex((item) => item.version === previous.version);
				if (previousIndex < 0 || previous.artifactId !== artifact.artifactId)
					throw new Error("Invalid superseded artifact predecessor.");
				versions[previousIndex] = structuredClone(previous);
			}
			for (const task of affectedTasks ?? []) next.tasks[task.id] = structuredClone(task);
			artifact.consumers = Object.values(next.tasks)
				.filter((task) =>
					task.requiredInputs.some(
						(ref) =>
							ref.artifactId === artifact.artifactId &&
							ref.version === artifact.version &&
							ref.digest === artifact.digest,
					),
				)
				.map((task) => task.id);
			const index = versions.findIndex((item) => item.version === artifact.version);
			if (index >= 0) versions[index] = structuredClone(artifact);
			else versions.push(structuredClone(artifact));
			versions.sort((a, b) => a.version - b.version);
			next.artifacts[artifact.artifactId] = versions;
			break;
		}
		case "budget.created":
		case "budget.reserved":
		case "budget.released": {
			const { task, ...budget } = event.payload as BudgetState & { task?: TaskStateRecord };
			next.budgets[budget.id] = structuredClone(budget);
			if (task) next.tasks[task.id] = structuredClone(task);
			break;
		}
		case "permit.issued":
		case "permit.dispatched":
		case "permit.settled": {
			const {
				budget,
				task: attachedTask,
				reason: _reason,
				reconciledBy: _reconciledBy,
				...permit
			} = event.payload as PermitStateRecord & {
				budget?: BudgetState;
				task?: TaskStateRecord;
				reason?: string;
				reconciledBy?: CoordinationActor;
			};
			if (budget) next.budgets[budget.id] = structuredClone(budget);
			if (attachedTask) next.tasks[attachedTask.id] = structuredClone(attachedTask);
			next.permits[permit.id] = structuredClone(permit);
			const task = next.tasks[permit.taskId];
			if (task && !task.permits.includes(permit.id))
				next.tasks[task.id] = { ...task, permits: [...task.permits, permit.id] };
			break;
		}
		case "delivery.queued":
		case "delivery.delivered":
		case "delivery.acknowledged":
		case "delivery.resolved": {
			const delivery = event.payload as DeliveryRecord;
			next.deliveries[delivery.id] = structuredClone(delivery);
			break;
		}
		case "checkpoint.saved": {
			const p = event.payload as { roleId: string; checkpoint: string };
			if (next.roles[p.roleId])
				next.roles[p.roleId] = { ...next.roles[p.roleId], checkpoint: p.checkpoint.slice(0, 8000) };
			break;
		}
	}
	return next;
}

export function makeEnvelope<T>(
	projection: CoordinationProjection,
	type: CoordinationEnvelope<T>["type"],
	actor: CoordinationActor,
	payload: T,
	idempotencyKey: string,
	refs: Partial<
		Pick<CoordinationEnvelope, "taskId" | "channelId" | "goalId" | "causationId" | "correlationId">
	> = {},
): CoordinationEnvelope<T> {
	return structuredClone({
		protocolVersion: 2 as const,
		seq: projection.seq + 1,
		eventId: randomUUID(),
		idempotencyKey,
		timestamp: Date.now(),
		actor,
		type,
		payload,
		...refs,
	});
}

export function restoreOffline(
	events: CoordinationEnvelope[],
	sessionId: string,
	branchId: string,
): CoordinationProjection {
	let projection = createProjection(sessionId, branchId);
	for (const event of events
		.filter((entry) => entry.protocolVersion === 2)
		.sort((a, b) => a.seq - b.seq))
		projection = reduceCoordinationEvent(projection, event);
	projection.online = false;
	for (const role of Object.values(projection.roles)) {
		role.active = false;
		role.runId = undefined;
	}
	return projection;
}
