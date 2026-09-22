import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { join } from "node:path";
import {
	freezeArtifact,
	sealContentAddressedArtifact,
	supersedeArtifact,
	verifyArtifactBytes,
} from "./artifacts.ts";
import {
	buildDigest,
	formatSignalDelivery,
	markAcknowledged,
	markDelivered,
	markResolved,
	queueDelivery,
	recordWake,
	shouldWake,
} from "./delivery.ts";
import type { PersistedEntrySink } from "./event-log.ts";
import { CoordinationEventLog } from "./event-log.ts";
import { normalizeCoordinationRoleId, normalizeRoleReferences } from "./identity.ts";
import {
	createBudget,
	dispatchPermit,
	issuePermit,
	markReservationDispatched,
	releaseBudget,
	reserveBudget,
	settlePermit,
} from "./permits.ts";
import {
	assertExpectedRevision,
	assertTaskCanComplete,
	assertTaskCanStart,
	CoordinationPolicyError,
	redactProjection,
	requireArtifactAccess,
	requireCapability,
	TASK_TRANSITIONS,
	taskGateSignals,
} from "./policy.ts";
import { createProjection } from "./projection.ts";
import type {
	ArtifactRef,
	ChannelState,
	CoordinationActor,
	CoordinationEnvelope,
	CoordinationProjection,
	SignalStateRecord,
	TaskExitUpdate,
	TaskStateRecord,
} from "./protocol.ts";
import { buildQueryPage, type QueryOptions } from "./query.ts";
import {
	assertPeerReviewReady,
	peerReviewChannel,
	peerRunDisposition,
	reviewMessage,
	reviewReadiness,
} from "./review.ts";
import { validateMutationRequest } from "./validation.ts";

function stableId(namespace: string, key: string, actor?: CoordinationActor): string {
	return `${namespace}-${createHash("sha256")
		.update(namespace)
		.update("\0")
		.update(JSON.stringify([actor?.roleId, actor?.runId, actor?.generation, key]))
		.digest("hex")
		.slice(0, 24)}`;
}
function required(value: string | undefined, name: string): string {
	if (!value?.trim()) throw new CoordinationPolicyError("BOUNDS_EXCEEDED", `${name} is required.`);
	return value;
}

export function hasLiveCoordinationRuns(projection: CoordinationProjection): boolean {
	return Object.values(projection.roles).some(
		(role) => role.active && role.roleId !== "coordinator",
	);
}

export interface CoordinationBrokerOptions {
	artifactStoreDir?: string;
	workflowMode?: "fast" | "strict";
	maxPayloadBytes?: number;
	maxDigestLines?: number;
	permitTtlMs?: number;
	defaultDelivery?: "urgent" | "digest" | "next_turn";
}
export class CoordinationBroker {
	private log: CoordinationEventLog;
	private options: Required<CoordinationBrokerOptions>;
	private hasReviewChannels = false;
	private deliverySink?: (
		delivery: CoordinationProjection["deliveries"][string],
		signal: SignalStateRecord,
	) => void;
	constructor(
		sessionId: string,
		branchId: string,
		sink?: PersistedEntrySink,
		maxEvents = 1000,
		restored?: CoordinationProjection,
		options: CoordinationBrokerOptions = {},
	) {
		this.options = {
			artifactStoreDir:
				options.artifactStoreDir ??
				join(
					process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent"),
					"artifacts",
					"coordination",
					createHash("sha256").update(sessionId).digest("hex"),
				),
			workflowMode: options.workflowMode ?? "strict",
			maxPayloadBytes: options.maxPayloadBytes ?? 128 * 1024,
			maxDigestLines: options.maxDigestLines ?? 20,
			permitTtlMs: options.permitTtlMs ?? 300000,
			defaultDelivery: options.defaultDelivery ?? "digest",
		};
		this.log = new CoordinationEventLog(
			restored ?? createProjection(sessionId, branchId),
			sink,
			maxEvents,
			this.options.maxPayloadBytes,
		);
		this.hasReviewChannels = Object.values(restored?.channels ?? {}).some((c) => !!c.review);
		// Centralize retries before revision/state preconditions, but after fencing the
		// authenticated actor. Persist the canonical request digest on the accepted event.
		const mutations = [
			"admitRole",
			"bindRole",
			"rebindRole",
			"detachRole",
			"createChannel",
			"updateChannel",
			"closeChannel",
			"sendChannel",
			"createTask",
			"handoffTask",
			"transitionTask",
			"reviewTask",
			"bindTaskInputs",
			"emitSignal",
			"supersedeSignal",
			"acknowledgeSignal",
			"resolveSignal",
			"createBudget",
			"reserveBudget",
			"releaseBudget",
			"preparePermit",
			"issuePermit",
			"dispatchPermit",
			"settlePermit",
			"reconcilePermit",
			"sealArtifact",
			"freezeArtifact",
			"supersedeArtifact",
			"invalidateArtifact",
			"queueSignalDelivery",
			"receipt",
			"checkpoint",
		] as const;
		for (const method of mutations) {
			const original = this[method].bind(this) as (params: any) => unknown;
			Object.defineProperty(this, method, {
				value: (params: Record<string, unknown>) => {
					params = normalizeRoleReferences(params);
					validateMutationRequest(method, params);
					if (
						(method === "emitSignal" || method === "supersedeSignal") &&
						Array.isArray(params.targets)
					) {
						const targets = params.targets.flatMap((target) =>
							Object.entries(target).map(([key, value]) => ({ [key]: value })),
						);
						if (targets.length > 100)
							throw new CoordinationPolicyError(
								"BOUNDS_EXCEEDED",
								"Signal targets exceed 100 references.",
							);
						params = { ...params, targets };
					}
					const actor = params.actor as CoordinationActor | undefined;
					if (actor) this.authenticate(actor);
					else if (method !== "admitRole" && method !== "bindRole")
						throw new CoordinationPolicyError("UNAUTHORIZED", "Authenticated actor required.");
					const result = this.log.request(
						method,
						actor ?? { roleId: "broker", runId: "bootstrap", generation: 0 },
						params,
						() => original(params),
					);
					if (method === "createChannel" && params.review) this.hasReviewChannels = true;
					if (
						this.hasReviewChannels &&
						!["queueSignalDelivery", "receipt", "checkpoint"].includes(method)
					)
						try {
							this.refreshReviewReadiness();
						} catch (error) {
							Object.assign(result as object, {
								warning: `Mutation accepted; review readiness notification needs retry: ${(error as Error).message}`,
							});
						}
					return result;
				},
			});
		}
	}
	static restore(
		entries: Array<{ customType?: string; data?: unknown }>,
		sessionId: string,
		branchId: string,
		sink?: PersistedEntrySink,
		maxEvents = 1000,
		options: CoordinationBrokerOptions = {},
	) {
		return new CoordinationBroker(
			sessionId,
			branchId,
			sink,
			maxEvents,
			CoordinationEventLog.restore(entries, sessionId, branchId),
			options,
		);
	}
	snapshot(viewerRoleId?: string): CoordinationProjection {
		return this.log.snapshot(viewerRoleId);
	}
	role(roleId: string) {
		return this.log.roleSnapshot(normalizeCoordinationRoleId(roleId));
	}
	get workflowMode() {
		return this.options.workflowMode;
	}
	query(params: QueryOptions & { actor: CoordinationActor }) {
		this.authenticate(params.actor);
		if (params.kind === "roles" && params.id)
			params = { ...params, id: normalizeCoordinationRoleId(params.id) };
		return buildQueryPage(
			this.snapshot(params.actor.roleId),
			params,
			Math.max(2048, Math.floor((this.options.maxPayloadBytes ?? 131072) / 2)),
		);
	}
	runDisposition(params: { actor: CoordinationActor }) {
		this.authenticate(params.actor);
		return peerRunDisposition(this.log.stateSnapshot(), params.actor.roleId);
	}
	getPermit(params: { actor: CoordinationActor; permitId: string }) {
		this.authenticate(params.actor);
		const permit = this.log.stateSnapshot().permits[params.permitId];
		if (
			!permit ||
			permit.roleId !== params.actor.roleId ||
			permit.runId !== params.actor.runId ||
			permit.generation !== params.actor.generation
		)
			throw new CoordinationPolicyError(
				"PERMIT_NOT_USABLE",
				"Permit does not belong to this active role generation.",
			);
		return {
			id: permit.id,
			state: permit.state,
			operation: permit.operation,
			expiresAt: permit.expiresAt,
		};
	}
	setDeliverySink(
		sink: (
			delivery: CoordinationProjection["deliveries"][string],
			signal: SignalStateRecord,
		) => void,
	): void {
		this.deliverySink = sink;
		this.retryPendingDeliveries();
		this.refreshReviewReadiness();
	}
	private refreshReviewReadiness(): void {
		if (!this.hasReviewChannels) return;
		for (const channel of Object.values(this.log.stateSnapshot().channels).filter(
			(c) => c.review,
		)) {
			const p = this.log.stateSnapshot(),
				update = reviewReadiness(p, channel);
			if (!update) continue;
			if (
				Object.keys(p.signals).length >= 1000 ||
				Object.keys(p.deliveries).length + update.deliveries.length > 20000
			)
				throw new CoordinationPolicyError(
					"BOUNDS_EXCEEDED",
					"Review readiness delivery capacity exhausted.",
				);
			this.log.append(
				"review.readiness",
				{ roleId: "broker", runId: "review-readiness", generation: 0 },
				update,
				`review:${channel.id}:${update.readiness.revision}`,
				{ channelId: channel.id },
			);
			for (const delivery of update.deliveries)
				try {
					this.deliverySink?.(delivery, update.signal);
				} catch {
					/* Durable delivery retries on reconnect. */
				}
		}
	}
	retryPendingDeliveries(roleId?: string): void {
		const p = this.log.stateSnapshot();
		const views = new Map<string, CoordinationProjection>();
		for (const delivery of Object.values(p.deliveries)) {
			if (
				delivery.state !== "queued" ||
				delivery.wakeCount === 0 ||
				(roleId && delivery.targetRoleId !== roleId)
			)
				continue;
			if (!views.has(delivery.targetRoleId))
				views.set(delivery.targetRoleId, redactProjection(p, delivery.targetRoleId));
			const signal = delivery.signalId
				? views.get(delivery.targetRoleId)?.signals[delivery.signalId]
				: undefined;
			if (signal && signal.state !== "superseded") {
				try {
					this.deliverySink?.(delivery, signal);
				} catch {
					/* Keep accepted deliveries queued for reconnect. */
				}
			}
		}
	}
	private authenticate(actor: CoordinationActor, capability = "self") {
		const role = this.log.roleSnapshot(actor.roleId);
		requireCapability({ roles: role ? { [role.roleId]: role } : {} }, actor, capability);
	}
	admitRole(params: {
		actor?: CoordinationActor;
		roleId: string;
		capabilities?: string[];
		private?: boolean;
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		if (Object.keys(projection.roles).length > 0) {
			if (!params.actor)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Only an authenticated coordinator can admit roles.",
				);
			requireCapability(projection, params.actor, "coordinator");
		}
		if (Object.keys(projection.roles).length >= 101)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Team role limit reached.");
		const actor = params.actor ?? { roleId: "broker", runId: "bootstrap", generation: 0 };
		if (projection.roles[params.roleId])
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Role is already admitted.",
				projection.roles[params.roleId],
			);
		return this.log.append(
			"role.admitted",
			actor,
			{
				roleId: required(params.roleId, "roleId"),
				capabilities: params.capabilities ?? [],
				private: params.private,
			},
			params.idempotencyKey,
		);
	}
	bindRole(params: {
		actor?: CoordinationActor;
		roleId: string;
		runId: string;
		generation?: number;
		capabilities?: string[];
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		const current = projection.roles[params.roleId];
		if (!current)
			throw new CoordinationPolicyError("UNKNOWN_ROLE", "Role must be admitted before binding.");
		if (current.active) {
			if (!params.actor)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Active roles require coordinator rebind.",
				);
			requireCapability(projection, params.actor, "coordinator");
		}
		const generation = params.generation ?? current.generation + 1;
		if (generation <= current.generation)
			throw new CoordinationPolicyError("STALE_GENERATION", "Generation must increase.", current);
		const result = this.log.append(
			"role.bound",
			params.actor ?? { roleId: "broker", runId: "bootstrap", generation: 0 },
			{
				roleId: params.roleId,
				runId: required(params.runId, "runId"),
				generation,
				capabilities: params.capabilities ?? current.capabilities,
			},
			params.idempotencyKey,
		);
		this.retryPendingDeliveries(params.roleId);
		return result;
	}
	rebindRole(params: {
		actor: CoordinationActor;
		roleId: string;
		runId: string;
		idempotencyKey: string;
		checkpoint?: string;
	}) {
		const projection = this.log.stateSnapshot();
		requireCapability(projection, params.actor, "coordinator");
		const current = projection.roles[params.roleId];
		if (!current) throw new CoordinationPolicyError("UNKNOWN_ROLE", "Unknown role.");
		const result = this.log.append(
			"role.rebound",
			params.actor,
			{
				roleId: params.roleId,
				runId: required(params.runId, "runId"),
				generation: current.generation + 1,
				checkpoint: params.checkpoint?.slice(0, 8000),
			},
			params.idempotencyKey,
		);
		this.retryPendingDeliveries(params.roleId);
		return result;
	}
	detachRole(params: {
		actor: CoordinationActor;
		roleId: string;
		runId?: string;
		outcome?: "finished" | "failed" | "needs_help" | "interrupted";
		idempotencyKey: string;
	}) {
		this.authenticate(params.actor, "coordinator");
		const p = this.log.stateSnapshot(),
			role = p.roles[params.roleId];
		if (!role) throw new CoordinationPolicyError("UNKNOWN_ROLE", "Unknown role.");
		if (params.runId && role.runId !== params.runId)
			throw new CoordinationPolicyError(
				"STALE_GENERATION",
				"An old watcher cannot detach a replacement run.",
			);
		const affectedTasks: TaskExitUpdate[] = [];
		if (this.workflowMode === "fast" && role.active)
			for (const task of Object.values(p.tasks)) {
				if (
					task.owner !== role.roleId ||
					task.activeRunId !== role.runId ||
					["completed", "failed", "cancelled"].includes(task.state)
				)
					continue;
				let state = task.state;
				let nextAction = "Owner exited; resume or hand off unfinished work.";
				if (["submitted", "review"].includes(task.state)) {
					state = "review";
					nextAction =
						"Await coordinator review: approve or request changes; process exit is not approval.";
					if (task.reviewSatisfied && params.outcome === "finished")
						try {
							assertTaskCanStart(p, task, false);
							assertTaskCanComplete(p, task);
							for (const ref of task.outputs)
								verifyArtifactBytes(
									p.artifacts[ref.artifactId].find((a) => a.version === ref.version)!,
								);
							state = "completed";
							nextAction = "";
						} catch (error) {
							nextAction = `Completion blocked: ${(error as Error).message}`.slice(0, 160);
						}
				} else if (["draft", "ready", "in_progress"].includes(task.state)) state = "blocked";
				affectedTasks.push({
					id: task.id,
					state,
					revision: task.revision + 1,
					...(nextAction ? { nextAction } : {}),
				});
			}
		return this.log.append(
			"role.detached",
			params.actor,
			{
				roleId: params.roleId,
				...(params.runId ? { runId: params.runId } : {}),
				...(affectedTasks.length ? { affectedTasks } : {}),
			},
			params.idempotencyKey,
		);
	}
	createChannel(params: {
		actor: CoordinationActor;
		name: string;
		purpose: string;
		members: ChannelState["members"];
		delivery?: ChannelState["delivery"];
		review?: ChannelState["review"];
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		requireCapability(projection, params.actor, "coordinator");
		if (
			Object.keys(projection.channels).length >= 32 ||
			params.members.length < 1 ||
			params.members.length > 20 ||
			new Set(params.members.map((m) => m.roleId)).size !== params.members.length
		)
			throw new CoordinationPolicyError(
				"BOUNDS_EXCEEDED",
				"Invalid channel membership or channel limit reached.",
			);
		for (const member of params.members)
			if (!projection.roles[member.roleId])
				throw new CoordinationPolicyError("UNKNOWN_ROLE", `Unknown channel role ${member.roleId}.`);
		if (params.review) {
			if (params.review.reviewTaskId && this.workflowMode !== "fast")
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"Peer review lifecycle requires fast mode.",
				);
			if (
				params.review.reviewTaskId &&
				Object.values(projection.channels).some(
					(c) =>
						c.review?.reviewTaskId === params.review!.reviewTaskId ||
						(c.review?.reviewerRoleId === params.review!.reviewerRoleId && c.review?.reviewTaskId),
				)
			)
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"A peer reviewer has one review assignment per workflow.",
				);
			const reviewer = projection.roles[params.review.reviewerRoleId];
			if (
				!reviewer?.capabilities.some((c) => ["reviewer", "coordinator"].includes(c)) ||
				!params.members.some((m) => m.roleId === reviewer.roleId && m.mode === "participate")
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"The review role must be admitted with reviewer capability and participate in the channel.",
				);
			for (const id of params.review.taskIds) {
				const source = projection.tasks[id];
				if (
					source &&
					(source.owner === params.review.reviewerRoleId ||
						!params.members.some((m) => m.roleId === source.owner && m.mode === "participate"))
				)
					throw new CoordinationPolicyError(
						"UNAUTHORIZED",
						"Every source owner must participate, with an independent reviewer.",
					);
			}
			if (
				params.review.reviewTaskId &&
				projection.tasks[params.review.reviewTaskId] &&
				projection.tasks[params.review.reviewTaskId].owner !== params.review.reviewerRoleId
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Review assignment has a different owner.",
				);
			if (
				params.review.reviewTaskId &&
				projection.tasks[params.review.reviewTaskId] &&
				!["draft", "ready", "blocked"].includes(projection.tasks[params.review.reviewTaskId].state)
			)
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"Declare the peer channel before starting its review assignment.",
				);
			if (
				Object.values(projection.channels).some((c) =>
					c.review?.taskIds.some((id) => params.review!.taskIds.includes(id)),
				)
			)
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"A task already belongs to a review channel.",
				);
		}
		const channel: ChannelState = {
			...(params.review ? { review: structuredClone(params.review) } : {}),
			id: stableId("channel", params.idempotencyKey, params.actor),
			name: required(params.name, "name").slice(0, 80),
			purpose: required(params.purpose, "purpose").slice(0, 500),
			members: structuredClone(params.members),
			revision: 1,
			delivery: params.delivery ?? this.options.defaultDelivery,
			status: "open",
		};
		return this.log.append("channel.created", params.actor, channel, params.idempotencyKey, {
			channelId: channel.id,
		});
	}
	updateChannel(params: {
		actor: CoordinationActor;
		channelId: string;
		expectedRevision: number;
		members?: ChannelState["members"];
		delivery?: ChannelState["delivery"];
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		requireCapability(projection, params.actor, "coordinator");
		const channel = projection.channels[params.channelId];
		if (!channel) throw new CoordinationPolicyError("STALE_REVISION", "Unknown channel.");
		assertExpectedRevision(channel, params.expectedRevision);
		const members = params.members ?? channel.members;
		if (
			members.length < 1 ||
			members.length > 20 ||
			new Set(members.map((m) => m.roleId)).size !== members.length
		)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Invalid channel membership.");
		if (channel.status !== "open")
			throw new CoordinationPolicyError("INVALID_TRANSITION", "Closed channels cannot be changed.");
		for (const member of members)
			if (!projection.roles[member.roleId])
				throw new CoordinationPolicyError("UNKNOWN_ROLE", "Channel member is not admitted.");
		if (
			channel.review &&
			!members.some((m) => m.roleId === channel.review!.reviewerRoleId && m.mode === "participate")
		)
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"A review channel must retain its participating reviewer.",
			);
		for (const id of channel.review?.taskIds ?? [])
			if (
				projection.tasks[id] &&
				!members.some((m) => m.roleId === projection.tasks[id].owner && m.mode === "participate")
			)
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"Review channels must retain participating source owners.",
				);
		const next: ChannelState = {
			...channel,
			members: structuredClone(members),
			delivery: params.delivery ?? channel.delivery,
			revision: channel.revision + 1,
		};
		return this.log.append(
			"channel.membership_changed",
			params.actor,
			next,
			params.idempotencyKey,
			{ channelId: channel.id },
		);
	}
	closeChannel(params: {
		actor: CoordinationActor;
		channelId: string;
		expectedRevision: number;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		requireCapability(p, params.actor, "coordinator");
		const c = p.channels[params.channelId];
		if (!c) throw new CoordinationPolicyError("STALE_REVISION", "Unknown channel.");
		assertExpectedRevision(c, params.expectedRevision);
		return this.log.append(
			"channel.closed",
			params.actor,
			{ id: c.id, revision: c.revision + 1 },
			params.idempotencyKey,
			{ channelId: c.id },
		);
	}
	sendChannel(params: {
		actor: CoordinationActor;
		channelId: string;
		expectedRevision: number;
		text: string;
		delivery?: ChannelState["delivery"];
		idempotencyKey: string;
	}) {
		const p = this.log.stateSnapshot();
		this.authenticate(params.actor);
		const channel = p.channels[params.channelId];
		if (channel?.status !== "open")
			throw new CoordinationPolicyError("INVALID_TRANSITION", "Channel is missing or closed.");
		assertExpectedRevision(channel, params.expectedRevision);
		const membership = channel.members.find((member) => member.roleId === params.actor.roleId);
		if (membership?.mode !== "participate") requireCapability(p, params.actor, "coordinator");
		if (typeof params.text !== "string" || !params.text.trim() || params.text.length > 8000)
			throw new CoordinationPolicyError(
				"BOUNDS_EXCEEDED",
				"Channel text must be between 1 and 8000 characters.",
			);
		if (
			Object.keys(p.signals).length >= 1000 ||
			Object.keys(p.deliveries).length + channel.members.length > 20000
		)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Signal or delivery limit reached.");
		const deliveryMode =
			params.delivery ?? (channel.review?.reviewTaskId ? "urgent" : channel.delivery);
		const signal: SignalStateRecord = {
			id: stableId("channel-message", params.idempotencyKey, params.actor),
			kind: "attention",
			state: "open",
			severity: deliveryMode === "urgent" ? "action" : "info",
			summary: params.text.slice(0, 500),
			details: params.text,
			targets: [{ channelId: channel.id }],
			artifactRefs: [],
			requiresAck: [],
			acknowledgements: [],
			revision: 1,
		};
		const recipients = channel.members.filter((member) => member.roleId !== params.actor.roleId);
		const deliveries = recipients.map((member) => {
			const queued = queueDelivery(
				member.roleId,
				signal.revision,
				{ channelId: channel.id, signalId: signal.id },
				stableId("delivery", `${signal.id}:${member.roleId}`),
			);
			return deliveryMode === "urgent" &&
				!(
					channel.review?.reviewTaskId &&
					(member.mode !== "participate" ||
						p.roles[member.roleId]?.capabilities.includes("coordinator"))
				)
				? recordWake(queued)
				: queued;
		});
		const result = this.log.append(
			"channel.message",
			params.actor,
			{
				id: channel.id,
				revision: channel.revision,
				summary: params.text,
				targets: signal.targets,
				signal,
				deliveries,
				queued: recipients.map((member) => member.roleId),
				offline: recipients
					.filter((member) => !p.roles[member.roleId]?.active)
					.map((member) => member.roleId),
			},
			params.idempotencyKey,
			{ channelId: channel.id },
		);
		for (const delivery of deliveries)
			if (delivery.wakeCount) {
				try {
					this.deliverySink?.(delivery, signal);
				} catch {
					/* The durable queue remains available for reconnect. */
				}
			}
		return result;
	}
	validateSpawnAssignment(params: {
		actor: CoordinationActor;
		owner: string;
		id?: string;
		requiredInputs?: ArtifactRef[];
	}) {
		this.authenticate(params.actor, "coordinator");
		validateMutationRequest("createTask", {
			...normalizeRoleReferences(params),
			title: "Spawn assignment",
			idempotencyKey: "spawn-preflight",
		});
		const p = this.log.stateSnapshot(),
			owner = normalizeCoordinationRoleId(params.owner);
		const assignmentId =
			params.id ??
			Object.values(p.channels).find(
				(c) => c.review?.reviewTaskId && c.review.reviewerRoleId === owner,
			)?.review?.reviewTaskId;
		const existing = assignmentId
			? p.tasks[assignmentId]
			: Object.values(p.tasks).find(
					(task) =>
						task.owner === owner && !["completed", "failed", "cancelled"].includes(task.state),
				);
		if (
			existing &&
			(existing.owner !== owner || ["completed", "failed", "cancelled"].includes(existing.state))
		)
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Assignment ID belongs to another owner or a terminal task.",
			);
		if (
			existing &&
			params.requiredInputs !== undefined &&
			JSON.stringify(existing.requiredInputs) !== JSON.stringify(params.requiredInputs)
		)
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Existing assignment has different inputs. Bind them before starting; spawn never silently changes an active contract.",
			);
		for (const channel of Object.values(p.channels)) {
			if (
				params.id &&
				channel.review?.taskIds.includes(params.id) &&
				!channel.members.some((m) => m.roleId === owner && m.mode === "participate")
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Assignment owner must participate in its review channel.",
				);
			if (
				channel.review?.reviewTaskId &&
				((params.id === channel.review.reviewTaskId && owner !== channel.review.reviewerRoleId) ||
					(owner === channel.review.reviewerRoleId &&
						params.id !== undefined &&
						params.id !== channel.review.reviewTaskId))
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Peer reviewer launch must use its declared review assignment.",
				);
		}
		const inputs = params.requiredInputs ?? existing?.requiredInputs ?? [];
		if (inputs.length > 100)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Too many assignment inputs.");
		for (const ref of inputs) {
			const latest = p.artifacts[ref.artifactId]?.at(-1);
			if (
				latest?.state !== "frozen" ||
				latest.version !== ref.version ||
				latest.digest !== ref.digest
			)
				throw new CoordinationPolicyError(
					"INPUT_NOT_FROZEN",
					"Spawn requires exact current frozen inputs.",
				);
			requireArtifactAccess(p, owner, latest);
			verifyArtifactBytes(latest);
		}
		return existing;
	}
	assignDefaultTask(params: {
		actor: CoordinationActor;
		owner: string;
		title: string;
		id?: string;
		requiredInputs?: ArtifactRef[];
		idempotencyKey: string;
	}) {
		const peer = Object.values(this.log.stateSnapshot().channels).find(
			(c) =>
				c.review?.reviewTaskId &&
				c.review.reviewerRoleId === normalizeCoordinationRoleId(params.owner),
		);
		const id =
			params.id ??
			peer?.review?.reviewTaskId ??
			stableId("task", params.idempotencyKey, params.actor);
		this.validateSpawnAssignment({ ...params, id });
		return this.createTask({ ...params, id, autoStart: true });
	}
	createTask(params: {
		actor: CoordinationActor;
		id?: string;
		title: string;
		owner: string;
		dependsOn?: string[];
		requiredInputs?: ArtifactRef[];
		requiredAcknowledgements?: string[];
		leases?: string[];
		verification?: string[];
		nextActions?: string[];
		blockers?: string[];
		start?: boolean;
		autoStart?: boolean;
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		requireCapability(projection, params.actor, "coordinator");
		if (
			(params.dependsOn?.length ?? 0) > 100 ||
			(params.requiredInputs?.length ?? 0) > 100 ||
			(params.requiredAcknowledgements?.length ?? 0) > 100
		)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Task references exceed bounds.");
		if (!projection.roles[params.owner])
			throw new CoordinationPolicyError("UNKNOWN_ROLE", "Task owner is not admitted.");
		if (Object.keys(projection.tasks).length >= 100)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Task limit reached.");
		const task: TaskStateRecord = {
			id: params.id ?? stableId("task", params.idempotencyKey, params.actor),
			title: required(params.title, "title").slice(0, 200),
			owner: params.owner,
			activeRunId: projection.roles[params.owner]?.runId,
			state: "draft",
			revision: 1,
			dependsOn: params.dependsOn ?? [],
			blockers: params.blockers ?? [],
			requiredInputs: params.requiredInputs ?? [],
			outputs: [],
			requiredAcknowledgements: params.requiredAcknowledgements ?? [],
			leases: params.leases ?? [],
			reviewSatisfied: false,
			verification: params.verification ?? [],
			nextActions: params.nextActions ?? [],
			permits: [],
		};
		for (const channel of Object.values(projection.channels))
			if (
				channel.review?.taskIds.includes(task.id) &&
				!channel.members.some((m) => m.roleId === task.owner && m.mode === "participate")
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Task owner must participate in its review channel.",
				);
		if (Object.hasOwn(projection.tasks, task.id))
			throw new CoordinationPolicyError("INVALID_TRANSITION", "Task ID already exists.");
		if (
			Object.values(projection.channels).some(
				(c) => c.review?.taskIds.includes(task.id) && c.review.reviewerRoleId === task.owner,
			)
		)
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"Source ownership must be independent of its reviewer.",
			);
		const peerReview = peerReviewChannel(projection, task.id);
		if (peerReview && task.owner !== peerReview.review!.reviewerRoleId)
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"Peer review task must belong to its designated reviewer.",
			);
		if (peerReview && params.start)
			throw new CoordinationPolicyError(
				"UNRESOLVED_DEPENDENCY",
				"Peer review assignments start automatically at GO; create draft or use spawn.",
			);
		if (!peerReview && (params.start || (params.autoStart && this.workflowMode === "fast"))) {
			if (!projection.roles[task.owner].active)
				throw new CoordinationPolicyError("STALE_GENERATION", "Starting requires an active owner.");
			try {
				assertTaskCanStart(projection, task, this.workflowMode === "strict");
				task.state = "in_progress";
			} catch (error) {
				// Spawn must still launch a worker that has a pending role/channel gate.
				// Explicit start remains all-or-nothing; auto-start records a draft.
				if (
					params.start ||
					!params.autoStart ||
					!(error instanceof CoordinationPolicyError) ||
					![
						"UNRESOLVED_DEPENDENCY",
						"UNRESOLVED_BLOCKER",
						"ACK_REQUIRED",
						"INPUT_NOT_FROZEN",
						"INPUT_SUPERSEDED",
					].includes(error.code)
				)
					throw error;
			}
		}
		return this.log.append("task.created", params.actor, task, params.idempotencyKey, {
			taskId: task.id,
		});
	}
	transitionTask(params: {
		actor: CoordinationActor;
		taskId: string;
		expectedRevision: number;
		state: TaskStateRecord["state"];
		outputs?: ArtifactRef[];
		verification?: string[];
		reviewSatisfied?: boolean;
		nextActions?: string[];
		leases?: string[];
		blockers?: string[];
		reason?: string;
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		this.authenticate(params.actor);
		const task = projection.tasks[params.taskId];
		if (!task) throw new CoordinationPolicyError("STALE_REVISION", "Unknown task.");
		assertExpectedRevision(task, params.expectedRevision);
		if (task.owner !== params.actor.roleId)
			requireCapability(projection, params.actor, "coordinator");
		else if (task.activeRunId !== params.actor.runId)
			throw new CoordinationPolicyError(
				"STALE_GENERATION",
				"Task responsibility remains with a different attempt generation.",
			);
		if (params.reviewSatisfied === true) {
			requireCapability(projection, params.actor, "reviewer");
			if (
				task.owner === params.actor.roleId &&
				!projection.roles[params.actor.roleId].capabilities.includes("coordinator")
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"A reviewer cannot set approval on its own assignment. Submit for independent review.",
				);
		}
		if (["ready", "in_progress", "submitted", "review", "completed"].includes(params.state))
			assertPeerReviewReady(projection, task);
		const peer = peerReviewChannel(projection, task.id);
		if (
			peer &&
			["submitted", "review", "completed"].includes(params.state) &&
			peer.review!.taskIds.some((id) => projection.tasks[id]?.state !== "completed")
		)
			throw new CoordinationPolicyError(
				"UNRESOLVED_DEPENDENCY",
				"Approve all source tasks before submitting the peer review report.",
			);
		const reviewInputsChanged =
			(params.outputs !== undefined &&
				JSON.stringify(params.outputs) !== JSON.stringify(task.outputs)) ||
			(params.verification !== undefined &&
				JSON.stringify(params.verification) !== JSON.stringify(task.verification));
		if (params.blockers && task.blockers.some((id) => !params.blockers!.includes(id)))
			requireCapability(projection, params.actor, "reviewer");
		const directStart =
			this.workflowMode === "fast" &&
			["draft", "blocked"].includes(task.state) &&
			params.state === "in_progress";
		if (!directStart && !TASK_TRANSITIONS[task.state].includes(params.state)) {
			const correctionHint =
				this.workflowMode === "fast" && task.state === "blocked" && params.state === "submitted"
					? " Call subagent_task start with the current expectedRevision first; start rechecks holds/dependencies. Then submit using the returned revision. No ready step is needed."
					: "";
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				`Cannot transition ${task.state} to ${params.state}.${correctionHint}`,
				task,
			);
		}
		if (
			(params.outputs?.length ?? 0) > 100 ||
			(params.verification?.length ?? 0) > 100 ||
			(params.nextActions?.length ?? 0) > 100 ||
			(params.leases?.length ?? 0) > 100 ||
			(params.blockers?.length ?? 0) > 100
		)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Task transition fields exceed bounds.");
		const next: TaskStateRecord = {
			...task,
			state: params.state,
			revision: task.revision + 1,
			outputs: params.outputs ?? task.outputs,
			verification: (params.verification ?? task.verification).map((v) => v.slice(0, 1000)),
			reviewSatisfied:
				params.reviewSatisfied ?? (reviewInputsChanged ? false : task.reviewSatisfied),
			nextActions: (params.nextActions ?? task.nextActions).map((v) => v.slice(0, 1000)),
			leases: params.leases ?? task.leases,
			blockers: params.blockers ?? task.blockers,
			...(params.reason ? { transitionReason: params.reason } : {}),
		};
		if (params.state === "ready" || params.state === "in_progress") {
			try {
				assertTaskCanStart(
					projection,
					next,
					params.state === "in_progress" && this.workflowMode === "strict",
				);
			} catch (error) {
				// The candidate revision has not been committed. Report persisted state.
				if (error instanceof CoordinationPolicyError && (error.current as any)?.id === task.id)
					error.current = task;
				throw error;
			}
		}
		const peerSource = Object.values(projection.channels).some(
			(c) => c.review?.reviewTaskId && c.review.taskIds.includes(task.id),
		);
		if ((peer || peerSource) && ["submitted", "review"].includes(params.state)) {
			assertTaskCanStart(projection, next, false);
			// Validate report evidence, not approval; the persisted approval flag stays false.
			assertTaskCanComplete(projection, { ...next, reviewSatisfied: true });
			for (const ref of [...next.requiredInputs, ...next.outputs])
				verifyArtifactBytes(
					projection.artifacts[ref.artifactId].find((a) => a.version === ref.version)!,
				);
		}
		if (params.state === "completed") {
			if (params.nextActions === undefined)
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"Completion requires explicit current next actions (an empty list is allowed).",
					task,
				);
			assertTaskCanComplete(projection, next);
			for (const output of next.outputs)
				verifyArtifactBytes(
					projection.artifacts[output.artifactId].find(
						(artifact) => artifact.version === output.version,
					)!,
				);
		}
		return this.log.append("task.transitioned", params.actor, next, params.idempotencyKey, {
			taskId: task.id,
		});
	}
	handoffTask(params: {
		actor: CoordinationActor;
		taskId: string;
		expectedRevision: number;
		owner: string;
		reason: string;
		nextActions: string[];
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		const task = p.tasks[params.taskId];
		if (!task) throw new CoordinationPolicyError("STALE_REVISION", "Unknown task.");
		assertExpectedRevision(task, params.expectedRevision);
		if (task.owner !== params.actor.roleId) requireCapability(p, params.actor, "coordinator");
		else if (task.activeRunId !== params.actor.runId)
			throw new CoordinationPolicyError("STALE_GENERATION", "Task belongs to a different run.");
		if (["completed", "failed", "cancelled"].includes(task.state) || task.owner === params.owner)
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Handoff requires a non-terminal task and a different owner.",
				task,
			);
		const owner = p.roles[params.owner];
		if (!owner?.active || !owner.runId)
			throw new CoordinationPolicyError("UNKNOWN_ROLE", "Handoff requires an active target role.");
		if (peerReviewChannel(p, task.id))
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Rebind the stable reviewer role instead of handing off its declared review assignment.",
			);
		for (const channel of Object.values(p.channels))
			if (
				channel.review?.taskIds.includes(task.id) &&
				(channel.review.reviewerRoleId === owner.roleId ||
					!channel.members.some((m) => m.roleId === owner.roleId && m.mode === "participate"))
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Handoff owner must participate and be independent of the reviewer.",
				);
		if (
			task.permits.some((id) =>
				["reserved", "dispatched", "outcome_unknown"].includes(p.permits[id]?.state),
			)
		)
			throw new CoordinationPolicyError(
				"PERMIT_NOT_USABLE",
				"Reconcile or cancel outstanding attempts before handoff.",
			);
		for (const ref of [...task.requiredInputs, ...task.outputs]) {
			const artifact = p.artifacts[ref.artifactId]?.find(
				(a) => a.version === ref.version && a.digest === ref.digest,
			);
			if (!artifact)
				throw new CoordinationPolicyError("INPUT_SUPERSEDED", "Handoff artifact is missing.");
			requireArtifactAccess(p, owner.roleId, artifact);
		}
		const next: TaskStateRecord = {
			...task,
			owner: owner.roleId,
			activeRunId: owner.runId,
			state: "draft",
			revision: task.revision + 1,
			reviewSatisfied: false,
			nextActions: params.nextActions,
		};
		return this.log.append(
			"task.handed_off",
			params.actor,
			{ ...next, transitionReason: params.reason },
			params.idempotencyKey,
			{ taskId: task.id },
		);
	}
	reviewTask(params: {
		actor: CoordinationActor;
		taskId: string;
		expectedRevision: number;
		decision: "approved" | "changes_requested";
		reason: string;
		nextActions?: string[];
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		requireCapability(p, params.actor, "reviewer");
		const task = p.tasks[params.taskId];
		if (!task) throw new CoordinationPolicyError("STALE_REVISION", "Unknown review task.");
		if (
			task.owner === params.actor.roleId &&
			!p.roles[params.actor.roleId].capabilities.includes("coordinator")
		)
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"A reviewer cannot approve its own assignment; hand the report to the coordinator.",
			);
		assertExpectedRevision(task, params.expectedRevision);
		if (!["submitted", "review"].includes(task.state))
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Only submitted tasks can be reviewed.",
			);
		const view = redactProjection(p, params.actor.roleId);
		if (
			task.outputs.some(
				(output) =>
					!view.artifacts[output.artifactId]?.some(
						(artifact) => artifact.version === output.version && artifact.digest === output.digest,
					),
			)
		)
			throw new CoordinationPolicyError("UNAUTHORIZED", "Reviewer cannot access a private output.");
		const group = Object.values(p.channels).find(
			(c) => c.review?.reviewTaskId && c.review.taskIds.includes(task.id),
		);
		if (group && !p.roles[params.actor.roleId].capabilities.includes("coordinator")) {
			if (group.review!.reviewerRoleId !== params.actor.roleId)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Only the designated peer reviewer may decide this task.",
				);
			const assignment = p.tasks[group.review!.reviewTaskId!];
			if (
				!assignment ||
				!["in_progress", "blocked"].includes(assignment.state) ||
				!assignment.reviewSourceInputs?.length
			)
				throw new CoordinationPolicyError(
					"UNRESOLVED_DEPENDENCY",
					"Wait for automatic review activation.",
				);
			if (params.decision === "approved") assertPeerReviewReady(p, assignment);
			else if (
				task.outputs.some(
					(ref) =>
						!assignment.reviewSourceInputs!.some((r) => JSON.stringify(r) === JSON.stringify(ref)),
				)
			)
				throw new CoordinationPolicyError(
					"INPUT_SUPERSEDED",
					"Findings must reference the activated review inputs.",
				);
			if (
				params.decision === "changes_requested" &&
				(task.reviewCorrections ?? 0) >= (group.review!.maxCorrections ?? 2)
			)
				throw new CoordinationPolicyError(
					"BOUNDS_EXCEEDED",
					"Peer correction limit reached; escalate to coordinator rather than looping.",
				);
		}
		const approved = params.decision === "approved";
		const next: TaskStateRecord = {
			...task,
			state: approved ? (this.workflowMode === "fast" ? "completed" : "review") : "blocked",
			reviewSatisfied: approved,
			revision: task.revision + 1,
			nextActions: params.nextActions ?? (approved ? task.nextActions : [params.reason]),
		};
		if (group && !approved) next.reviewCorrections = (task.reviewCorrections ?? 0) + 1;
		if (approved) {
			if (this.workflowMode === "fast" && params.nextActions === undefined)
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"Fast approval completes the task: supply explicit nextActions (an empty list is allowed).",
					task,
				);
			if (this.workflowMode === "fast") assertTaskCanStart(p, next, false);
			assertTaskCanComplete(p, next);
			for (const output of next.outputs)
				verifyArtifactBytes(
					p.artifacts[output.artifactId].find((artifact) => artifact.version === output.version)!,
				);
		}
		const channel = Object.values(p.channels).find((c) => c.review?.taskIds.includes(task.id));
		if (
			channel &&
			(channel.status !== "open" ||
				!channel.members.some((m) => m.roleId === params.actor.roleId && m.mode === "participate"))
		)
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"Review requires its open peer handoff channel and participating reviewer.",
			);
		const notice = channel
			? reviewMessage(channel, next, params.actor, params.reason, approved, params.idempotencyKey)
			: undefined;
		if (
			notice &&
			(Object.keys(p.signals).length >= 1000 ||
				Object.keys(p.deliveries).length + notice.deliveries.length > 20000)
		)
			throw new CoordinationPolicyError(
				"BOUNDS_EXCEEDED",
				"Review handoff delivery capacity exhausted.",
			);
		const result = this.log.append(
			"task.transitioned",
			params.actor,
			{ ...next, reviewReason: params.reason, ...(notice ? { reviewMessage: notice } : {}) },
			params.idempotencyKey,
			{ taskId: task.id },
		);
		for (const d of notice?.deliveries ?? [])
			if (d.wakeCount)
				try {
					this.deliverySink?.(d, notice!.signal);
				} catch {
					/* Retry from durable queue. */
				}
		return result;
	}
	bindTaskInputs(params: {
		actor: CoordinationActor;
		taskId: string;
		expectedRevision: number;
		requiredInputs: ArtifactRef[];
		leases?: string[];
		reason: string;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		requireCapability(p, params.actor, "coordinator");
		const task = p.tasks[params.taskId];
		if (!task) throw new CoordinationPolicyError("STALE_REVISION", "Unknown task.");
		assertExpectedRevision(task, params.expectedRevision);
		if (
			!["draft", "ready", "blocked"].includes(task.state) ||
			task.permits.some((id) => ["dispatched", "outcome_unknown"].includes(p.permits[id]?.state))
		)
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Inputs cannot be rebound during an active or unknown attempt.",
			);
		for (const ref of params.requiredInputs) {
			const artifact = p.artifacts[ref.artifactId]?.at(-1);
			if (
				!artifact ||
				artifact.version !== ref.version ||
				artifact.digest !== ref.digest ||
				artifact.state !== "frozen"
			)
				throw new CoordinationPolicyError("INPUT_NOT_FROZEN", "Bind exact current frozen inputs.");
			verifyArtifactBytes(artifact);
		}
		return this.log.append(
			"task.inputs_bound",
			params.actor,
			{
				...task,
				state: "draft",
				revision: task.revision + 1,
				requiredInputs: params.requiredInputs,
				leases: params.leases ?? task.leases,
				reviewSatisfied: false,
				inputBindingReason: params.reason,
			},
			params.idempotencyKey,
			{ taskId: task.id },
		);
	}
	supersedeSignal(params: {
		actor: CoordinationActor;
		signalId: string;
		expectedRevision: number;
		summary: string;
		targets?: SignalStateRecord["targets"];
		requiresAck?: string[];
		kind?: SignalStateRecord["kind"];
		severity?: SignalStateRecord["severity"];
		artifactRefs?: ArtifactRef[];
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		requireCapability(p, params.actor, "reviewer");
		const signal = p.signals[params.signalId];
		if (!signal) throw new CoordinationPolicyError("STALE_REVISION", "Unknown signal.");
		this.signalRecipients({ actor: params.actor, signalId: signal.id });
		assertExpectedRevision(signal, params.expectedRevision);
		if (signal.state === "superseded")
			throw new CoordinationPolicyError("INVALID_TRANSITION", "Signal already superseded.");
		if (Object.keys(p.signals).length >= 1000)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Signal limit reached.");
		const successor: SignalStateRecord = {
			...signal,
			id: stableId("signal", params.idempotencyKey, params.actor),
			summary: params.summary,
			targets: params.targets ?? signal.targets,
			requiresAck: params.requiresAck ?? signal.requiresAck,
			kind: params.kind ?? signal.kind,
			severity: params.severity ?? signal.severity,
			artifactRefs: params.artifactRefs ?? signal.artifactRefs,
			state: "open",
			revision: 1,
			acknowledgements: [],
			resolvedBy: undefined,
			supersedes: signal.id,
		};
		this.validateSignalScope(p, params.actor, successor);
		const affectedTasks = Object.values(p.tasks)
			.filter(
				(task) =>
					!["completed", "failed", "cancelled"].includes(task.state) &&
					(task.blockers.includes(signal.id) || task.dependsOn.includes(signal.id)),
			)
			.map((task) => ({
				...task,
				state: "blocked" as const,
				revision: task.revision + 1,
				reviewSatisfied: false,
				blockers: task.blockers.map((id) => (id === signal.id ? successor.id : id)),
				dependsOn: task.dependsOn.map((id) => (id === signal.id ? successor.id : id)),
			}));
		return this.log.append(
			"signal.superseded",
			params.actor,
			{
				...successor,
				previous: { ...signal, state: "superseded", revision: signal.revision + 1 },
				affectedTasks,
			},
			params.idempotencyKey,
		);
	}
	emitSignal(params: {
		actor: CoordinationActor;
		kind: SignalStateRecord["kind"];
		severity: SignalStateRecord["severity"];
		summary: string;
		targets?: SignalStateRecord["targets"];
		requiresAck?: string[];
		artifactRefs?: ArtifactRef[];
		idempotencyKey: string;
	}) {
		const projection = this.log.stateSnapshot();
		this.authenticate(params.actor);
		if (
			(params.targets?.length ?? 0) > 100 ||
			(params.requiresAck?.length ?? 0) > 100 ||
			(params.artifactRefs?.length ?? 0) > 100
		)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Signal references exceed bounds.");
		if (["go", "approved"].includes(params.kind))
			requireCapability(projection, params.actor, "reviewer");
		if (Object.keys(projection.signals).length >= 1000)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Signal limit reached.");
		this.validateSignalScope(projection, params.actor, params);
		const signal: SignalStateRecord = {
			id: stableId("signal", params.idempotencyKey, params.actor),
			kind: params.kind,
			state: "open",
			severity: params.severity,
			summary: required(params.summary, "summary").slice(0, 500),
			targets: params.targets ?? [],
			artifactRefs: params.artifactRefs ?? [],
			requiresAck: params.requiresAck ?? [],
			acknowledgements: [],
			revision: 1,
		};
		if (Object.hasOwn(projection.signals, signal.id))
			throw new CoordinationPolicyError(
				"IDEMPOTENCY_CONFLICT",
				"Signal already exists outside the retained retry window; query its current state.",
			);
		return this.log.append("signal.emitted", params.actor, signal, params.idempotencyKey);
	}
	private validateSignalScope(
		projection: CoordinationProjection,
		actor: CoordinationActor,
		params: {
			targets?: SignalStateRecord["targets"];
			requiresAck?: string[];
			artifactRefs?: ArtifactRef[];
		},
	) {
		const coordinator = projection.roles[actor.roleId].capabilities.includes("coordinator");
		for (const target of params.targets ?? []) {
			if (
				target.roleId &&
				(!Object.hasOwn(projection.roles, target.roleId) ||
					(projection.roles[target.roleId].private &&
						target.roleId !== actor.roleId &&
						!coordinator))
			)
				throw new CoordinationPolicyError("UNKNOWN_ROLE", "Unknown or private signal recipient.");
			if (
				target.channelId &&
				(!projection.channels[target.channelId] ||
					(!coordinator &&
						!projection.channels[target.channelId].members.some(
							(member) => member.roleId === actor.roleId,
						)))
			)
				throw new CoordinationPolicyError("UNAUTHORIZED", "Signal sender is not a channel member.");
			if (
				target.taskId &&
				(!projection.tasks[target.taskId] ||
					(!coordinator &&
						projection.tasks[target.taskId].owner !== actor.roleId &&
						!projection.roles[actor.roleId].capabilities.includes("reviewer")))
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Signal sender cannot control this task.",
				);
		}
		if ((params.requiresAck ?? []).some((roleId) => !Object.hasOwn(projection.roles, roleId)))
			throw new CoordinationPolicyError("UNKNOWN_ROLE", "Acknowledgement role is not admitted.");
		for (const ref of params.artifactRefs ?? []) {
			const artifact = projection.artifacts[ref.artifactId]?.find(
				(candidate) => candidate.version === ref.version && candidate.digest === ref.digest,
			);
			if (!artifact)
				throw new CoordinationPolicyError("INPUT_SUPERSEDED", "Unknown signal artifact reference.");
			requireArtifactAccess(projection, actor.roleId, artifact);
		}
	}
	acknowledgeSignal(params: {
		actor: CoordinationActor;
		signalId: string;
		expectedRevision: number;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		this.authenticate(params.actor);
		const s = p.signals[params.signalId];
		if (!s) throw new CoordinationPolicyError("STALE_REVISION", "Unknown signal.");
		assertExpectedRevision(s, params.expectedRevision);
		if (s.state === "superseded")
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"A superseded signal cannot be acknowledged.",
			);
		if (!s.requiresAck.includes(params.actor.roleId))
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"Signal does not require this role acknowledgement.",
			);
		if (
			s.acknowledgements.some(
				(a) => a.roleId === params.actor.roleId && a.generation === params.actor.generation,
			)
		)
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Signal already acknowledged by this generation.",
			);
		const next = {
			...s,
			acknowledgements: [...s.acknowledgements, { ...params.actor, at: Date.now() }],
			revision: s.revision + 1,
			deliveries: Object.values(p.deliveries)
				.filter(
					(delivery) =>
						delivery.signalId === s.id &&
						delivery.targetRoleId === params.actor.roleId &&
						delivery.state === "delivered",
				)
				.map(markAcknowledged),
		};
		return this.log.append("signal.acknowledged", params.actor, next, params.idempotencyKey);
	}
	resolveSignal(params: {
		actor: CoordinationActor;
		signalId: string;
		expectedRevision: number;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		const ownSignal =
			this.workflowMode === "fast" &&
			p.audit.some(
				(e) =>
					["signal.emitted", "signal.superseded"].includes(e.type) &&
					e.actor.roleId === params.actor.roleId &&
					(e.payload as SignalStateRecord).id === params.signalId,
			);
		if (!ownSignal) requireCapability(p, params.actor, "reviewer");
		const s = p.signals[params.signalId];
		if (!s) throw new CoordinationPolicyError("STALE_REVISION", "Unknown signal.");
		this.signalRecipients({ actor: params.actor, signalId: s.id });
		assertExpectedRevision(s, params.expectedRevision);
		if (s.state !== "open")
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Only an open signal can be resolved.",
			);
		if (
			s.requiresAck.some(
				(role) =>
					!s.acknowledgements.some(
						(a) => a.roleId === role && p.roles[role]?.generation === a.generation,
					),
			)
		)
			throw new CoordinationPolicyError(
				"ACK_REQUIRED",
				"Required acknowledgements are incomplete.",
			);
		return this.log.append(
			"signal.resolved",
			params.actor,
			{
				...s,
				state: "resolved",
				resolvedBy: params.actor.roleId,
				revision: s.revision + 1,
				deliveries: Object.values(p.deliveries)
					.filter((delivery) => delivery.signalId === s.id && delivery.state !== "queued")
					.map(markResolved),
			},
			params.idempotencyKey,
		);
	}
	createBudget(params: {
		actor: CoordinationActor;
		budgetId: string;
		limit: number;
		idempotencyKey: string;
	}) {
		this.authenticate(params.actor, "coordinator");
		if (Object.keys(this.log.snapshot().budgets).length >= 100)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Budget limit reached.");
		if (Object.hasOwn(this.log.snapshot().budgets, params.budgetId))
			throw new CoordinationPolicyError("INVALID_TRANSITION", "Budget ID already exists.");
		return this.log.append(
			"budget.created",
			params.actor,
			createBudget(params.budgetId, params.limit),
			params.idempotencyKey,
		);
	}
	reserveBudget(params: {
		actor: CoordinationActor;
		budgetId: string;
		amount?: number;
		taskId?: string;
		expectedRevision?: number;
		idempotencyKey: string;
	}) {
		this.authenticate(params.actor, "coordinator");
		const p = this.log.stateSnapshot(),
			b = p.budgets[params.budgetId];
		if (!b) throw new CoordinationPolicyError("LEASE_REQUIRED", "Unknown budget.");
		const reservationId = stableId("reservation", params.idempotencyKey, params.actor);
		let task: TaskStateRecord | undefined;
		if (params.taskId) {
			const current = p.tasks[params.taskId];
			if (!current) throw new CoordinationPolicyError("STALE_REVISION", "Unknown task.");
			assertExpectedRevision(current, params.expectedRevision);
			if (["completed", "failed", "cancelled"].includes(current.state))
				throw new CoordinationPolicyError(
					"INVALID_TRANSITION",
					"Cannot attach a lease to a terminal task.",
					current,
				);
			task = {
				...current,
				revision: current.revision + 1,
				leases: [...current.leases, reservationId],
			};
		} else if (params.expectedRevision !== undefined)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "expectedRevision requires taskId.");
		return this.log.append(
			"budget.reserved",
			params.actor,
			{ ...reserveBudget(b, params.amount, reservationId), ...(task ? { task } : {}) },
			params.idempotencyKey,
			task ? { taskId: task.id } : {},
		);
	}
	releaseBudget(params: {
		actor: CoordinationActor;
		budgetId: string;
		reservationId: string;
		idempotencyKey: string;
	}) {
		this.authenticate(params.actor, "coordinator");
		const b = this.log.snapshot().budgets[params.budgetId];
		if (!b) throw new CoordinationPolicyError("LEASE_REQUIRED", "Unknown budget.");
		return this.log.append(
			"budget.released",
			params.actor,
			releaseBudget(b, params.reservationId),
			params.idempotencyKey,
		);
	}
	preparePermit(params: {
		actor: CoordinationActor;
		taskId: string;
		expectedRevision: number;
		budgetId: string;
		operation: string;
		idempotencyKey: string;
	}) {
		const p = this.log.stateSnapshot();
		const task = p.tasks[params.taskId],
			budget = p.budgets[params.budgetId];
		if (!task) throw new CoordinationPolicyError("STALE_REVISION", "Unknown task.");
		assertExpectedRevision(task, params.expectedRevision);
		if (!budget) throw new CoordinationPolicyError("LEASE_REQUIRED", "Unknown budget.");
		const assigned = new Set(
			Object.values(p.permits)
				.filter((permit) => permit.budgetId === budget.id)
				.map((permit) => permit.reservationId),
		);
		const unused = task.leases.find(
			(id) => budget.reservations[id]?.state === "reserved" && !assigned.has(id),
		);
		// Reuse granted capacity first. Fast mode can replenish an assigned bounded pool.
		if (
			!unused &&
			(this.workflowMode !== "fast" ||
				!task.leases.some((id) => Object.hasOwn(budget.reservations, id)))
		)
			requireCapability(p, params.actor, "coordinator");
		if (Object.keys(p.permits).length >= 1000 || (!unused && task.leases.length >= 100))
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Attempt or task lease limit reached.");
		const reservationId = unused ?? stableId("reservation", params.idempotencyKey, params.actor);
		const nextBudget = unused ? budget : reserveBudget(budget, 1, reservationId);
		const nextTask = unused
			? task
			: { ...task, revision: task.revision + 1, leases: [...task.leases, reservationId] };
		p.budgets[budget.id] = nextBudget;
		p.tasks[task.id] = nextTask;
		const permit = issuePermit({
			projection: p,
			actor: params.actor,
			task: nextTask,
			budgetId: budget.id,
			reservationId,
			operation: params.operation,
			idempotencyKey: params.idempotencyKey,
			permitId: stableId("permit", params.idempotencyKey, params.actor),
			ttlMs: this.options.permitTtlMs,
		});
		return this.log.append(
			"permit.issued",
			params.actor,
			{ ...permit, budget: nextBudget, task: nextTask },
			params.idempotencyKey,
			{ taskId: task.id },
		);
	}
	issuePermit(params: {
		actor: CoordinationActor;
		taskId: string;
		budgetId: string;
		reservationId: string;
		operation: string;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		this.authenticate(params.actor);
		const t = p.tasks[params.taskId];
		if (!t) throw new CoordinationPolicyError("STALE_REVISION", "Unknown task.");
		if (Object.keys(p.permits).length >= 1000)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Permit limit reached.");
		const permit = issuePermit({
			projection: p,
			actor: params.actor,
			task: t,
			budgetId: params.budgetId,
			reservationId: params.reservationId,
			operation: required(params.operation, "operation"),
			idempotencyKey: params.idempotencyKey,
			permitId: stableId("permit", params.idempotencyKey, params.actor),
			ttlMs: this.options.permitTtlMs,
		});
		return this.log.append("permit.issued", params.actor, permit, params.idempotencyKey, {
			taskId: t.id,
		});
	}
	dispatchPermit(params: {
		actor: CoordinationActor;
		permitId: string;
		idempotencyKey: string;
		operation?: string;
		outcomeUnknown?: boolean;
	}) {
		const p = this.log.snapshot();
		this.authenticate(params.actor);
		const permit = p.permits[params.permitId];
		if (!permit) throw new CoordinationPolicyError("PERMIT_NOT_USABLE", "Unknown permit.");
		if (params.operation !== undefined && params.operation !== permit.operation)
			throw new CoordinationPolicyError(
				"PERMIT_NOT_USABLE",
				"Permit operation does not match the protected attempt.",
			);
		const dispatched = dispatchPermit(permit, params.actor);
		const task = p.tasks[permit.taskId];
		if (!task || task.revision !== permit.taskRevision)
			throw new CoordinationPolicyError(
				"PERMIT_NOT_USABLE",
				"Task revision changed after permit issue.",
				task,
			);
		assertTaskCanStart(p, task);
		const gates = taskGateSignals(p, task);
		if (
			gates.length !== permit.requiredSignals.length ||
			gates.some(
				(gate) =>
					!permit.requiredSignals.some(
						(bound) => bound.id === gate.id && bound.revision === gate.revision,
					),
			)
		)
			throw new CoordinationPolicyError(
				"PERMIT_NOT_USABLE",
				"A gate revision changed after permit issue.",
			);
		for (const ref of permit.sealedInputs) {
			const artifact = p.artifacts[ref.artifactId]?.find(
				(a) => a.version === ref.version && a.digest === ref.digest,
			);
			if (!artifact)
				throw new CoordinationPolicyError("INPUT_SUPERSEDED", "Permit input no longer exists.");
			verifyArtifactBytes(artifact);
		}
		const budget = markReservationDispatched(
			p.budgets[permit.budgetId],
			permit.reservationId,
			params.outcomeUnknown,
		);
		return this.log.append(
			"permit.dispatched",
			params.actor,
			{
				...(params.outcomeUnknown ? settlePermit(dispatched, "outcome_unknown") : dispatched),
				budget,
			},
			params.idempotencyKey,
			{ taskId: permit.taskId },
		);
	}
	settlePermit(params: {
		actor: CoordinationActor;
		permitId: string;
		outcome: "settled" | "outcome_unknown" | "expired" | "cancelled_before_dispatch";
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		this.authenticate(params.actor);
		const permit = p.permits[params.permitId];
		if (!permit) throw new CoordinationPolicyError("PERMIT_NOT_USABLE", "Unknown permit.");
		if (
			permit.roleId !== params.actor.roleId ||
			permit.runId !== params.actor.runId ||
			permit.generation !== params.actor.generation
		)
			throw new CoordinationPolicyError(
				"STALE_GENERATION",
				"Only the owning attempt generation can settle it.",
			);
		const settled = settlePermit(permit, params.outcome);
		const budget = structuredClone(p.budgets[permit.budgetId]);
		const reservation = budget?.reservations[permit.reservationId];
		if (!reservation)
			throw new CoordinationPolicyError("LEASE_REQUIRED", "Attempt reservation is missing.");
		if (params.outcome === "cancelled_before_dispatch" || params.outcome === "expired") {
			reservation.state = "released";
			budget.used -= reservation.amount;
		} else {
			if (reservation.state === "outcome_unknown") budget.dispatchedUnknown -= reservation.amount;
			reservation.state = params.outcome;
			if (params.outcome === "outcome_unknown") budget.dispatchedUnknown += reservation.amount;
		}
		return this.log.append(
			"permit.settled",
			params.actor,
			{ ...settled, budget },
			params.idempotencyKey,
			{ taskId: permit.taskId },
		);
	}
	reconcilePermit(params: {
		actor: CoordinationActor;
		permitId: string;
		outcome: "settled" | "outcome_unknown" | "expired" | "cancelled_before_dispatch";
		reason: string;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		requireCapability(p, params.actor, "coordinator");
		const permit = p.permits[params.permitId];
		if (!permit) throw new CoordinationPolicyError("PERMIT_NOT_USABLE", "Unknown permit.");
		const settled = settlePermit(permit, params.outcome);
		const budget = structuredClone(p.budgets[permit.budgetId]);
		const reservation = budget?.reservations[permit.reservationId];
		if (!reservation)
			throw new CoordinationPolicyError("LEASE_REQUIRED", "Attempt reservation is missing.");
		if (params.outcome === "cancelled_before_dispatch" || params.outcome === "expired") {
			reservation.state = "released";
			budget.used -= reservation.amount;
		} else {
			if (reservation.state === "outcome_unknown") budget.dispatchedUnknown -= reservation.amount;
			reservation.state = params.outcome;
			if (params.outcome === "outcome_unknown") budget.dispatchedUnknown += reservation.amount;
		}
		return this.log.append(
			"permit.settled",
			params.actor,
			{ ...settled, budget, reason: params.reason, reconciledBy: params.actor },
			params.idempotencyKey,
			{ taskId: permit.taskId },
		);
	}
	sealArtifact(
		params: Omit<Parameters<typeof sealContentAddressedArtifact>[0], "projection" | "storeDir"> & {
			projection?: CoordinationProjection;
			storeDir?: string;
			version?: number;
			reason?: string;
			freeze?: boolean;
			idempotencyKey: string;
		},
	) {
		this.authenticate(params.actor);
		const p = this.log.stateSnapshot();
		if (params.freeze && this.workflowMode === "strict")
			requireCapability(p, params.actor, "reviewer");
		if (Object.values(p.artifacts).reduce((count, versions) => count + versions.length, 0) >= 1000)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Artifact version limit reached.");
		const version = (p.artifacts[params.artifactId]?.at(-1)?.version ?? 0) + 1;
		if (params.version !== undefined && params.version !== version)
			throw new CoordinationPolicyError(
				"STALE_REVISION",
				`Publication allocates version ${version}; omit version or supply that expected next version.`,
				{ id: params.artifactId, revision: version },
			);
		const artifact = {
			...sealContentAddressedArtifact({
				...params,
				storeDir: params.storeDir ?? this.options.artifactStoreDir,
				projection: p,
			}),
			...(params.reason ? { publicationReason: params.reason } : {}),
		};
		return this.log.append(
			params.freeze ? "artifact.frozen" : "artifact.sealed",
			params.actor,
			params.freeze ? freezeArtifact(artifact) : artifact,
			params.idempotencyKey,
		);
	}
	freezeArtifact(params: {
		actor: CoordinationActor;
		artifactId: string;
		version: number;
		idempotencyKey: string;
	}) {
		const p = this.log.stateSnapshot();
		const a = p.artifacts[params.artifactId]?.find((i) => i.version === params.version);
		if (!a) throw new CoordinationPolicyError("STALE_REVISION", "Unknown artifact.");
		if (this.workflowMode !== "fast" || a.producer.roleId !== params.actor.roleId)
			requireCapability(p, params.actor, "reviewer");
		requireArtifactAccess(p, params.actor.roleId, a);
		return this.log.append(
			"artifact.frozen",
			params.actor,
			freezeArtifact(a),
			params.idempotencyKey,
		);
	}
	supersedeArtifact(params: {
		actor: CoordinationActor;
		artifactId: string;
		previousVersion: number;
		successorVersion: number;
		policy: "cancel" | "finish_as_invalid" | "audited_override";
		reason?: string;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		try {
			requireCapability(
				p,
				params.actor,
				params.policy === "audited_override" ? "coordinator" : "reviewer",
			);
		} catch (error) {
			if (
				this.workflowMode === "fast" &&
				error instanceof CoordinationPolicyError &&
				error.code === "UNAUTHORIZED"
			)
				throw new CoordinationPolicyError(
					"UNAUTHORIZED",
					"Artifact supersede is reviewer/coordinator consumer control, not a replacement upload. For your corrected output, use subagent_artifact publish with the SAME artifactId and sourcePath; version allocation is automatic. Submit the new frozen ref; peer review rebinding is automatic.",
				);
			throw error;
		}
		if (params.policy === "audited_override" && !params.reason?.trim())
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"An audited override requires an explicit reason.",
			);
		const versions = p.artifacts[params.artifactId] ?? [];
		const previous = versions.find((artifact) => artifact.version === params.previousVersion);
		const successor = versions.find((artifact) => artifact.version === params.successorVersion);
		if (!previous || !successor || successor.version <= previous.version)
			throw new CoordinationPolicyError(
				"STALE_REVISION",
				"A newer successor artifact version is required.",
			);
		requireArtifactAccess(p, params.actor.roleId, previous);
		requireArtifactAccess(p, params.actor.roleId, successor);
		const pair = supersedeArtifact(previous, successor, params.policy);
		const affectedTasks: TaskStateRecord[] = [];
		for (const task of Object.values(p.tasks)) {
			if (
				["completed", "failed", "cancelled"].includes(task.state) ||
				!task.requiredInputs.some(
					(ref) =>
						ref.artifactId === previous.artifactId &&
						ref.version === previous.version &&
						ref.digest === previous.digest,
				)
			)
				continue;
			const active = task.state === "in_progress";
			const state = active
				? params.policy === "audited_override"
					? task.state
					: params.policy === "finish_as_invalid"
						? "failed"
						: "cancelled"
				: "blocked";
			affectedTasks.push({
				...task,
				state,
				revision: task.revision + 1,
				nextActions: [
					...task.nextActions.slice(-99),
					`Input ${previous.artifactId}@${previous.version} superseded`,
				],
			});
		}
		return this.log.append(
			"artifact.superseded",
			params.actor,
			{
				...pair.successor,
				previous: pair.previous,
				affectedTasks,
				reason: params.reason?.slice(0, 1000),
			},
			params.idempotencyKey,
		);
	}
	invalidateArtifact(params: {
		actor: CoordinationActor;
		artifactId: string;
		version: number;
		policy: "cancel" | "finish_as_invalid" | "audited_override";
		reason: string;
		idempotencyKey: string;
	}) {
		const p = this.log.snapshot();
		requireCapability(
			p,
			params.actor,
			params.policy === "audited_override" ? "coordinator" : "reviewer",
		);
		const artifact = p.artifacts[params.artifactId]?.find(
			(item) => item.version === params.version,
		);
		if (!artifact) throw new CoordinationPolicyError("STALE_REVISION", "Unknown artifact.");
		requireArtifactAccess(p, params.actor.roleId, artifact);
		if (!["sealed", "frozen"].includes(artifact.state))
			throw new CoordinationPolicyError(
				"INVALID_TRANSITION",
				"Only live artifacts can be invalidated.",
			);
		const affectedTasks = Object.values(p.tasks)
			.filter(
				(task) =>
					!["completed", "failed", "cancelled"].includes(task.state) &&
					task.requiredInputs.some(
						(ref) =>
							ref.artifactId === artifact.artifactId &&
							ref.version === artifact.version &&
							ref.digest === artifact.digest,
					),
			)
			.map((task) => ({
				...task,
				state:
					task.state === "in_progress"
						? params.policy === "audited_override"
							? task.state
							: params.policy === "finish_as_invalid"
								? ("failed" as const)
								: ("cancelled" as const)
						: ("blocked" as const),
				revision: task.revision + 1,
				reviewSatisfied: false,
				nextActions: [
					...task.nextActions.slice(-99),
					`Input ${artifact.artifactId}@${artifact.version} invalidated`,
				],
			}));
		return this.log.append(
			"artifact.invalidated",
			params.actor,
			{
				...artifact,
				state: "invalidated",
				invalidationPolicy: params.policy,
				affectedTasks,
				reason: params.reason,
			},
			params.idempotencyKey,
		);
	}
	signalRecipients(params: { actor: CoordinationActor; signalId: string }): string[] {
		this.authenticate(params.actor);
		const p = this.log.snapshot();
		if (typeof params.signalId !== "string" || !Object.hasOwn(p.signals, params.signalId))
			throw new CoordinationPolicyError("STALE_REVISION", "Unknown signal.");
		const signal = p.signals[params.signalId];
		if (
			!redactProjection(p, params.actor.roleId).signals[signal.id] &&
			!p.audit.some(
				(event) =>
					["signal.emitted", "signal.superseded"].includes(event.type) &&
					event.actor.roleId === params.actor.roleId &&
					(event.payload as SignalStateRecord).id === signal.id,
			)
		)
			throw new CoordinationPolicyError("UNAUTHORIZED", "Signal is not visible to the sender.");
		return [
			...new Set(
				[
					...signal.requiresAck,
					...signal.targets.flatMap((target) =>
						target.roleId
							? [target.roleId]
							: target.taskId
								? [p.tasks[target.taskId]?.owner]
								: (p.channels[target.channelId ?? ""]?.members.map((member) => member.roleId) ??
									[]),
					),
				].filter((roleId): roleId is string => !!roleId && Object.hasOwn(p.roles, roleId)),
			),
		];
	}
	queueSignalDelivery(params: {
		actor: CoordinationActor;
		signalId: string;
		targetRoleId: string;
		idempotencyKey: string;
		interactive?: boolean;
	}) {
		this.authenticate(params.actor);
		const p = this.log.snapshot(),
			s = p.signals[params.signalId];
		if (!s) throw new CoordinationPolicyError("STALE_REVISION", "Unknown signal.");
		if (
			!redactProjection(p, params.actor.roleId).signals[s.id] &&
			!p.audit.some(
				(event) =>
					["signal.emitted", "signal.superseded"].includes(event.type) &&
					event.actor.roleId === params.actor.roleId &&
					(event.payload as SignalStateRecord).id === s.id,
			)
		)
			throw new CoordinationPolicyError("UNAUTHORIZED", "Signal is not visible to the sender.");
		if (
			!p.roles[params.targetRoleId] ||
			!(
				s.requiresAck.includes(params.targetRoleId) ||
				s.targets.some(
					(target) =>
						target.roleId === params.targetRoleId ||
						(target.channelId &&
							p.channels[target.channelId]?.members.some(
								(member) => member.roleId === params.targetRoleId,
							)) ||
						(target.taskId && p.tasks[target.taskId]?.owner === params.targetRoleId),
				)
			)
		)
			throw new CoordinationPolicyError("UNAUTHORIZED", "Recipient is outside the signal scope.");
		const visibleSignal = redactProjection(p, params.targetRoleId).signals[s.id];
		if (!visibleSignal)
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"Recipient cannot access this signal's private context.",
			);
		const duplicate = Object.values(p.deliveries).find(
			(d) =>
				d.signalId === s.id && d.targetRoleId === params.targetRoleId && d.revision === s.revision,
		);
		if (duplicate)
			return {
				event: p.audit.findLast(
					(e) => (e.payload as any)?.id === duplicate.id,
				) as CoordinationEnvelope,
				projection: p,
				replayed: true,
			};
		if (Object.keys(p.deliveries).length >= 20000)
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Delivery limit reached.");
		let d = queueDelivery(
			params.targetRoleId,
			s.revision,
			{ signalId: s.id },
			stableId("delivery", `${s.id}:${s.revision}:${params.targetRoleId}`),
		);
		if (shouldWake(d, s.severity, params.interactive)) d = recordWake(d);
		const result = this.log.append("delivery.queued", params.actor, d, params.idempotencyKey);
		try {
			if (d.wakeCount > 0) this.deliverySink?.(d, visibleSignal);
		} catch {
			/* Accepted state is durable even when notification delivery fails. */
		}
		return result;
	}
	receipt(params: {
		actor: CoordinationActor;
		deliveryId: string;
		state: "delivered";
		idempotencyKey: string;
	}) {
		const p = this.log.stateSnapshot();
		this.authenticate(params.actor);
		const delivery = p.deliveries[params.deliveryId];
		if (!delivery || delivery.targetRoleId !== params.actor.roleId)
			throw new CoordinationPolicyError(
				"STALE_GENERATION",
				"Delivery receipt must come from its current target generation.",
			);
		if (params.state !== "delivered")
			throw new CoordinationPolicyError(
				"UNAUTHORIZED",
				"Receipts cannot fabricate agent acknowledgement or resolve obligations.",
			);
		let next = markDelivered(delivery);
		const signal = delivery.signalId ? p.signals[delivery.signalId] : undefined;
		if (
			signal?.acknowledgements.some(
				(ack) => ack.roleId === params.actor.roleId && ack.generation === params.actor.generation,
			)
		)
			next = markAcknowledged(next);
		if (signal?.state === "resolved") next = markResolved(next);
		return this.log.append("delivery.delivered", params.actor, next, params.idempotencyKey);
	}
	inbox(params: { actor: CoordinationActor; limit?: number }) {
		this.authenticate(params.actor);
		if (!this.log.hasInbox(params.actor.roleId)) return { lines: [], deliveryIds: [] };
		const p = this.log.stateSnapshot();
		const queued = Object.values(p.deliveries).filter(
			(d) => d.targetRoleId === params.actor.roleId && d.state === "queued" && d.wakeCount === 0,
		);
		if (!queued.length) return { lines: [], deliveryIds: [] };
		const view = redactProjection(p, params.actor.roleId);
		const limit = Math.max(1, Math.min(params.limit ?? this.options.maxDigestLines, 100));
		const lines: string[] = [],
			deliveryIds: string[] = [];
		let bytes = 0;
		for (const delivery of queued) {
			const signal = view.signals[delivery.signalId ?? ""];
			// A late notification must not resurrect a resolved/superseded hold.
			if (signal?.state !== "open") continue;
			const line = formatSignalDelivery(signal, params.actor.roleId);
			if (lines.length >= limit || bytes + Buffer.byteLength(line) > 20 * 1024) break;
			lines.push(line);
			deliveryIds.push(delivery.id);
			bytes += Buffer.byteLength(line);
		}
		return { lines, deliveryIds };
	}
	checkpoint(params: { actor: CoordinationActor; idempotencyKey: string }) {
		this.authenticate(params.actor);
		return this.log.checkpoint(params.idempotencyKey, params.actor);
	}
	digest(actor: CoordinationActor, sinceSeq = 0, maxLines = this.options.maxDigestLines) {
		this.authenticate(actor);
		const projection = this.snapshot(actor.roleId);
		const digest = buildDigest(
			projection,
			actor.roleId,
			sinceSeq,
			Math.min(maxLines, this.options.maxDigestLines),
		);
		const included = projection.audit.filter((event) => digest.eventIds.includes(event.eventId));
		const deliveryIds = Object.values(projection.deliveries)
			.filter(
				(delivery) =>
					delivery.targetRoleId === actor.roleId &&
					delivery.state === "queued" &&
					included.some((event) => {
						const payload = event.payload as {
							id?: string;
							revision?: number;
							signal?: { id: string; revision: number };
						};
						const source = payload.signal ?? payload;
						return source.id === delivery.signalId && source.revision === delivery.revision;
					}),
			)
			.map((delivery) => delivery.id);
		return { ...digest, deliveryIds };
	}
}
