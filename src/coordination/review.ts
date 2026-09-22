import { createHash } from "node:crypto";
import { verifyArtifactBytes } from "./artifacts.ts";
import { queueDelivery, recordWake } from "./delivery.ts";
import { assertTaskCanStart, CoordinationPolicyError, requireArtifactAccess } from "./policy.ts";
import type {
	ArtifactRef,
	ChannelState,
	CoordinationActor,
	CoordinationProjection,
	DeliveryRecord,
	SignalStateRecord,
	TaskStateRecord,
} from "./protocol.ts";

export interface ReviewReadinessEvent {
	channelId: string;
	readiness: NonNullable<ChannelState["readiness"]>;
	signal: SignalStateRecord;
	deliveries: DeliveryRecord[];
	task?: TaskStateRecord;
}
export interface ReviewMessage {
	id: string;
	revision: number;
	summary: string;
	signal: SignalStateRecord;
	deliveries: DeliveryRecord[];
}
const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const reportSubmitted = (task: TaskStateRecord | undefined) =>
	!!task && ["submitted", "review", "completed"].includes(task.state);

export function peerReviewChannel(p: CoordinationProjection, taskId: string) {
	return Object.values(p.channels).find((c) => c.review?.reviewTaskId === taskId);
}

/** Broker-side gate: talking with peers is allowed, reviewing early is not. */
export function assertPeerReviewReady(p: CoordinationProjection, task: TaskStateRecord): void {
	const channel = peerReviewChannel(p, task.id);
	if (!channel) return;
	if (channel.status !== "open" || !channel.readiness?.ready || !task.reviewSourceInputs?.length)
		throw new CoordinationPolicyError(
			"UNRESOLVED_DEPENDENCY",
			"Peer review awaits all frozen submissions; discuss questions in the channel, then end the turn and wait for automatic GO.",
			task,
		);
	for (const id of channel.review!.taskIds) {
		const source = p.tasks[id];
		if (
			!reportSubmitted(source) ||
			source.outputs.some(
				(r) => !task.reviewSourceInputs!.some((i) => JSON.stringify(i) === JSON.stringify(r)),
			)
		)
			throw new CoordinationPolicyError(
				"INPUT_SUPERSEDED",
				"Peer review inputs changed; wait for renewed GO.",
			);
	}
}

/** Called once at settlement over authenticated transport, never polled by a model. */
export function peerRunDisposition(
	p: CoordinationProjection,
	roleId: string,
): { keepAlive: boolean; phase: string } {
	for (const channel of Object.values(p.channels)) {
		if (!channel.review?.reviewTaskId || channel.status !== "open") continue;
		const report = p.tasks[channel.review.reviewTaskId];
		const producer = channel.review.taskIds.some((id) => p.tasks[id]?.owner === roleId);
		if (roleId !== channel.review.reviewerRoleId && !producer) continue;
		if (reportSubmitted(report) || (report && ["cancelled", "failed"].includes(report.state)))
			continue;
		return {
			keepAlive: true,
			phase:
				roleId === channel.review.reviewerRoleId
					? channel.readiness?.ready
						? "reviewing"
						: "waiting_for_submissions"
					: "available_for_peer_review",
		};
	}
	return { keepAlive: false, phase: "released" };
}

/** Pure state check first; verify bytes only for a new ready set. */
export function reviewReadiness(
	p: CoordinationProjection,
	channel: ChannelState,
): ReviewReadinessEvent | undefined {
	if (!channel.review) return;
	const reviewer = channel.review.reviewerRoleId;
	const peer = !!channel.review.reviewTaskId;
	const report = peer ? p.tasks[channel.review.reviewTaskId!] : undefined;
	if (
		peer &&
		channel.status === "open" &&
		(!report ||
			(!reportSubmitted(report) &&
				!["cancelled", "failed"].includes(report.state) &&
				(!p.roles[reviewer]?.active || report.activeRunId !== p.roles[reviewer].runId)))
	)
		return; // Activation follows an actual authorized run binding, never an offline role.
	const tasks = channel.review.taskIds.map((id) => p.tasks[id]);
	const submitted = tasks.every(reportSubmitted);
	const finished = peer && reportSubmitted(report) && tasks.every((t) => t?.state === "completed");
	const released =
		peer &&
		(finished ||
			channel.status === "closed" ||
			(!!report && ["cancelled", "failed"].includes(report.state)));
	let ready =
		channel.status === "open" &&
		submitted &&
		tasks.every(
			(task) =>
				task.outputs.length && task.verification.length && task.verification.every((s) => s.trim()),
		);
	let refs: ArtifactRef[] = [];
	if (ready)
		try {
			for (const task of tasks) {
				assertTaskCanStart(p, task, false);
				if (
					task.permits.some((id) =>
						["reserved", "dispatched", "outcome_unknown"].includes(p.permits[id]?.state),
					)
				)
					throw new Error("Unsettled attempt");
				for (const ref of task.outputs) {
					const a = p.artifacts[ref.artifactId]?.at(-1);
					if (a?.state !== "frozen" || a.version !== ref.version || a.digest !== ref.digest)
						throw new Error("Non-current output");
					requireArtifactAccess(p, reviewer, a);
					refs.push(ref);
				}
			}
			refs = [...new Map(refs.map((ref) => [JSON.stringify(ref), ref])).values()];
			if (refs.length > 100) throw new Error("Review input bound exceeded");
		} catch {
			ready = false;
			refs = [];
		}
	const sources = tasks.filter(Boolean).map((t) => ({
		id: t.id,
		owner: t.owner,
		outputs: t.outputs,
		inputs: t.requiredInputs,
		verification: t.verification,
	}));
	let fingerprint = released
		? `released:${channel.status}:${report?.state === "failed" || report?.state === "cancelled" ? report.state : "finished"}`
		: ready
			? hash(
					peer
						? [
								sources,
								report!.activeRunId,
								p.roles[reviewer]?.generation,
								report!.requiredInputs.filter(
									(r) =>
										!report!.reviewSourceInputs?.some(
											(old) => JSON.stringify(old) === JSON.stringify(r),
										),
								),
							]
						: sources,
				)
			: "waiting";
	if (
		channel.readiness?.fingerprint === fingerprint ||
		(!released && !ready && !channel.readiness && !submitted)
	)
		return;
	let activated: TaskStateRecord | undefined;
	if (ready && !released)
		try {
			for (const ref of refs) verifyArtifactBytes(p.artifacts[ref.artifactId].at(-1)!);
			if (peer) {
				if (!report || !["draft", "ready", "blocked", "in_progress"].includes(report.state))
					throw new Error("Review assignment cannot be activated");
				if (
					report.permits.some((id) =>
						["reserved", "dispatched", "outcome_unknown"].includes(p.permits[id]?.state),
					)
				)
					throw new Error("Review has unsettled attempts");
				const base = report.requiredInputs.filter(
					(r) =>
						!report.reviewSourceInputs?.some((old) => JSON.stringify(old) === JSON.stringify(r)),
				);
				const inputs = [...new Map([...base, ...refs].map((r) => [JSON.stringify(r), r])).values()];
				if (inputs.length > 100) throw new Error("Review input bound exceeded");
				activated = {
					...report,
					requiredInputs: inputs,
					reviewSourceInputs: refs,
					state: "in_progress",
					revision: report.revision + 1,
					reviewSatisfied: false,
				};
				assertTaskCanStart(p, activated, false);
				for (const ref of inputs) {
					const a = p.artifacts[ref.artifactId].find((a) => a.version === ref.version)!;
					requireArtifactAccess(p, reviewer, a);
					verifyArtifactBytes(a);
				}
			}
		} catch {
			ready = false;
			refs = [];
			activated = undefined;
			fingerprint = "waiting";
		}
	if (
		channel.readiness?.fingerprint === fingerprint ||
		(!released && !ready && !channel.readiness && !submitted)
	)
		return;
	if (peer && !released && !ready && report?.state === "in_progress")
		activated = {
			...report,
			state: "blocked",
			reviewSatisfied: false,
			revision: report.revision + 1,
		};
	const revision = (channel.readiness?.revision ?? 0) + 1;
	const signalId = `review-ready-${hash([channel.id, revision]).slice(0, 24)}`;
	const recipients = peer
		? released
			? [
					...new Set([
						...tasks.filter(Boolean).map((t) => t.owner),
						...(!finished ? [reviewer] : []),
					]),
				]
			: ready
				? [reviewer]
				: submitted
					? [reviewer, "coordinator"]
					: []
		: [...new Set(["coordinator", reviewer])];
	const signal: SignalStateRecord = {
		id: signalId,
		kind: released ? "decision" : ready ? "ready" : "attention",
		severity: "action",
		state: "open",
		summary:
			released && !finished
				? `Peer workflow #${channel.name} closed or cancelled. End your turn and close; this is not a successful review.`
				: finished
					? `Peer review complete in #${channel.name}. Workers are released: return your final result and close. The review report still requires independent coordinator approval.`
					: ready
						? peer
							? `Review GO in #${channel.name}: ${report!.id}@${activated!.revision} started with all frozen inputs. Review now; send questions/findings directly in this channel. No acknowledgement required.`
							: `Review GO for #${channel.name}: all ${tasks.length} frozen submissions are ready. Launch/rebind ${reviewer} now; do not wait for worker exit or final-result messages.`
						: `Review not ready for #${channel.name}: check current frozen outputs, evidence, input gates and unsettled attempts. Any previous GO is withdrawn.`,
		details: JSON.stringify({
			channelId: channel.id,
			ready,
			tasks: tasks
				.filter(Boolean)
				.map((t) => ({ taskId: t.id, revision: t.revision, state: t.state })),
		}),
		targets: recipients.map((roleId) => ({ roleId })),
		artifactRefs: refs,
		requiresAck: [],
		acknowledgements: [],
		revision: 1,
	};
	const deliveries = recipients.map((roleId) =>
		recordWake(
			queueDelivery(
				roleId,
				1,
				{ signalId },
				`review-delivery-${hash([signalId, roleId]).slice(0, 24)}`,
			),
		),
	);
	return {
		channelId: channel.id,
		readiness: { ready, fingerprint, signalId, revision },
		signal,
		deliveries,
		...(activated ? { task: activated } : {}),
	};
}

/** Decisions remain shared history; corrections wake only their affected owner. */
export function reviewMessage(
	channel: ChannelState,
	task: TaskStateRecord,
	actor: CoordinationActor,
	reason: string,
	approved: boolean,
	key: string,
): ReviewMessage {
	const id = `review-message-${hash([actor, key]).slice(0, 24)}`;
	const summary = `${approved ? "APPROVED" : "CHANGES REQUESTED"}: ${task.id}@${task.revision} (${task.owner}). ${reason}`;
	const peerCorrection = !!channel.review?.reviewTaskId && !approved;
	const signal: SignalStateRecord = {
		id,
		kind: "attention",
		severity: peerCorrection ? "action" : "info",
		state: "open",
		summary: summary.slice(0, 500),
		details: summary,
		targets: [{ channelId: channel.id }],
		artifactRefs: [],
		requiresAck: [],
		acknowledgements: [],
		revision: 1,
	};
	const deliveries = channel.members
		.filter((m) => m.roleId !== actor.roleId)
		.map((m) => {
			const d = queueDelivery(
				m.roleId,
				1,
				{ channelId: channel.id, signalId: id },
				`review-delivery-${hash([id, m.roleId]).slice(0, 24)}`,
			);
			return peerCorrection && m.roleId === task.owner ? recordWake(d) : d;
		});
	return { id: channel.id, revision: channel.revision, summary, signal, deliveries };
}
