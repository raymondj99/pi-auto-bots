import { redactProjection } from "./policy.ts";
import type { CoordinationProjection } from "./protocol.ts";
import { peerReviewChannel } from "./review.ts";

export interface CoordinationOperationalView {
	cursor: number;
	roles: Array<{ roleId: string; generation: number; online: boolean }>;
	tasks: Array<{
		id: string;
		owner: string;
		state: string;
		revision: number;
		ownerOnline?: boolean;
		pendingAction?: string;
		dependencies: string[];
		dependencyStates?: Array<{ id: string; state: string }>;
		blockers: string[];
	}>;
	blockers: Array<{
		id: string;
		severity: string;
		summary: string;
		pendingAcknowledgements: string[];
	}>;
	artifacts: Array<{
		artifactId: string;
		version: number;
		digest: string;
		state: string;
		consumers: string[];
	}>;
	budgets: Array<{ id: string; limit: number; used: number; dispatchedUnknown: number }>;
	deliveries: Array<{ id: string; targetRoleId: string; state: string; wakeCount: number }>;
	audit: Array<{ seq: number; type: string; actorRoleId: string; timestamp: number }>;
}

/** Shared bounded read-only dashboard/TUI projection; contains no paths or credentials. */
export function buildOperationalView(
	projection: CoordinationProjection,
	viewerRoleId: string,
	limit = 100,
): CoordinationOperationalView {
	const view = redactProjection(projection, viewerRoleId);
	const bounded = Math.max(1, Math.min(limit, 1000));
	return {
		cursor: view.seq,
		roles: Object.values(view.roles)
			.slice(0, 100)
			.map((r) => ({ roleId: r.roleId, generation: r.generation, online: r.active })),
		tasks: Object.values(view.tasks)
			.slice(0, 100)
			.map((t) => ({
				id: t.id,
				owner: t.owner,
				state: t.state,
				revision: t.revision,
				ownerOnline: !!view.roles[t.owner]?.active && view.roles[t.owner]?.runId === t.activeRunId,
				pendingAction:
					peerReviewChannel(view, t.id) && ["draft", "blocked"].includes(t.state)
						? "waiting for peer submissions (automatic wake)"
						: ["submitted", "review"].includes(t.state) && !t.reviewSatisfied
							? "awaiting review"
							: t.state === "review" && t.reviewSatisfied
								? "ready to complete"
								: t.state === "blocked" && !view.roles[t.owner]?.active
									? "resume or hand off"
									: undefined,
				dependencies: t.dependsOn.slice(0, 100),
				dependencyStates: t.dependsOn.slice(0, 100).map((id) => ({
					id,
					state: view.tasks[id]?.state ?? view.signals[id]?.state ?? "unavailable",
				})),
				blockers: t.blockers.slice(0, 100),
			})),
		blockers: Object.values(view.signals)
			.filter((s) => s.state === "open" && ["blocking", "critical"].includes(s.severity))
			.slice(0, bounded)
			.map((s) => ({
				id: s.id,
				severity: s.severity,
				summary: s.summary,
				pendingAcknowledgements: s.requiresAck.filter(
					(role) =>
						!s.acknowledgements.some(
							(a) => a.roleId === role && a.generation === view.roles[role]?.generation,
						),
				),
			})),
		artifacts: Object.values(view.artifacts)
			.flat()
			.slice(-bounded)
			.map((a) => ({
				artifactId: a.artifactId,
				version: a.version,
				digest: a.digest,
				state: a.state,
				consumers: Object.values(view.tasks)
					.filter((task) =>
						task.requiredInputs.some(
							(ref) =>
								ref.artifactId === a.artifactId &&
								ref.version === a.version &&
								ref.digest === a.digest,
						),
					)
					.map((task) => task.id)
					.slice(0, 100),
			})),
		budgets: Object.values(view.budgets)
			.slice(0, 100)
			.map((b) => ({
				id: b.id,
				limit: b.limit,
				used: b.used,
				dispatchedUnknown: b.dispatchedUnknown,
			})),
		deliveries: Object.values(view.deliveries)
			.slice(-bounded)
			.map((d) => ({
				id: d.id,
				targetRoleId: d.targetRoleId,
				state: d.state,
				wakeCount: d.wakeCount,
			})),
		audit: view.audit.slice(-bounded).map((e) => ({
			seq: e.seq,
			type: e.type,
			actorRoleId: e.actor.roleId,
			timestamp: e.timestamp,
		})),
	};
}
