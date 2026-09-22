export type RoleId = string;
export type RunId = string;
export type TaskId = string;
export type ArtifactId = string;
export type SignalId = string;
export type PermitId = string;
export type BudgetId = string;

export const COORDINATION_EVENT_TYPES = [
	"role.admitted",
	"role.bound",
	"role.rebound",
	"role.detached",
	"channel.created",
	"channel.membership_changed",
	"channel.closed",
	"channel.message",
	"review.readiness",
	"task.created",
	"task.transitioned",
	"task.inputs_bound",
	"task.handed_off",
	"signal.emitted",
	"signal.acknowledged",
	"signal.resolved",
	"signal.superseded",
	"artifact.sealed",
	"artifact.frozen",
	"artifact.superseded",
	"artifact.invalidated",
	"artifact.verified",
	"budget.created",
	"budget.reserved",
	"budget.released",
	"permit.issued",
	"permit.dispatched",
	"permit.settled",
	"delivery.queued",
	"delivery.delivered",
	"delivery.acknowledged",
	"delivery.resolved",
	"checkpoint.saved",
] as const;
export type CoordinationEventType = (typeof COORDINATION_EVENT_TYPES)[number];
export type CoordinationSeverity = "info" | "action" | "blocking" | "critical";
export type SignalKind =
	| "ready"
	| "hold"
	| "go"
	| "blocked"
	| "unblocked"
	| "dependency_requested"
	| "dependency_satisfied"
	| "decision"
	| "correction"
	| "superseded"
	| "artifact_published"
	| "artifact_frozen"
	| "artifact_invalidated"
	| "review_requested"
	| "changes_requested"
	| "approved"
	| "budget_reserved"
	| "budget_released"
	| "budget_exhausted"
	| "attention"
	| "ack";
export type SignalState = "open" | "resolved" | "superseded";
export type TaskState =
	| "draft"
	| "ready"
	| "in_progress"
	| "blocked"
	| "submitted"
	| "review"
	| "completed"
	| "failed"
	| "cancelled";
export type ArtifactState = "sealed" | "frozen" | "superseded" | "invalidated";
export type PermitState =
	| "reserved"
	| "dispatched"
	| "settled"
	| "expired"
	| "cancelled_before_dispatch"
	| "outcome_unknown";
export type DeliveryState = "queued" | "delivered" | "acknowledged" | "resolved";
export type ChannelDelivery = "urgent" | "digest" | "next_turn";
export type ChannelMemberMode = "watch" | "participate";
export type StableErrorCode =
	| "COORDINATION_DISABLED"
	| "UNAUTHORIZED"
	| "STALE_REVISION"
	| "IDEMPOTENCY_CONFLICT"
	| "UNKNOWN_ROLE"
	| "STALE_GENERATION"
	| "INVALID_TRANSITION"
	| "UNRESOLVED_DEPENDENCY"
	| "UNRESOLVED_BLOCKER"
	| "ACK_REQUIRED"
	| "INPUT_NOT_FROZEN"
	| "INPUT_SUPERSEDED"
	| "LEASE_REQUIRED"
	| "BUDGET_EXHAUSTED"
	| "PERMIT_NOT_USABLE"
	| "ARTIFACT_TAMPERED"
	| "CHECKPOINT_CORRUPT"
	| "BOUNDS_EXCEEDED";

export interface CoordinationActor {
	roleId: RoleId;
	runId: RunId;
	generation: number;
}
export interface CoordinationEnvelope<T = unknown> {
	protocolVersion: 2;
	seq: number;
	eventId: string;
	idempotencyKey: string;
	requestDigest?: string;
	timestamp: number;
	actor: CoordinationActor;
	goalId?: string;
	taskId?: TaskId;
	channelId?: string;
	causationId?: string;
	correlationId?: string;
	type: CoordinationEventType;
	payload: T;
}
export interface RoleState {
	roleId: RoleId;
	runId?: RunId;
	generation: number;
	active: boolean;
	capabilities: string[];
	private?: boolean;
	checkpoint?: string;
	unavailable?: boolean;
}
export interface ChannelState {
	id: string;
	name: string;
	purpose: string;
	members: Array<{ roleId: RoleId; mode: ChannelMemberMode }>;
	revision: number;
	delivery: ChannelDelivery;
	status: "open" | "closed";
	review?: {
		taskIds: TaskId[];
		reviewerRoleId: RoleId;
		reviewTaskId?: TaskId;
		maxCorrections?: number;
	};
	readiness?: { fingerprint: string; signalId: SignalId; revision: number; ready: boolean };
}
export interface ArtifactRef {
	artifactId: ArtifactId;
	version: number;
	digest: string;
}
export interface ArtifactStateRecord {
	artifactId: ArtifactId;
	version: number;
	digest: string;
	bytes: number;
	path?: string;
	state: ArtifactState;
	producer: CoordinationActor;
	publicationReason?: string;
	supersedes?: ArtifactRef;
	privateTo?: RoleId[];
	invalidationPolicy?: "cancel" | "finish_as_invalid" | "audited_override";
	consumers: TaskId[];
}
export interface SignalStateRecord {
	id: SignalId;
	kind: SignalKind;
	state: SignalState;
	severity: CoordinationSeverity;
	summary: string;
	details?: string;
	targets: Array<{ roleId?: RoleId; channelId?: string; taskId?: TaskId }>;
	artifactRefs: ArtifactRef[];
	requiresAck: RoleId[];
	acknowledgements: Array<{ roleId: RoleId; runId: RunId; generation: number; at: number }>;
	supersedes?: SignalId;
	revision: number;
	resolvedBy?: RoleId;
}
export interface TaskStateRecord {
	id: TaskId;
	title: string;
	owner: RoleId;
	activeRunId?: RunId;
	state: TaskState;
	revision: number;
	dependsOn: string[];
	blockers: SignalId[];
	requiredInputs: ArtifactRef[];
	reviewSourceInputs?: ArtifactRef[];
	reviewCorrections?: number;
	outputs: ArtifactRef[];
	requiredAcknowledgements: RoleId[];
	leases: string[];
	reviewSatisfied: boolean;
	verification: string[];
	nextActions: string[];
	permits: PermitId[];
}
export interface TaskExitUpdate {
	id: TaskId;
	state: TaskState;
	revision: number;
	nextAction?: string;
}
export interface BudgetState {
	id: BudgetId;
	limit: number;
	used: number;
	dispatchedUnknown: number;
	reservations: Record<
		string,
		{
			amount: number;
			state: "reserved" | "dispatched" | "settled" | "released" | "outcome_unknown";
		}
	>;
}
export interface PermitStateRecord {
	id: PermitId;
	taskId: TaskId;
	taskRevision: number;
	roleId: RoleId;
	runId: RunId;
	generation: number;
	sealedInputs: ArtifactRef[];
	requiredSignals: Array<{ id: SignalId; revision: number }>;
	acknowledgedBy: RoleId[];
	budgetId: BudgetId;
	reservationId: string;
	operation: string;
	state: PermitState;
	idempotencyKey: string;
	expiresAt: number;
}
export interface DeliveryRecord {
	id: string;
	targetRoleId: RoleId;
	signalId?: SignalId;
	channelId?: string;
	revision: number;
	state: DeliveryState;
	queuedAt: number;
	deliveredAt?: number;
	acknowledgedAt?: number;
	resolvedAt?: number;
	wakeCount: number;
}
export interface CoordinationProjection {
	protocolVersion: 2;
	seq: number;
	sessionId: string;
	branchId: string;
	online: boolean;
	roles: Record<RoleId, RoleState>;
	channels: Record<string, ChannelState>;
	tasks: Record<TaskId, TaskStateRecord>;
	signals: Record<SignalId, SignalStateRecord>;
	artifacts: Record<ArtifactId, ArtifactStateRecord[]>;
	budgets: Record<BudgetId, BudgetState>;
	permits: Record<PermitId, PermitStateRecord>;
	deliveries: Record<string, DeliveryRecord>;
	audit: CoordinationEnvelope[];
	idempotency: Record<string, { digest: string; result: unknown }>;
}
export type MutationResult<T = unknown> =
	| { ok: true; event?: CoordinationEnvelope; state?: T }
	| { ok: false; code: StableErrorCode; message: string; current?: unknown };

export function stableStringify(value: unknown): string {
	if (value === null || typeof value !== "object") return JSON.stringify(value);
	if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
	const object = value as Record<string, unknown>;
	return `{${Object.keys(object)
		.sort()
		.map((key) => `${JSON.stringify(key)}:${stableStringify(object[key])}`)
		.join(",")}}`;
}

export function boundedText(value: unknown, field: string, max = 8000): string {
	if (typeof value !== "string" || !value.trim() || value.length > max)
		throw new Error(`${field} must be non-empty text (maximum ${max} characters).`);
	return value;
}
