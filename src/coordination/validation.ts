import { CoordinationPolicyError } from "./policy.ts";

const fields: Record<string, readonly string[]> = {
	admitRole: ["roleId", "capabilities", "private"],
	bindRole: ["roleId", "runId", "generation", "capabilities"],
	rebindRole: ["roleId", "runId", "checkpoint"],
	detachRole: ["roleId", "runId", "outcome"],
	createChannel: ["name", "purpose", "members", "delivery", "review"],
	updateChannel: ["channelId", "expectedRevision", "members", "delivery"],
	closeChannel: ["channelId", "expectedRevision"],
	sendChannel: ["channelId", "expectedRevision", "text", "delivery"],
	createTask: [
		"id",
		"title",
		"owner",
		"dependsOn",
		"requiredInputs",
		"requiredAcknowledgements",
		"leases",
		"verification",
		"nextActions",
		"blockers",
		"start",
		"autoStart",
	],
	handoffTask: ["taskId", "expectedRevision", "owner", "reason", "nextActions"],
	transitionTask: [
		"taskId",
		"expectedRevision",
		"state",
		"outputs",
		"verification",
		"reviewSatisfied",
		"nextActions",
		"leases",
		"blockers",
		"reason",
	],
	bindTaskInputs: ["taskId", "expectedRevision", "requiredInputs", "reason", "leases"],
	supersedeSignal: [
		"signalId",
		"expectedRevision",
		"summary",
		"targets",
		"requiresAck",
		"kind",
		"severity",
		"artifactRefs",
	],
	reconcilePermit: ["permitId", "outcome", "reason"],
	invalidateArtifact: ["artifactId", "version", "policy", "reason"],
	reviewTask: ["taskId", "expectedRevision", "decision", "reason", "nextActions"],
	emitSignal: ["kind", "severity", "summary", "targets", "requiresAck", "artifactRefs"],
	acknowledgeSignal: ["signalId", "expectedRevision"],
	resolveSignal: ["signalId", "expectedRevision"],
	createBudget: ["budgetId", "limit"],
	reserveBudget: ["budgetId", "amount", "taskId", "expectedRevision"],
	releaseBudget: ["budgetId", "reservationId"],
	preparePermit: ["taskId", "expectedRevision", "budgetId", "operation"],
	issuePermit: ["taskId", "budgetId", "reservationId", "operation"],
	dispatchPermit: ["permitId", "operation", "outcomeUnknown"],
	settlePermit: ["permitId", "outcome"],
	sealArtifact: [
		"artifactId",
		"sourcePath",
		"storeDir",
		"privateTo",
		"projection",
		"freeze",
		"version",
		"reason",
	],
	freezeArtifact: ["artifactId", "version"],
	supersedeArtifact: ["artifactId", "previousVersion", "successorVersion", "policy", "reason"],
	queueSignalDelivery: ["signalId", "targetRoleId", "interactive"],
	receipt: ["deliveryId", "state"],
	checkpoint: [],
};
const requiredFields: Record<string, string> = {
	admitRole: "roleId",
	bindRole: "roleId runId",
	rebindRole: "roleId runId",
	detachRole: "roleId",
	createChannel: "name purpose members",
	updateChannel: "channelId expectedRevision",
	closeChannel: "channelId expectedRevision",
	sendChannel: "channelId expectedRevision text",
	handoffTask: "taskId expectedRevision owner reason nextActions",
	createTask: "title owner",
	transitionTask: "taskId expectedRevision state",
	reviewTask: "taskId expectedRevision decision reason",
	bindTaskInputs: "taskId expectedRevision requiredInputs reason",
	supersedeSignal: "signalId expectedRevision summary",
	invalidateArtifact: "artifactId version policy reason",
	reconcilePermit: "permitId outcome reason",
	emitSignal: "kind severity summary",
	acknowledgeSignal: "signalId expectedRevision",
	resolveSignal: "signalId expectedRevision",
	preparePermit: "taskId expectedRevision budgetId operation",
	createBudget: "budgetId limit",
	reserveBudget: "budgetId",
	releaseBudget: "budgetId reservationId",
	issuePermit: "taskId budgetId reservationId operation",
	dispatchPermit: "permitId",
	settlePermit: "permitId outcome",
	sealArtifact: "artifactId sourcePath",
	freezeArtifact: "artifactId version",
	supersedeArtifact: "artifactId previousVersion successorVersion policy",
	queueSignalDelivery: "signalId targetRoleId",
	receipt: "deliveryId state",
	checkpoint: "",
};
const enumFields: Record<string, readonly string[]> = {
	decision: ["approved", "changes_requested"],
	delivery: ["urgent", "digest", "next_turn"],
	severity: ["info", "action", "blocking", "critical"],
	policy: ["cancel", "finish_as_invalid", "audited_override"],
	outcome: ["settled", "outcome_unknown", "expired", "cancelled_before_dispatch"],
	kind: [
		"ready",
		"hold",
		"go",
		"blocked",
		"unblocked",
		"dependency_requested",
		"dependency_satisfied",
		"decision",
		"correction",
		"superseded",
		"artifact_published",
		"artifact_frozen",
		"artifact_invalidated",
		"review_requested",
		"changes_requested",
		"approved",
		"budget_reserved",
		"budget_released",
		"budget_exhausted",
		"attention",
		"ack",
	],
};
const ids = new Set([
	"id",
	"roleId",
	"runId",
	"owner",
	"channelId",
	"taskId",
	"signalId",
	"budgetId",
	"reservationId",
	"permitId",
	"artifactId",
	"targetRoleId",
	"deliveryId",
]);
const numbers = new Set([
	"generation",
	"expectedRevision",
	"limit",
	"amount",
	"version",
	"previousVersion",
	"successorVersion",
]);
const booleans = new Set([
	"private",
	"reviewSatisfied",
	"outcomeUnknown",
	"interactive",
	"start",
	"autoStart",
	"freeze",
]);
const arrays = new Set([
	"capabilities",
	"dependsOn",
	"requiredAcknowledgements",
	"verification",
	"nextActions",
	"leases",
	"blockers",
	"requiresAck",
	"privateTo",
]);
function invalid(field: string): never {
	throw new CoordinationPolicyError(
		"BOUNDS_EXCEEDED",
		`Invalid or unbounded coordination field: ${field}.`,
	);
}
function object(value: unknown): value is Record<string, unknown> {
	return !!value && typeof value === "object" && !Array.isArray(value);
}
function text(value: unknown, field: string, max = 8000): asserts value is string {
	if (typeof value !== "string" || !value.trim() || value.length > max || value.includes("\0"))
		invalid(field);
}
function identifier(value: unknown, field: string) {
	text(value, field, 200);
	if (
		["__proto__", "constructor", "prototype"].includes(value) ||
		/[\u0000-\u001f\u007f]/.test(value)
	)
		invalid(field);
}
function refs(value: unknown, field: string) {
	if (!Array.isArray(value) || value.length > 100) invalid(field);
	for (const ref of value) {
		if (
			!object(ref) ||
			Object.keys(ref).some((key) => !["artifactId", "version", "digest"].includes(key))
		)
			invalid(field);
		identifier(ref.artifactId, `${field}.artifactId`);
		if (!Number.isSafeInteger(ref.version) || Number(ref.version) < 1) invalid(`${field}.version`);
		text(ref.digest, `${field}.digest`, 100);
	}
}

/** Validate raw socket callers too; provider tool schemas are not a trust boundary. */
export function validateMutationRequest(method: string, params: Record<string, unknown>): void {
	const allowed = fields[method];
	if (!Array.isArray(allowed) || !object(params)) invalid("request");
	for (const field of (requiredFields[method] ?? "").split(" ").filter(Boolean))
		if (params[field] === undefined) invalid(field);
	for (const [field, value] of Object.entries(params)) {
		if (!["actor", "idempotencyKey", ...allowed].includes(field)) invalid(field);
		if (value === undefined || field === "projection") continue;
		if (field === "actor") {
			if (
				!object(value) ||
				Object.keys(value).some((key) => !["roleId", "runId", "generation"].includes(key)) ||
				!Number.isSafeInteger(value.generation) ||
				Number(value.generation) < 0
			)
				invalid(field);
			identifier(value.roleId, "actor.roleId");
			identifier(value.runId, "actor.runId");
			continue;
		}
		if (field === "idempotencyKey") {
			text(value, field, 200);
			continue;
		}
		if (ids.has(field)) {
			identifier(value, field);
			continue;
		}
		if (numbers.has(field)) {
			if (!Number.isSafeInteger(value) || Number(value) < 0) invalid(field);
			continue;
		}
		if (booleans.has(field)) {
			if (typeof value !== "boolean") invalid(field);
			continue;
		}
		if (arrays.has(field)) {
			if (!Array.isArray(value) || value.length > 100) invalid(field);
			for (const item of value)
				if (["verification", "nextActions"].includes(field)) text(item, field, 1000);
				else identifier(item, field);
			continue;
		}
		if (["requiredInputs", "outputs", "artifactRefs"].includes(field)) {
			refs(value, field);
			continue;
		}
		if (field === "review") {
			if (
				!object(value) ||
				Object.keys(value).some(
					(key) => !["taskIds", "reviewerRoleId", "reviewTaskId", "maxCorrections"].includes(key),
				)
			)
				invalid(field);
			identifier(value.reviewerRoleId, "review.reviewerRoleId");
			if (value.reviewTaskId !== undefined) identifier(value.reviewTaskId, "review.reviewTaskId");
			if (
				value.maxCorrections !== undefined &&
				(!Number.isInteger(value.maxCorrections) ||
					(value.maxCorrections as number) < 0 ||
					(value.maxCorrections as number) > 10)
			)
				invalid("review.maxCorrections");
			if (Array.isArray(value.taskIds) && value.taskIds.includes(value.reviewTaskId))
				invalid("review.reviewTaskId");
			if (
				!Array.isArray(value.taskIds) ||
				value.taskIds.length < 1 ||
				value.taskIds.length > 20 ||
				new Set(value.taskIds).size !== value.taskIds.length
			)
				invalid("review.taskIds");
			for (const id of value.taskIds) identifier(id, "review.taskIds");
			continue;
		}
		if (field === "members") {
			if (!Array.isArray(value) || value.length < 1 || value.length > 20) invalid(field);
			for (const member of value) {
				if (
					!object(member) ||
					Object.keys(member).some((key) => !["roleId", "mode"].includes(key)) ||
					!["watch", "participate"].includes(String(member.mode))
				)
					invalid(field);
				identifier(member.roleId, "members.roleId");
			}
			continue;
		}
		if (field === "targets") {
			if (!Array.isArray(value) || value.length > 100) invalid(field);
			for (const target of value) {
				if (!object(target) || Object.keys(target).length === 0) invalid(field);
				for (const [key, id] of Object.entries(target)) {
					if (!["roleId", "channelId", "taskId"].includes(key)) invalid(field);
					identifier(id, field);
				}
			}
			continue;
		}
		if (method === "detachRole" && field === "outcome") {
			if (!["finished", "failed", "needs_help", "interrupted"].includes(String(value)))
				invalid(field);
			continue;
		}
		if (enumFields[field] && !enumFields[field].includes(String(value))) invalid(field);
		if (field === "state") {
			const states =
				method === "receipt"
					? ["delivered", "acknowledged", "resolved"]
					: [
							"draft",
							"ready",
							"in_progress",
							"blocked",
							"submitted",
							"review",
							"completed",
							"failed",
							"cancelled",
						];
			if (!states.includes(String(value))) invalid(field);
		}
		const limits: Record<string, number> = {
			name: 80,
			purpose: 500,
			summary: 500,
			title: 200,
			reason: 1000,
			sourcePath: 4096,
			storeDir: 4096,
			operation: 100,
		};
		text(value, field, limits[field] ?? 8000);
	}
	text(params.idempotencyKey, "idempotencyKey", 200);
}
