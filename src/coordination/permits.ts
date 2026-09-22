import { randomUUID } from "node:crypto";
import { verifyArtifactBytes } from "./artifacts.ts";
import { assertTaskCanStart, CoordinationPolicyError, taskGateSignals } from "./policy.ts";
import type {
	ArtifactRef,
	BudgetState,
	CoordinationActor,
	CoordinationProjection,
	PermitStateRecord,
	TaskStateRecord,
} from "./protocol.ts";

export function createBudget(id: string, limit: number): BudgetState {
	if (!id || !Number.isInteger(limit) || limit < 0 || limit > 1_000_000)
		throw new CoordinationPolicyError(
			"BOUNDS_EXCEEDED",
			"Budget limit must be a non-negative bounded integer.",
		);
	return { id, limit, used: 0, dispatchedUnknown: 0, reservations: {} };
}

export function reserveBudget(
	budget: BudgetState,
	amount = 1,
	reservationId: string = randomUUID(),
): BudgetState {
	const next = structuredClone(budget) as BudgetState;
	if (
		!Number.isSafeInteger(amount) ||
		amount <= 0 ||
		!reservationId ||
		Object.hasOwn(next.reservations, reservationId) ||
		Object.keys(next.reservations).length >= 10_000
	)
		throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Reservation amount/count is invalid.");
	if (next.used + amount > next.limit)
		throw new CoordinationPolicyError(
			"BUDGET_EXHAUSTED",
			"Budget cannot be overspent concurrently.",
			budget,
		);
	next.used += amount;
	next.reservations[reservationId] = { amount, state: "reserved" };
	return next;
}

export function releaseBudget(budget: BudgetState, reservationId: string): BudgetState {
	const next = structuredClone(budget) as BudgetState;
	const reservation = next.reservations[reservationId];
	if (reservation?.state !== "reserved") return next;
	reservation.state = "released";
	next.used -= reservation.amount;
	return next;
}

export function markReservationDispatched(
	budget: BudgetState,
	reservationId: string,
	unknown = false,
): BudgetState {
	const next = structuredClone(budget) as BudgetState;
	const reservation = next.reservations[reservationId];
	if (reservation?.state !== "reserved")
		throw new CoordinationPolicyError("LEASE_REQUIRED", "Reservation must be reserved and unused.");
	reservation.state = unknown ? "outcome_unknown" : "dispatched";
	if (unknown) next.dispatchedUnknown += reservation.amount;
	return next;
}

export function issuePermit(params: {
	projection: CoordinationProjection;
	actor: CoordinationActor;
	task: TaskStateRecord;
	budgetId: string;
	reservationId: string;
	operation: string;
	idempotencyKey: string;
	permitId?: string;
	ttlMs?: number;
}): PermitStateRecord {
	const task = params.task;
	if (task.state !== "in_progress")
		throw new CoordinationPolicyError(
			"INVALID_TRANSITION",
			"Protected attempts require an in-progress assigned task.",
			task,
		);
	if (task.owner !== params.actor.roleId || task.activeRunId !== params.actor.runId)
		throw new CoordinationPolicyError(
			"STALE_GENERATION",
			"Permit actor does not own the active task run.",
			task,
		);
	assertTaskCanStart(params.projection, task);
	const budget = params.projection.budgets[params.budgetId];
	if (
		!budget?.reservations[params.reservationId] ||
		budget.reservations[params.reservationId].state !== "reserved"
	)
		throw new CoordinationPolicyError(
			"LEASE_REQUIRED",
			"Permit requires a reserved budget lease.",
			budget,
		);
	if (!task.leases.includes(params.reservationId))
		throw new CoordinationPolicyError(
			"LEASE_REQUIRED",
			"Reservation is not assigned to this task.",
		);
	if (
		Object.values(params.projection.permits).some(
			(permit) =>
				permit.budgetId === params.budgetId && permit.reservationId === params.reservationId,
		)
	) {
		throw new CoordinationPolicyError(
			"LEASE_REQUIRED",
			"Reservation already belongs to an attempt.",
		);
	}
	const sealedInputs: ArtifactRef[] = [];
	for (const input of task.requiredInputs) {
		const artifact = params.projection.artifacts[input.artifactId]?.find(
			(candidate) => candidate.version === input.version && candidate.digest === input.digest,
		);
		if (artifact?.state !== "frozen")
			throw new CoordinationPolicyError("INPUT_NOT_FROZEN", "Permit input must be frozen.", input);
		sealedInputs.push(verifyArtifactBytes(artifact));
	}
	return {
		id: params.permitId ?? randomUUID(),
		taskId: task.id,
		taskRevision: task.revision,
		roleId: params.actor.roleId,
		runId: params.actor.runId,
		generation: params.actor.generation,
		sealedInputs,
		requiredSignals: taskGateSignals(params.projection, task).map((signal) => ({
			id: signal.id,
			revision: signal.revision,
		})),
		acknowledgedBy: [
			...new Set([
				...task.requiredAcknowledgements,
				...taskGateSignals(params.projection, task).flatMap((signal) => signal.requiresAck),
			]),
		],
		budgetId: params.budgetId,
		reservationId: params.reservationId,
		operation: params.operation,
		state: "reserved",
		idempotencyKey: params.idempotencyKey,
		expiresAt: Date.now() + (params.ttlMs ?? 300000),
	};
}

export function dispatchPermit(
	permit: PermitStateRecord,
	actor: CoordinationActor,
): PermitStateRecord {
	if (
		permit.state !== "reserved" ||
		permit.roleId !== actor.roleId ||
		permit.runId !== actor.runId ||
		permit.generation !== actor.generation ||
		permit.expiresAt < Date.now()
	) {
		throw new CoordinationPolicyError(
			"PERMIT_NOT_USABLE",
			"Permit is not usable by this role generation.",
			permit,
		);
	}
	return { ...permit, state: "dispatched" };
}

export function settlePermit(
	permit: PermitStateRecord,
	outcome: "settled" | "outcome_unknown" | "expired" | "cancelled_before_dispatch",
): PermitStateRecord {
	const allowed: Record<PermitStateRecord["state"], readonly string[]> = {
		reserved: ["expired", "cancelled_before_dispatch"],
		dispatched: ["settled", "outcome_unknown"],
		outcome_unknown: ["settled"],
		settled: [],
		expired: [],
		cancelled_before_dispatch: [],
	};
	if (outcome === "expired" && permit.expiresAt > Date.now())
		throw new CoordinationPolicyError(
			"PERMIT_NOT_USABLE",
			"An unexpired permit cannot be marked expired.",
		);
	if (!allowed[permit.state].includes(outcome))
		throw new CoordinationPolicyError(
			"PERMIT_NOT_USABLE",
			"Illegal single-use attempt settlement transition.",
			permit,
		);
	return { ...permit, state: outcome };
}
