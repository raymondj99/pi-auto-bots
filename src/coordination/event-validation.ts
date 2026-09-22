import { type TSchema, Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";

const id = Type.String({
	minLength: 1,
	maxLength: 200,
	pattern:
		"^(?=.{1,200}$)(?!(?:__proto__|constructor|prototype)$)[^\\u0000-\\u001f\\u007f-\\u009f]+$",
});
const text = (maxLength: number) => Type.String({ maxLength });
const integer = (maximum = Number.MAX_SAFE_INTEGER, minimum = 0) =>
	Type.Integer({ minimum, maximum });
const enumeration = (...values: string[]) => Type.String({ pattern: `^(?:${values.join("|")})$` });
const list = (schema: TSchema, maxItems = 100) => Type.Array(schema, { maxItems });
const object = (properties: Record<string, TSchema>) =>
	Type.Object(properties, { additionalProperties: false });
const actor = object({ roleId: id, runId: id, generation: integer() });
const ref = object({
	artifactId: id,
	version: integer(Number.MAX_SAFE_INTEGER, 1),
	digest: text(100),
});
const member = object({ roleId: id, mode: enumeration("watch", "participate") });
const readiness = object({
	fingerprint: text(100),
	signalId: id,
	revision: integer(Number.MAX_SAFE_INTEGER, 1),
	ready: Type.Boolean(),
});
const channel = object({
	review: Type.Optional(
		object({
			taskIds: Type.Array(id, { minItems: 1, maxItems: 20, uniqueItems: true }),
			reviewerRoleId: id,
			reviewTaskId: Type.Optional(id),
			maxCorrections: Type.Optional(integer(10)),
		}),
	),
	readiness: Type.Optional(readiness),
	id,
	name: text(80),
	purpose: text(500),
	members: list(member, 20),
	revision: integer(Number.MAX_SAFE_INTEGER, 1),
	delivery: enumeration("urgent", "digest", "next_turn"),
	status: enumeration("open", "closed"),
});
const taskProperties = {
	id,
	title: text(200),
	owner: id,
	activeRunId: Type.Optional(id),
	state: enumeration(
		"draft",
		"ready",
		"in_progress",
		"blocked",
		"submitted",
		"review",
		"completed",
		"failed",
		"cancelled",
	),
	revision: integer(Number.MAX_SAFE_INTEGER, 1),
	dependsOn: list(id),
	blockers: list(id),
	requiredInputs: list(ref),
	reviewSourceInputs: Type.Optional(list(ref)),
	reviewCorrections: Type.Optional(integer(10)),
	outputs: list(ref),
	requiredAcknowledgements: list(id),
	leases: list(id),
	reviewSatisfied: Type.Boolean(),
	verification: list(text(1000)),
	nextActions: list(text(1000)),
	permits: list(id, 1000),
	reviewReason: Type.Optional(text(1000)),
	inputBindingReason: Type.Optional(text(1000)),
	transitionReason: Type.Optional(text(1000)),
};
const task = object(taskProperties);
const target = Type.Union([
	object({ roleId: id }),
	object({ channelId: id }),
	object({ taskId: id }),
]);
const signal = object({
	id,
	kind: enumeration(
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
	),
	state: enumeration("open", "resolved", "superseded"),
	severity: enumeration("info", "action", "blocking", "critical"),
	summary: text(500),
	details: Type.Optional(text(8000)),
	targets: list(target),
	artifactRefs: list(ref),
	requiresAck: list(id),
	acknowledgements: list(object({ roleId: id, runId: id, generation: integer(), at: integer() })),
	supersedes: Type.Optional(id),
	revision: integer(Number.MAX_SAFE_INTEGER, 1),
	resolvedBy: Type.Optional(id),
});
const delivery = object({
	id,
	targetRoleId: id,
	signalId: Type.Optional(id),
	channelId: Type.Optional(id),
	revision: integer(Number.MAX_SAFE_INTEGER, 1),
	state: enumeration("queued", "delivered", "acknowledged", "resolved"),
	queuedAt: integer(),
	deliveredAt: Type.Optional(integer()),
	acknowledgedAt: Type.Optional(integer()),
	resolvedAt: Type.Optional(integer()),
	wakeCount: integer(),
});
const budget = object({
	id,
	limit: integer(1000000),
	used: integer(1000000),
	dispatchedUnknown: integer(1000000),
	reservations: Type.Record(
		id,
		object({
			amount: integer(1000000, 1),
			state: enumeration("reserved", "dispatched", "settled", "released", "outcome_unknown"),
		}),
		{ maxProperties: 10000, additionalProperties: false },
	),
});
const artifactProperties = {
	artifactId: id,
	version: integer(Number.MAX_SAFE_INTEGER, 1),
	digest: text(100),
	bytes: integer(64 * 1024 * 1024),
	path: Type.Optional(text(4096)),
	state: enumeration("sealed", "frozen", "superseded", "invalidated"),
	producer: actor,
	publicationReason: Type.Optional(text(1000)),
	supersedes: Type.Optional(ref),
	privateTo: Type.Optional(list(id)),
	invalidationPolicy: Type.Optional(enumeration("cancel", "finish_as_invalid", "audited_override")),
	consumers: list(id),
};
const artifact = object(artifactProperties);
const permit = object({
	id,
	taskId: id,
	taskRevision: integer(Number.MAX_SAFE_INTEGER, 1),
	roleId: id,
	runId: id,
	generation: integer(),
	sealedInputs: list(ref),
	requiredSignals: list(object({ id, revision: integer() }), 1000),
	acknowledgedBy: list(id),
	budgetId: id,
	reservationId: id,
	operation: text(100),
	state: enumeration(
		"reserved",
		"dispatched",
		"settled",
		"expired",
		"cancelled_before_dispatch",
		"outcome_unknown",
	),
	idempotencyKey: text(200),
	expiresAt: integer(),
	budget: Type.Optional(budget),
	task: Type.Optional(task),
	reason: Type.Optional(text(1000)),
	reconciledBy: Type.Optional(actor),
});
const schemas: Record<string, TSchema> = {
	"role.admitted": object({
		roleId: id,
		capabilities: Type.Optional(list(id)),
		private: Type.Optional(Type.Boolean()),
	}),
	"role.bound": object({
		roleId: id,
		runId: id,
		generation: integer(Number.MAX_SAFE_INTEGER, 1),
		capabilities: Type.Optional(list(id)),
	}),
	"role.rebound": object({
		roleId: id,
		runId: id,
		generation: integer(Number.MAX_SAFE_INTEGER, 1),
		checkpoint: Type.Optional(text(8000)),
	}),
	"role.detached": object({
		roleId: id,
		runId: Type.Optional(id),
		affectedTasks: Type.Optional(
			list(
				object({
					id,
					state: taskProperties.state,
					revision: integer(Number.MAX_SAFE_INTEGER, 1),
					nextAction: Type.Optional(text(160)),
				}),
			),
		),
	}),
	"review.readiness": object({
		channelId: id,
		readiness,
		signal,
		deliveries: list(delivery),
		task: Type.Optional(task),
	}),
	"channel.created": channel,
	"channel.membership_changed": channel,
	"channel.closed": object({ id, revision: integer(Number.MAX_SAFE_INTEGER, 1) }),
	"channel.message": object({
		id,
		revision: integer(Number.MAX_SAFE_INTEGER, 1),
		summary: text(8000),
		targets: list(target),
		signal,
		deliveries: list(delivery, 20),
		queued: list(id, 20),
		offline: list(id, 20),
	}),
	"task.created": task,
	"task.transitioned": object({
		...taskProperties,
		reviewMessage: Type.Optional(
			object({
				id,
				revision: integer(Number.MAX_SAFE_INTEGER, 1),
				summary: text(8000),
				signal,
				deliveries: list(delivery),
			}),
		),
	}),
	"task.inputs_bound": task,
	"task.handed_off": task,
	"signal.emitted": signal,
	"signal.acknowledged": object({
		...signal.properties,
		deliveries: Type.Optional(list(delivery, 200)),
	}),
	"signal.resolved": object({
		...signal.properties,
		deliveries: Type.Optional(list(delivery, 200)),
	}),
	"signal.superseded": object({
		...signal.properties,
		previous: signal,
		affectedTasks: list(task),
	}),
	"artifact.sealed": artifact,
	"artifact.frozen": artifact,
	"artifact.verified": artifact,
	"artifact.invalidated": object({
		...artifactProperties,
		affectedTasks: list(task),
		reason: text(1000),
	}),
	"artifact.superseded": object({
		...artifactProperties,
		previous: Type.Optional(artifact),
		affectedTasks: Type.Optional(list(task)),
		reason: Type.Optional(text(1000)),
	}),
	"budget.created": budget,
	"budget.reserved": object({ ...budget.properties, task: Type.Optional(task) }),
	"budget.released": budget,
	"permit.issued": permit,
	"permit.dispatched": permit,
	"permit.settled": permit,
	"delivery.queued": delivery,
	"delivery.delivered": delivery,
	"delivery.acknowledged": delivery,
	"delivery.resolved": delivery,
	"checkpoint.saved": object({
		roleId: id,
		checkpoint: text(120000),
		checksum: Type.Optional(Type.String({ pattern: "^[a-f0-9]{64}$" })),
	}),
};

const envelope = object({
	protocolVersion: Type.Literal(2),
	seq: integer(Number.MAX_SAFE_INTEGER, 1),
	eventId: id,
	idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
	requestDigest: Type.Optional(Type.String({ pattern: "^[a-f0-9]{64}$" })),
	timestamp: integer(),
	actor,
	goalId: Type.Optional(id),
	taskId: Type.Optional(id),
	channelId: Type.Optional(id),
	causationId: Type.Optional(id),
	correlationId: Type.Optional(id),
	type: text(80),
	payload: Type.Unknown(),
});

export function validEventEnvelope(value: unknown): boolean {
	return Value.Check(envelope, value);
}

const records = (schema: TSchema, maxProperties: number) =>
	Type.Record(id, schema, { maxProperties, additionalProperties: false });
const checkpoint = object({
	protocolVersion: Type.Literal(2),
	seq: integer(),
	sessionId: id,
	branchId: id,
	online: Type.Boolean(),
	roles: records(
		object({
			roleId: id,
			runId: Type.Optional(id),
			generation: integer(),
			active: Type.Boolean(),
			capabilities: list(id),
			private: Type.Optional(Type.Boolean()),
			unavailable: Type.Optional(Type.Boolean()),
		}),
		101,
	),
	channels: records(channel, 32),
	tasks: records(task, 100),
	signals: records(signal, 1000),
	artifacts: records(list(artifact, 1000), 1000),
	budgets: records(budget, 100),
	permits: records(permit, 1000),
	deliveries: records(delivery, 20000),
	audit: list(envelope, 0),
	idempotency: object({}),
});
export function validCheckpointProjection(value: unknown): boolean {
	if (!Value.Check(checkpoint, value)) return false;
	const projection = value as {
		artifacts: Record<string, unknown[]>;
		budgets: Record<string, unknown>;
	};
	return (
		Object.values(projection.artifacts).reduce((sum, versions) => sum + versions.length, 0) <=
			1000 &&
		Object.values(projection.budgets).every((state) => validEventPayload("budget.created", state))
	);
}

export function validEventPayload(type: string, payload: unknown): boolean {
	const schema = Object.hasOwn(schemas, type) ? schemas[type] : undefined;
	if (!schema || !Value.Check(schema, payload)) return false;
	const candidate = payload as { budget?: unknown };
	const accounting = type.startsWith("budget.") ? payload : candidate.budget;
	if (accounting) {
		const record = accounting as {
			used: number;
			limit: number;
			dispatchedUnknown: number;
			reservations: Record<string, { amount: number; state: string }>;
		};
		const reservations = Object.values(record.reservations);
		if (
			record.used > record.limit ||
			reservations.reduce(
				(sum, lease) => sum + (lease.state === "released" ? 0 : lease.amount),
				0,
			) !== record.used
		)
			return false;
		if (
			reservations.reduce(
				(sum, lease) => sum + (lease.state === "outcome_unknown" ? lease.amount : 0),
				0,
			) !== record.dispatchedUnknown
		)
			return false;
	}
	return true;
}
