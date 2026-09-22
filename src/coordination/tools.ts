import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { StringEnum } from "@mariozechner/pi-ai";
import type { ExtensionAPI, ToolDefinition } from "@mariozechner/pi-coding-agent";
import { truncateHead } from "@mariozechner/pi-coding-agent";
import { type TSchema, Type } from "@sinclair/typebox";
import type { CoordinationBroker } from "./broker.ts";
import { CoordinationPolicyError } from "./policy.ts";
import type { CoordinationActor } from "./protocol.ts";
import { QUERY_FIELDS, QUERY_KINDS } from "./query.ts";
import { compactMutationResult, publicCoordinationError, publicMutationResult } from "./results.ts";

const actionFields: Record<string, Record<string, string>> = {
	subagent_task: {
		create:
			"taskId title owner dependsOn requiredInputs requiredAcknowledgements blockers leases verification nextActions start",
		handoff: "taskId expectedRevision owner reason nextActions",
		bind_inputs: "taskId expectedRevision requiredInputs reason leases",
		approve: "taskId expectedRevision reason nextActions",
		changes_requested: "taskId expectedRevision reason nextActions",
		...Object.fromEntries(
			["ready", "start", "unblock"].map((action) => [
				action,
				"taskId expectedRevision leases blockers reason",
			]),
		),
		submit: "taskId expectedRevision outputs verification reviewSatisfied nextActions reason",
		request_review: "taskId expectedRevision reason",
		complete: "taskId expectedRevision outputs verification reviewSatisfied nextActions reason",
		block: "taskId expectedRevision blockers nextActions reason",
		fail: "taskId expectedRevision nextActions reason",
		cancel: "taskId expectedRevision nextActions reason",
	},
	subagent_channel: {
		create: "name purpose members delivery review",
		send: "channelId expectedRevision text delivery",
		update: "channelId expectedRevision members delivery",
		close: "channelId expectedRevision",
	},
	subagent_role: {
		admit: "roleId capabilities private",
		rebind: "roleId runId",
		detach: "roleId runId",
	},
	subagent_signal: {
		emit: "kind severity summary targets requiresAck artifactRefs",
		supersede: "signalId expectedRevision summary kind severity targets requiresAck artifactRefs",
		acknowledge: "signalId expectedRevision",
		resolve: "signalId expectedRevision",
	},
	subagent_artifact: {
		publish: "artifactId sourcePath storeDir version reason",
		seal: "artifactId sourcePath storeDir version reason",
		freeze: "artifactId version",
		supersede: "artifactId previousVersion successorVersion policy reason",
		invalidate: "artifactId version policy reason",
	},
	subagent_permit: {
		prepare: "taskId expectedRevision budgetId operation",
		create_budget: "budgetId limit",
		reserve: "budgetId taskId expectedRevision",
		release: "budgetId reservationId",
		issue: "budgetId reservationId taskId operation",
		arm: "permitId operation",
		dispatch: "permitId operation",
		settle: "permitId outcome",
		reconcile: "permitId outcome reason",
	},
};

/** Never silently discard a schema-visible argument belonging to another action. */
function validateToolAction(name: string, params: any): void {
	const actions = actionFields[name];
	if (!actions) return;
	const allowed = actions[params.action];
	if (allowed === undefined)
		throw new CoordinationPolicyError("BOUNDS_EXCEEDED", `Unknown ${name} action.`);
	const fields = new Set(["action", "idempotencyKey", ...allowed.split(" ")]);
	for (const [field, value] of Object.entries(params))
		if (value !== undefined && !fields.has(field)) {
			throw new CoordinationPolicyError(
				"BOUNDS_EXCEEDED",
				`${name} ${params.action} does not accept ${field}. Allowed fields: ${allowed}.`,
			);
		}
}

const requiredActionFields: Record<string, Record<string, string>> = {
	subagent_task: {
		create: "title owner",
		bind_inputs: "taskId expectedRevision requiredInputs reason",
		handoff: "taskId expectedRevision owner reason nextActions",
		approve: "taskId expectedRevision reason nextActions",
		changes_requested: "taskId expectedRevision reason",
		complete: "taskId expectedRevision nextActions",
	},
	subagent_channel: {
		create: "name purpose members",
		send: "channelId expectedRevision text",
		update: "channelId expectedRevision",
		close: "channelId expectedRevision",
	},
	subagent_role: { admit: "roleId", rebind: "roleId runId", detach: "roleId" },
	subagent_signal: {
		emit: "kind summary",
		supersede: "signalId expectedRevision summary",
		acknowledge: "signalId expectedRevision",
		resolve: "signalId expectedRevision",
	},
	subagent_artifact: {
		publish: "artifactId sourcePath",
		seal: "artifactId sourcePath",
		freeze: "artifactId version",
		supersede: "artifactId previousVersion successorVersion",
		invalidate: "artifactId version reason",
	},
	subagent_permit: {
		prepare: "taskId expectedRevision budgetId operation",
		create_budget: "budgetId limit",
		reserve: "budgetId",
		release: "budgetId reservationId",
		issue: "taskId budgetId reservationId operation",
		arm: "permitId operation",
		dispatch: "permitId operation",
		settle: "permitId",
		reconcile: "permitId reason",
	},
};

/** Object-root schema with action-specific variants. Flat replay arguments are
 * normalized before Pi validates them, without exposing cross-action fields. */
export function actionSpecificSchema(name: string, schema: TSchema): TSchema {
	if (!actionFields[name]) return schema;
	const variants = Object.entries(actionFields[name]).map(([action, fields]) => {
		const required = new Set([
			"action",
			"idempotencyKey",
			...(
				requiredActionFields[name]?.[action] ??
				(name === "subagent_task" ? "taskId expectedRevision" : "")
			).split(" "),
		]);
		const properties: Record<string, TSchema> = { action: StringEnum([action]) };
		for (const field of ["idempotencyKey", ...fields.split(" ")]) {
			const original = schema.properties[field];
			properties[field] = required.has(field)
				? Type.Required(Type.Object({ [field]: original })).properties[field]
				: Type.Optional(original);
		}
		return Type.Object(properties, {
			additionalProperties: false,
			description: `${name} ${action}`,
		});
	});
	return Type.Object({ request: Type.Union(variants) }, { additionalProperties: false });
}

function registerTool<T extends TSchema>(pi: ExtensionAPI, definition: ToolDefinition<T>) {
	const grouped = !!actionFields[definition.name];
	const tool: ToolDefinition<T> & { prepareArguments(args: any): any } = {
		...definition,
		parameters: actionSpecificSchema(definition.name, definition.parameters) as T,
		prepareArguments: (args: any) => {
			if (!grouped || !args || typeof args !== "object") return args;
			const request = args.request ?? args;
			validateToolAction(definition.name, request);
			const required = [
				"idempotencyKey",
				...(
					requiredActionFields[definition.name]?.[request.action] ??
					(definition.name === "subagent_task" ? "taskId expectedRevision" : "")
				).split(" "),
			].filter(Boolean);
			const missing = required.filter((field) => request[field] === undefined);
			if (missing.length)
				throw new CoordinationPolicyError(
					"BOUNDS_EXCEEDED",
					`${definition.name} ${request.action} requires ${missing.join(", ")}.`,
				);
			return "request" in args ? args : { request: args };
		},
		async execute(id, params: any, signal, onUpdate, ctx) {
			try {
				if (grouped && params?.request) {
					if (Object.keys(params).some((key) => key !== "request"))
						throw new CoordinationPolicyError(
							"BOUNDS_EXCEEDED",
							"Only request is accepted at the tool envelope root.",
						);
					params = params.request;
				}
				validateToolAction(definition.name, params);
				return await definition.execute(id, params, signal, onUpdate, ctx);
			} catch (error) {
				const safe = publicCoordinationError(error);
				throw Object.assign(new Error(JSON.stringify(safe)), {
					code: safe.code,
					current: safe.current,
				});
			}
		},
	};
	pi.registerTool(tool);
}

function render(result: unknown) {
	result = publicMutationResult(result);
	const compact = compactMutationResult(result);
	const text = truncateHead(JSON.stringify(compact, null, compact === result ? 2 : undefined));
	return {
		content: [
			{
				type: "text" as const,
				text: text.content + (text.truncated ? "\n[Coordination result truncated.]" : ""),
			},
		],
		details: result,
	};
}
/**
 * Every tool name this extension registers in a coordinated session. Used to build
 * the child `--tools` allowlist so a restricted subagent can still coordinate.
 */
export const COORDINATION_TOOLS = [
	"subagent_team",
	"subagent_task",
	"subagent_channel",
	"subagent_role",
	"subagent_signal",
	"subagent_artifact",
	"subagent_permit",
	"subagent_checkpoint",
] as const;

export interface CoordinationToolContext {
	broker: CoordinationBroker;
	actor: CoordinationActor;
	protectedOperations?: readonly string[];
	armPermit?: (permitId: string, operation: string) => Promise<unknown>;
}

export function registerCoordinationWorkflowTools(
	pi: ExtensionAPI,
	getContext: () => CoordinationToolContext,
) {
	const denied = new Set((process.env.PI_DENY_TOOLS ?? "").split(",").map((s) => s.trim()));
	const result = (value: unknown) => render(value);
	if (!denied.has("subagent_team"))
		registerTool(pi, {
			name: "subagent_team",
			label: "Coordination Team",
			description:
				"Query scoped current state. Supply sinceCursor explicitly for an audit digest; routine inbox updates arrive automatically. Never poll for completion.",
			parameters: Type.Object({
				sinceCursor: Type.Optional(Type.Integer({ minimum: 0 })),
				kind: Type.Optional(StringEnum(QUERY_KINDS)),
				id: Type.Optional(Type.String({ maxLength: 200 })),
				field: Type.Optional(StringEnum(QUERY_FIELDS)),
				offset: Type.Optional(Type.Integer({ minimum: 0 })),
				limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 100 })),
				expectedSeq: Type.Optional(Type.Integer({ minimum: 0 })),
			}),
			async execute(_id, p) {
				const { broker, actor } = getContext();
				const state = await broker.query({
					actor,
					kind: p.kind,
					id: p.id,
					field: p.field,
					offset: p.offset,
					limit: p.limit,
					expectedSeq: p.expectedSeq,
				});
				const digest =
					p.sinceCursor === undefined
						? { cursor: state.seq, lines: [], eventIds: [], deliveryIds: [], truncated: 0 }
						: await broker.digest(actor, p.sinceCursor);
				return result({ digest, state });
			},
		});
	if (!denied.has("subagent_task"))
		registerTool(pi, {
			name: "subagent_task",
			label: "Coordination Task",
			description:
				"Create or revision-guard a typed task. Create accepts taskId, title, owner, inputs, leases and verification/nextActions. bind_inputs requires taskId, expectedRevision, frozen requiredInputs and reason. Other transitions require taskId and expectedRevision; approve/changes_requested also require reason and accept nextActions. Submit/request_review are not completion. Workers may submit reviewSatisfied:false, never approve themselves. Submit then end the turn; peer runs park automatically, other runs exit. Correction recipe: start a blocked task with its current revision, fix/test, publish new versions, then submit with the returned/current revision. Fast start needs no preceding ready call; blocked-to-submit is invalid. Review-channel decisions atomically post your reason to peers; do not duplicate that handoff. Fast-mode approve requires explicit nextActions (empty list allowed) and verifies/completes atomically; strict mode requires a separate complete. Fast mode: create with start:true or start directly from draft; no lease for ordinary work. Strict mode start requires a live lease. Explicit dependencies/holds still gate start. handoff requires owner, reason and explicit nextActions; accepted handoff returns the task to draft and the old owner must stop.",
			parameters: Type.Object({
				action: StringEnum([
					"create",
					"bind_inputs",
					"handoff",
					"ready",
					"start",
					"block",
					"unblock",
					"submit",
					"request_review",
					"approve",
					"changes_requested",
					"complete",
					"fail",
					"cancel",
				] as const),
				idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
				taskId: Type.Optional(Type.String()),
				expectedRevision: Type.Optional(Type.Integer()),
				title: Type.Optional(Type.String()),
				owner: Type.Optional(Type.String()),
				start: Type.Optional(Type.Boolean()),
				dependsOn: Type.Optional(Type.Array(Type.String(), { maxItems: 100 })),
				requiredInputs: Type.Optional(
					Type.Array(
						Type.Object({
							artifactId: Type.String(),
							version: Type.Integer(),
							digest: Type.String(),
						}),
						{ maxItems: 100 },
					),
				),
				requiredAcknowledgements: Type.Optional(Type.Array(Type.String(), { maxItems: 100 })),
				blockers: Type.Optional(Type.Array(Type.String(), { maxItems: 100 })),
				reason: Type.Optional(Type.String({ minLength: 1, maxLength: 1000 })),
				outputs: Type.Optional(
					Type.Array(
						Type.Object({
							artifactId: Type.String(),
							version: Type.Integer(),
							digest: Type.String(),
						}),
					),
				),
				verification: Type.Optional(Type.Array(Type.String())),
				reviewSatisfied: Type.Optional(Type.Boolean()),
				nextActions: Type.Optional(Type.Array(Type.String())),
				leases: Type.Optional(Type.Array(Type.String())),
			}),
			async execute(_id, p) {
				const { broker, actor } = getContext();
				if (p.action === "create")
					return result(
						await broker.createTask({
							actor,
							id: p.taskId,
							title: p.title!,
							owner: p.owner!,
							dependsOn: p.dependsOn,
							requiredInputs: p.requiredInputs,
							requiredAcknowledgements: p.requiredAcknowledgements,
							leases: p.leases,
							verification: p.verification,
							nextActions: p.nextActions,
							blockers: p.blockers,
							start: p.start,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "bind_inputs")
					return result(
						await broker.bindTaskInputs({
							actor,
							taskId: p.taskId!,
							expectedRevision: p.expectedRevision!,
							requiredInputs: p.requiredInputs!,
							leases: p.leases,
							reason: p.reason!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "handoff")
					return result(
						await broker.handoffTask({
							actor,
							taskId: p.taskId!,
							expectedRevision: p.expectedRevision!,
							owner: p.owner!,
							reason: p.reason!,
							nextActions: p.nextActions!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "approve" || p.action === "changes_requested")
					return result(
						await broker.reviewTask({
							actor,
							taskId: p.taskId!,
							expectedRevision: p.expectedRevision!,
							decision: p.action === "approve" ? "approved" : "changes_requested",
							reason: p.reason!,
							nextActions: p.nextActions,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				const states = {
					ready: "ready",
					start: "in_progress",
					block: "blocked",
					unblock: "ready",
					submit: "submitted",
					request_review: "review",
					complete: "completed",
					fail: "failed",
					cancel: "cancelled",
				} as const;
				return result(
					await broker.transitionTask({
						actor,
						taskId: p.taskId!,
						expectedRevision: p.expectedRevision!,
						state: states[p.action],
						outputs: p.outputs,
						verification: p.verification,
						reviewSatisfied: p.reviewSatisfied,
						nextActions: p.nextActions,
						leases: p.leases,
						blockers: p.blockers,
						reason: p.reason,
						idempotencyKey: p.idempotencyKey,
					}),
				);
			},
		});
	if (!denied.has("subagent_channel"))
		registerTool(pi, {
			name: "subagent_channel",
			label: "Coordination Channel",
			description:
				"Create/send/update/close a channel using the action-specific request. Send/update/close require expectedRevision. Create can declare review:{taskIds,reviewerRoleId}: automatically emits Review GO once all named tasks submit current frozen outputs, independent of worker exit. Add reviewTaskId to enable peer-driven lifecycle in fast mode: reviewer task is gated, automatically binds submissions/starts, idle workers stay available for questions/corrections, and report submission releases them. No coordinator GO relay or resumes. maxCorrections defaults to 2 per task. Channel sends wake peers by default, not the coordinator; digest sends are FYI. Discuss questions and findings here, not only automatic notices.",
			parameters: Type.Object({
				action: StringEnum(["create", "send", "update", "close"] as const),
				idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
				channelId: Type.Optional(Type.String()),
				expectedRevision: Type.Optional(Type.Integer()),
				name: Type.Optional(Type.String()),
				purpose: Type.Optional(Type.String()),
				review: Type.Optional(
					Type.Object(
						{
							taskIds: Type.Array(Type.String(), { minItems: 1, maxItems: 20, uniqueItems: true }),
							reviewerRoleId: Type.String(),
							reviewTaskId: Type.Optional(Type.String()),
							maxCorrections: Type.Optional(Type.Integer({ minimum: 0, maximum: 10 })),
						},
						{ additionalProperties: false },
					),
				),
				text: Type.Optional(Type.String({ minLength: 1, maxLength: 8000 })),
				members: Type.Optional(
					Type.Array(
						Type.Object({
							roleId: Type.String(),
							mode: StringEnum(["watch", "participate"] as const),
						}),
					),
				),
				delivery: Type.Optional(StringEnum(["urgent", "digest", "next_turn"] as const)),
			}),
			async execute(_id, p) {
				const { broker, actor } = getContext();
				if (p.action === "create")
					return result(
						await broker.createChannel({
							actor,
							name: p.name!,
							purpose: p.purpose!,
							members: p.members!,
							delivery: p.delivery,
							review: p.review,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "send")
					return result(
						await broker.sendChannel({
							actor,
							channelId: p.channelId!,
							expectedRevision: p.expectedRevision!,
							text: p.text!,
							delivery: p.delivery,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "close")
					return result(
						await broker.closeChannel({
							actor,
							channelId: p.channelId!,
							expectedRevision: p.expectedRevision!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				return result(
					await broker.updateChannel({
						actor,
						channelId: p.channelId!,
						expectedRevision: p.expectedRevision!,
						members: p.members,
						delivery: p.delivery,
						idempotencyKey: p.idempotencyKey,
					}),
				);
			},
		});
	if (!denied.has("subagent_role"))
		registerTool(pi, {
			name: "subagent_role",
			label: "Coordination Role",
			description: "Coordinator-only stable role admission or generation-fenced rebind.",
			parameters: Type.Object({
				action: StringEnum(["admit", "rebind", "detach"] as const),
				idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
				roleId: Type.String(),
				runId: Type.Optional(Type.String()),
				capabilities: Type.Optional(Type.Array(Type.String())),
				private: Type.Optional(Type.Boolean()),
			}),
			async execute(_id, p) {
				const { broker, actor } = getContext();
				if (p.action === "admit")
					return result(
						await broker.admitRole({
							actor,
							roleId: p.roleId,
							capabilities: p.capabilities,
							private: p.private,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "detach")
					return result(
						await broker.detachRole({
							actor,
							roleId: p.roleId,
							runId: p.runId,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				return result(
					await broker.rebindRole({
						actor,
						roleId: p.roleId,
						runId: p.runId!,
						idempotencyKey: p.idempotencyKey,
					}),
				);
			},
		});
}

/** Actor identity is supplied by the authenticated transport/wiring, never tool arguments. */
export function registerCoordinationTools(
	pi: ExtensionAPI,
	getContext: () => CoordinationToolContext,
	enabled: () => boolean,
) {
	const denied = new Set(
		(process.env.PI_DENY_TOOLS ?? "")
			.split(",")
			.map((s) => s.trim())
			.filter(Boolean),
	);
	const guard = () => {
		if (!enabled()) throw new Error("Coordination is unavailable in this session.");
		return getContext();
	};
	if (!denied.has("subagent_signal"))
		registerTool(pi, {
			name: "subagent_signal",
			label: "Coordination Signal",
			description:
				"Emit, acknowledge, resolve or supersede signals. Mutation of an existing signal requires signalId and expectedRevision. supersede requires summary and optionally replaces targets/requiresAck/kind/severity/artifactRefs. Targets route notifications; explicit task targets limit execution gates to those tasks. Acknowledge the ready signal requiring your role, not an ordinary channel message. Actor identity is authenticated.",
			promptSnippet: "Emit, acknowledge, or resolve typed coordination workflow signals.",
			promptGuidelines: [
				"Use typed signals for blockers, readiness, GO/HOLD, decisions, corrections, reviews, and acknowledgements; prose labels do not change state.",
			],
			parameters: Type.Object({
				action: StringEnum(["emit", "acknowledge", "resolve", "supersede"] as const),
				idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
				signalId: Type.Optional(Type.String()),
				expectedRevision: Type.Optional(Type.Integer()),
				kind: Type.Optional(
					StringEnum([
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
					] as const),
				),
				severity: Type.Optional(StringEnum(["info", "action", "blocking", "critical"] as const)),
				summary: Type.Optional(Type.String({ minLength: 1, maxLength: 500 })),
				targets: Type.Optional(
					Type.Array(
						Type.Object({
							roleId: Type.Optional(Type.String()),
							channelId: Type.Optional(Type.String()),
							taskId: Type.Optional(Type.String()),
						}),
						{ maxItems: 100 },
					),
				),
				requiresAck: Type.Optional(Type.Array(Type.String(), { maxItems: 100 })),
				artifactRefs: Type.Optional(
					Type.Array(
						Type.Object({
							artifactId: Type.String(),
							version: Type.Integer({ minimum: 1 }),
							digest: Type.String(),
						}),
						{ maxItems: 100 },
					),
				),
			}),
			async execute(_id, p) {
				const { broker, actor } = guard();
				if (p.action === "emit" || p.action === "supersede") {
					const emitted =
						p.action === "supersede"
							? await broker.supersedeSignal({
									actor,
									signalId: p.signalId!,
									expectedRevision: p.expectedRevision!,
									summary: p.summary!,
									targets: p.targets,
									requiresAck: p.requiresAck,
									kind: p.kind,
									severity: p.severity,
									artifactRefs: p.artifactRefs,
									idempotencyKey: p.idempotencyKey,
								})
							: await broker.emitSignal({
									actor,
									kind: p.kind ?? "attention",
									severity: p.severity ?? "info",
									summary: p.summary ?? p.kind ?? "signal",
									targets: p.targets,
									requiresAck: p.requiresAck,
									artifactRefs: p.artifactRefs,
									idempotencyKey: p.idempotencyKey,
								});
					const signal = emitted.event!.payload as any;
					const targetRoles = await broker.signalRecipients({ actor, signalId: signal.id });
					for (const roleId of targetRoles.filter(
						(roleId) => roleId !== actor.roleId || signal.requiresAck.includes(roleId),
					))
						await broker.queueSignalDelivery({
							actor,
							signalId: signal.id,
							targetRoleId: roleId as string,
							idempotencyKey: `delivery:${createHash("sha256")
								.update(JSON.stringify([p.idempotencyKey, roleId]))
								.digest("hex")}`,
						});
					return render(emitted);
				}
				if (p.action === "acknowledge")
					return render(
						await broker.acknowledgeSignal({
							actor,
							signalId: p.signalId!,
							expectedRevision: p.expectedRevision!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				return render(
					await broker.resolveSignal({
						actor,
						signalId: p.signalId!,
						expectedRevision: p.expectedRevision!,
						idempotencyKey: p.idempotencyKey,
					}),
				);
			},
		});
	if (!denied.has("subagent_artifact"))
		registerTool(pi, {
			name: "subagent_artifact",
			label: "Coordination Artifact",
			description:
				"Normal output/correction: use publish with artifactId and sourcePath. Storage and next version are automatic; re-publish the SAME artifactId for corrected bytes, then submit the returned frozen ref. No supersede call is needed. Paths resolve from your working directory. Optional version asserts the next version; reason records provenance; storeDir overrides storage. Fast producers can publish; strict mode requires reviewer. Advanced: seal, freeze(version). supersede/invalidate are privileged reviewer/coordinator consumer-control actions, NOT replacement uploads.",
			promptSnippet: "Manage immutable coordination artifacts.",
			promptGuidelines: [
				"Protected work references exact frozen content-addressed versions, never mutable paths.",
			],
			parameters: Type.Object({
				action: StringEnum(["publish", "seal", "freeze", "supersede", "invalidate"] as const),
				idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
				artifactId: Type.String(),
				version: Type.Optional(Type.Integer()),
				previousVersion: Type.Optional(Type.Integer()),
				successorVersion: Type.Optional(Type.Integer()),
				sourcePath: Type.Optional(Type.String()),
				storeDir: Type.Optional(Type.String()),
				policy: Type.Optional(
					StringEnum(["cancel", "finish_as_invalid", "audited_override"] as const),
				),
				reason: Type.Optional(Type.String({ minLength: 1, maxLength: 1000 })),
			}),
			async execute(_id, p, _signal, _update, ctx) {
				const { broker, actor } = guard();
				if (p.action === "seal" || p.action === "publish") {
					if (!p.sourcePath?.trim())
						throw new Error("Publication requires sourcePath; storage and version are automatic.");
					const cwd = ctx?.cwd ?? process.cwd();
					return render(
						await broker.sealArtifact({
							actor,
							artifactId: p.artifactId,
							sourcePath: resolve(cwd, p.sourcePath),
							...(p.storeDir ? { storeDir: resolve(cwd, p.storeDir) } : {}),
							version: p.version,
							reason: p.reason,
							...(p.action === "publish" ? { freeze: true } : {}),
							idempotencyKey: p.idempotencyKey,
						}),
					);
				}
				if (p.action === "invalidate")
					return render(
						await broker.invalidateArtifact({
							actor,
							artifactId: p.artifactId,
							version: p.version!,
							policy: p.policy ?? "cancel",
							reason: p.reason!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "freeze")
					return render(
						await broker.freezeArtifact({
							actor,
							artifactId: p.artifactId,
							version: p.version!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				return render(
					await broker.supersedeArtifact({
						actor,
						artifactId: p.artifactId,
						previousVersion: p.previousVersion!,
						successorVersion: p.successorVersion!,
						policy: p.policy ?? "cancel",
						reason: p.reason,
						idempotencyKey: p.idempotencyKey,
					}),
				);
			},
		});
	if (!denied.has("subagent_permit"))
		registerTool(pi, {
			name: "subagent_permit",
			label: "Coordination Permit",
			description:
				"Prefer prepare: atomically reserve+attach+issue and arm in one call (taskId, expectedRevision, budgetId, operation). Fast workers can replenish an already assigned budget pool. Alternatively reserve budget, issue a single-use permit, then arm it before the protected tool. reserve optionally attaches the new lease atomically to taskId with expectedRevision (returns updated task revision). Otherwise pass the returned reservation in task create/ready leases. arm and legacy dispatch only arm; the execution hook dispatches. reconcile requires reason.",
			promptSnippet: "Manage broker-enforced protected execution permits.",
			promptGuidelines: [
				"Use subagent_permit prepare for configured protected operations only; ordinary shell work is unprotected by default in fast mode. The execution hook dispatches and settles; unknown outcomes stay charged.",
			],
			parameters: Type.Object({
				action: StringEnum([
					"prepare",
					"create_budget",
					"reserve",
					"release",
					"issue",
					"arm",
					"dispatch",
					"settle",
					"reconcile",
				] as const),
				idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }),
				budgetId: Type.Optional(Type.String()),
				limit: Type.Optional(Type.Integer()),
				taskId: Type.Optional(Type.String()),
				expectedRevision: Type.Optional(Type.Integer({ minimum: 1 })),
				reservationId: Type.Optional(Type.String()),
				permitId: Type.Optional(Type.String()),
				operation: Type.Optional(Type.String()),
				reason: Type.Optional(Type.String({ minLength: 1, maxLength: 1000 })),
				outcome: Type.Optional(
					StringEnum([
						"settled",
						"outcome_unknown",
						"expired",
						"cancelled_before_dispatch",
					] as const),
				),
			}),
			async execute(_id, p) {
				const { broker, actor, armPermit, protectedOperations } = guard();
				if (p.action === "prepare") {
					if (!armPermit) throw new Error("Protected execution is unavailable in this session.");
					if (protectedOperations && !protectedOperations.includes(p.operation!))
						throw new Error("Operation is not protected by this session; invoke it normally.");
					const prepared = await broker.preparePermit({
						actor,
						taskId: p.taskId!,
						expectedRevision: p.expectedRevision!,
						budgetId: p.budgetId!,
						operation: p.operation!,
						idempotencyKey: p.idempotencyKey,
					});
					await armPermit(prepared.event.payload.id, p.operation!);
					return render(prepared);
				}
				if (p.action === "create_budget")
					return render(
						await broker.createBudget({
							actor,
							budgetId: p.budgetId!,
							limit: p.limit!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "reserve")
					return render(
						await broker.reserveBudget({
							actor,
							budgetId: p.budgetId!,
							taskId: p.taskId,
							expectedRevision: p.expectedRevision,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "release")
					return render(
						await broker.releaseBudget({
							actor,
							budgetId: p.budgetId!,
							reservationId: p.reservationId!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "issue")
					return render(
						await broker.issuePermit({
							actor,
							taskId: p.taskId!,
							budgetId: p.budgetId!,
							reservationId: p.reservationId!,
							operation: p.operation ?? "protected",
							idempotencyKey: p.idempotencyKey,
						}),
					);
				if (p.action === "arm" || p.action === "dispatch") {
					const arm = guard().armPermit;
					if (!arm) throw new Error("Protected execution is unavailable in this session.");
					return render(await arm(p.permitId!, p.operation!));
				}
				if (p.action === "reconcile")
					return render(
						await broker.reconcilePermit({
							actor,
							permitId: p.permitId!,
							outcome: p.outcome ?? "settled",
							reason: p.reason!,
							idempotencyKey: p.idempotencyKey,
						}),
					);
				return render(
					await broker.settlePermit({
						actor,
						permitId: p.permitId!,
						outcome: p.outcome ?? "settled",
						idempotencyKey: p.idempotencyKey,
					}),
				);
			},
		});
	if (!denied.has("subagent_checkpoint"))
		registerTool(pi, {
			name: "subagent_checkpoint",
			label: "Coordination Checkpoint",
			description:
				"Persist a bounded recovery checkpoint; restored roles remain offline until explicitly rebound.",
			parameters: Type.Object({ idempotencyKey: Type.String({ minLength: 1, maxLength: 200 }) }),
			async execute(_id, p) {
				const { broker, actor } = guard();
				return render(await broker.checkpoint({ actor, idempotencyKey: p.idempotencyKey }));
			},
		});
}
