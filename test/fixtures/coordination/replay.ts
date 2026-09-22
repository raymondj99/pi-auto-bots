import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CoordinationBroker } from "../../../src/coordination/broker.ts";
import type { ReplayObservation } from "../../../src/coordination/metrics.ts";
import type {
	ArtifactRef,
	ArtifactStateRecord,
	BudgetState,
	CoordinationActor,
	DeliveryRecord,
	PermitStateRecord,
	TaskStateRecord,
} from "../../../src/coordination/protocol.ts";

/** Model-free replay of the fixture's explicit failures. Prose remains informal;
 * no production blocker, acknowledgement or GO state is inferred from text.
 */
export function runCharacterizationReplay(fixture: { events: Array<Record<string, any>> }) {
	const directory = mkdtempSync(join(tmpdir(), "coordination-replay-"));
	const observations: ReplayObservation[] = [];
	const rejected: Array<{ time: number; code: string }> = [];
	const broker = new CoordinationBroker("replay", "branch");
	const coordinator = { roleId: "coordinator", runId: "run-coord-1", generation: 1 };
	let inference: CoordinationActor = {
		roleId: "inference",
		runId: "run-inference-1",
		generation: 1,
	};
	let input: ArtifactRef | undefined;
	let taskId = "unassigned";
	let reservationId = "unreserved";
	let activePermit: PermitStateRecord | undefined;
	let reviewChannel = "";
	let budgetCreated = false;
	broker.admitRole({
		roleId: coordinator.roleId,
		capabilities: ["coordinator"],
		idempotencyKey: "admit-coordinator",
	});
	broker.bindRole({
		roleId: coordinator.roleId,
		runId: coordinator.runId,
		idempotencyKey: "bind-coordinator",
	});
	for (const roleId of ["builder", "reviewer", "inference", "old-reviewer"]) {
		broker.admitRole({
			actor: coordinator,
			roleId,
			capabilities: ["worker"],
			idempotencyKey: `admit-${roleId}`,
		});
		broker.bindRole({ roleId, runId: `run-${roleId}-1`, idempotencyKey: `bind-${roleId}` });
	}
	broker.setDeliverySink((delivery) => {
		if (delivery.wakeCount > 0) observations.push({ kind: "wake" });
	});
	const reserve = (key: string) => {
		if (!budgetCreated) {
			broker.createBudget({
				actor: coordinator,
				budgetId: "gpu-calls",
				limit: 1,
				idempotencyKey: "budget",
			});
			budgetCreated = true;
		}
		const budget = broker.reserveBudget({
			actor: coordinator,
			budgetId: "gpu-calls",
			idempotencyKey: key,
		}).event.payload as BudgetState;
		reservationId = Object.keys(budget.reservations).at(-1)!;
	};
	const createTask = (id: string) => {
		const task = broker.createTask({
			actor: coordinator,
			id,
			owner: "inference",
			title: "Replay protected attempt",
			requiredInputs: input ? [input] : [],
			idempotencyKey: `create-${id}`,
		}).event.payload as TaskStateRecord;
		taskId = task.id;
	};
	const dispatch = (time: number, valid: boolean) => {
		try {
			let task = broker.snapshot().tasks[taskId];
			if (task?.state === "draft" || task?.state === "blocked")
				task = broker.transitionTask({
					actor: inference,
					taskId,
					state: "ready",
					expectedRevision: task.revision,
					leases: [reservationId],
					idempotencyKey: `ready-${time}`,
				}).event.payload;
			if (task?.state === "ready")
				broker.transitionTask({
					actor: inference,
					taskId,
					state: "in_progress",
					expectedRevision: task.revision,
					idempotencyKey: `start-${time}`,
				});
			activePermit = broker.issuePermit({
				actor: inference,
				taskId,
				budgetId: "gpu-calls",
				reservationId,
				operation: "costly_call",
				idempotencyKey: `permit-${time}`,
			}).event.payload;
			broker.dispatchPermit({
				actor: inference,
				permitId: activePermit.id,
				operation: "costly_call",
				idempotencyKey: `dispatch-${time}`,
			});
			// This is the side-effect spy: it is reached only after real broker consumption.
			observations.push({ kind: "dispatch", valid });
		} catch (error) {
			rejected.push({ time, code: String((error as { code?: string }).code ?? "ERROR") });
		}
	};
	try {
		for (const event of fixture.events) {
			if (event.kind === "v1.message") {
				const control = event.signal as
					| { kind: "blocked"; severity: "blocking"; requiresAck: string[] }
					| undefined;
				const signal = broker.emitSignal({
					actor: coordinator,
					kind: control?.kind ?? "attention",
					severity: control?.severity ?? "info",
					summary: event.text,
					targets: [{ roleId: control ? "inference" : event.to }],
					requiresAck: control?.requiresAck,
					idempotencyKey: `context-${event.t}`,
				}).event.payload;
				const targetRoleId = control ? "inference" : event.to;
				broker.queueSignalDelivery({
					actor: coordinator,
					signalId: signal.id,
					targetRoleId,
					idempotencyKey: `context-delivery-${event.t}`,
				});
				if (control) {
					createTask("blocked-replacement-attempt");
					broker.transitionTask({
						actor: inference,
						taskId,
						expectedRevision: 1,
						state: "blocked",
						blockers: [signal.id],
						idempotencyKey: "explicit-blocker",
					});
				}
			} else if (event.kind === "v1.artifact_publish") {
				const previous = input;
				const source = join(directory, "source");
				writeFileSync(source, String(event.digest));
				const sealed = broker.sealArtifact({
					actor: coordinator,
					artifactId: "pack",
					sourcePath: source,
					storeDir: join(directory, "objects"),
					idempotencyKey: `seal-${event.t}`,
				}).event.payload as ArtifactStateRecord;
				input = { artifactId: sealed.artifactId, version: sealed.version, digest: sealed.digest };
				if (event.frozen)
					broker.freezeArtifact({
						actor: coordinator,
						artifactId: "pack",
						version: sealed.version,
						idempotencyKey: `freeze-${event.t}`,
					});
				if (previous) {
					broker.supersedeArtifact({
						actor: coordinator,
						artifactId: "pack",
						previousVersion: previous.version,
						successorVersion: input.version,
						policy: "cancel",
						idempotencyKey: `supersede-${event.t}`,
					});
					observations.push({ kind: "supersession" });
				} else {
					reserve("initial-reservation");
					createTask("consume-initial-pack");
				}
			} else if (event.kind === "v1.protected_dispatch") {
				dispatch(event.t, event.valid !== false);
			} else if (event.kind === "v1.channel_create") {
				reviewChannel = broker.createChannel({
					actor: coordinator,
					name: "review",
					purpose: "Shared artifact contract",
					members: [
						{ roleId: "builder", mode: "participate" },
						{ roleId: "old-reviewer", mode: "participate" },
					],
					idempotencyKey: "review-channel",
				}).event.payload.id;
				broker.updateChannel({
					actor: coordinator,
					channelId: reviewChannel,
					expectedRevision: 1,
					members: [
						{ roleId: "builder", mode: "participate" },
						{ roleId: "reviewer", mode: "participate" },
					],
					idempotencyKey: "replace-review-member",
				});
			} else if (event.kind === "v1.delivery_uncertain") {
				const signal = broker.emitSignal({
					actor: coordinator,
					kind: "hold",
					severity: "critical",
					summary: "Explicit replay hold",
					targets: [{ roleId: "inference" }],
					idempotencyKey: "hold",
				}).event.payload;
				const delivery = broker.queueSignalDelivery({
					actor: coordinator,
					signalId: signal.id,
					targetRoleId: "inference",
					idempotencyKey: "hold-delivery",
				}).event.payload as DeliveryRecord;
				// The model-free sink has injected the hold; confirm delivery separately.
				broker.receipt({
					actor: inference,
					deliveryId: delivery.id,
					state: "delivered",
					idempotencyKey: "hold-injected",
				});
				broker.queueSignalDelivery({
					actor: coordinator,
					signalId: signal.id,
					targetRoleId: "inference",
					idempotencyKey: "hold-retry",
				});
				broker.resolveSignal({
					actor: coordinator,
					signalId: signal.id,
					expectedRevision: 1,
					idempotencyKey: "hold-resolved",
				});
			} else if (event.kind === "v1.budget_reserve" && event.state === "dispatched") {
				createTask("valid-current-pack-attempt");
				dispatch(event.t, true);
			} else if (event.kind === "v1.disconnect") {
				if (activePermit)
					broker.settlePermit({
						actor: inference,
						permitId: activePermit.id,
						outcome: "outcome_unknown",
						idempotencyKey: "unknown",
					});
				broker.detachRole({ actor: coordinator, roleId: "inference", idempotencyKey: "detach" });
			} else if (event.kind === "v1.replacement") {
				broker.rebindRole({
					actor: coordinator,
					roleId: "inference",
					runId: event.newRunId,
					idempotencyKey: "rebind",
				});
				inference = { roleId: "inference", runId: event.newRunId, generation: 2 };
			} else if (event.kind === "v1.budget_reserve" && event.state === "reused_after_unknown") {
				try {
					reserve("illegal-reuse");
					observations.push({ kind: "unknown_reuse" });
				} catch (error) {
					rejected.push({ time: event.t, code: String((error as { code?: string }).code) });
				}
			}
		}
		const projection = broker.snapshot();
		return { observations, rejected, projection, reviewChannel };
	} finally {
		rmSync(directory, { recursive: true, force: true });
	}
}
