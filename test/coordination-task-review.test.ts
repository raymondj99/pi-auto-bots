import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import { CoordinationBroker } from "../src/coordination/broker.ts";

it("reviewers approve exact submitted outputs; workers cannot self-approve or swap evidence after approval", () => {
	const directory = mkdtempSync(join(tmpdir(), "coordination-review-"));
	try {
		const b = new CoordinationBroker("session", "branch");
		const coordinator = { roleId: "coordinator", runId: "coordinator", generation: 1 };
		const worker = { roleId: "worker", runId: "worker", generation: 1 };
		const reviewer = { roleId: "reviewer", runId: "reviewer", generation: 1 };
		b.admitRole({
			roleId: "coordinator",
			capabilities: ["coordinator"],
			idempotencyKey: "coordinator-admit",
		});
		b.bindRole({ roleId: "coordinator", runId: "coordinator", idempotencyKey: "coordinator-bind" });
		for (const actor of [worker, reviewer]) {
			b.admitRole({
				actor: coordinator,
				roleId: actor.roleId,
				capabilities: [actor.roleId],
				idempotencyKey: `${actor.roleId}-admit`,
			});
			b.bindRole({
				roleId: actor.roleId,
				runId: actor.runId,
				idempotencyKey: `${actor.roleId}-bind`,
			});
		}
		const sourcePath = join(directory, "output");
		writeFileSync(sourcePath, "verified output");
		const artifact = b.sealArtifact({
			actor: worker,
			artifactId: "output",
			sourcePath,
			storeDir: join(directory, "objects"),
			idempotencyKey: "seal",
		}).event.payload;
		b.freezeArtifact({
			actor: reviewer,
			artifactId: "output",
			version: 1,
			idempotencyKey: "freeze",
		});
		b.createBudget({
			actor: coordinator,
			budgetId: "resource",
			limit: 1,
			idempotencyKey: "budget",
		});
		const budget = b.reserveBudget({
			actor: coordinator,
			budgetId: "resource",
			idempotencyKey: "reserve",
		}).event.payload;
		const task = b.createTask({
			actor: coordinator,
			owner: "worker",
			title: "Produce verified output",
			idempotencyKey: "task",
		}).event.payload;
		b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: 1,
			state: "ready",
			leases: Object.keys(budget.reservations),
			idempotencyKey: "ready",
		});
		b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: 2,
			state: "in_progress",
			idempotencyKey: "start",
		});
		const outputs = [{ artifactId: "output", version: 1, digest: artifact.digest }];
		b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: 3,
			state: "submitted",
			outputs,
			verification: ["Tests passed"],
			idempotencyKey: "submit",
		});
		assert.throws(
			() =>
				b.reviewTask({
					actor: worker,
					taskId: task.id,
					expectedRevision: 4,
					decision: "approved",
					reason: "self approval",
					idempotencyKey: "self-approve",
				}),
			{ code: "UNAUTHORIZED" },
		);
		b.reviewTask({
			actor: reviewer,
			taskId: task.id,
			expectedRevision: 4,
			decision: "approved",
			reason: "Independently checked the frozen output",
			idempotencyKey: "approve",
		});
		assert.throws(
			() =>
				b.transitionTask({
					actor: worker,
					taskId: task.id,
					expectedRevision: 5,
					state: "completed",
					verification: ["Changed evidence"],
					nextActions: [],
					idempotencyKey: "swap-evidence",
				}),
			{ code: "INVALID_TRANSITION" },
		);
		assert.throws(
			() =>
				b.transitionTask({
					actor: worker,
					taskId: task.id,
					expectedRevision: 5,
					state: "completed",
					reviewSatisfied: true,
					nextActions: [],
					idempotencyKey: "self-approve-again",
				}),
			{ code: "UNAUTHORIZED" },
		);
		assert.throws(() =>
			b.transitionTask({
				actor: worker,
				taskId: task.id,
				expectedRevision: 5,
				state: "completed",
				outputs: [{ artifactId: "missing", version: 1, digest: "missing" }],
				nextActions: [],
				idempotencyKey: "swap-output",
			}),
		);
		const completed = b.transitionTask({
			actor: worker,
			taskId: task.id,
			expectedRevision: 5,
			state: "completed",
			nextActions: [],
			idempotencyKey: "complete",
		});
		assert.equal(completed.event.payload.state, "completed");
		assert.deepEqual(completed.event.payload.nextActions, []);
	} finally {
		rmSync(directory, { recursive: true, force: true });
	}
});
