import assert from "node:assert/strict";
import {
	chmodSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	symlinkSync,
	truncateSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";
import {
	MAX_ARTIFACT_BYTES,
	sealContentAddressedArtifact,
	verifyArtifactBytes,
} from "../src/coordination/artifacts.ts";
import { CoordinationBroker } from "../src/coordination/broker.ts";
import { createProjection } from "../src/coordination/projection.ts";

it("sealing deduplicates content without overwriting an existing corrupted destination", () => {
	const directory = mkdtempSync(join(tmpdir(), "coordination-artifact-"));
	try {
		const actor = { roleId: "worker", runId: "run", generation: 1 };
		const firstPath = join(directory, "first"),
			secondPath = join(directory, "second");
		writeFileSync(firstPath, "same bytes");
		writeFileSync(secondPath, "same bytes");
		const params = {
			projection: createProjection(),
			actor,
			artifactId: "pack",
			storeDir: join(directory, "objects"),
		};
		const first = sealContentAddressedArtifact({ ...params, sourcePath: firstPath });
		const second = sealContentAddressedArtifact({ ...params, sourcePath: secondPath });
		assert.equal(first.path, second.path);
		chmodSync(first.path!, 0o600);
		writeFileSync(first.path!, "corrupt");
		assert.throws(() => sealContentAddressedArtifact({ ...params, sourcePath: secondPath }), {
			code: "ARTIFACT_TAMPERED",
		});
		assert.equal(readFileSync(first.path!, "utf8"), "corrupt");
		rmSync(first.path!);
		symlinkSync(firstPath, first.path!);
		assert.throws(() => verifyArtifactBytes(first), { code: "ARTIFACT_TAMPERED" });
		truncateSync(firstPath, MAX_ARTIFACT_BYTES + 1);
		assert.throws(() => sealContentAddressedArtifact({ ...params, sourcePath: firstPath }), {
			code: "BOUNDS_EXCEEDED",
		});
	} finally {
		rmSync(directory, { recursive: true, force: true });
	}
});

it("tampering after permit issuance is detected at consumption before dispatch", () => {
	const directory = mkdtempSync(join(tmpdir(), "coordination-consume-"));
	try {
		const b = new CoordinationBroker("session", "branch");
		const actor = { roleId: "coordinator", runId: "run", generation: 1 };
		b.admitRole({ roleId: actor.roleId, capabilities: ["coordinator"], idempotencyKey: "admit" });
		b.bindRole({ roleId: actor.roleId, runId: actor.runId, idempotencyKey: "bind" });
		const sourcePath = join(directory, "source");
		writeFileSync(sourcePath, "input bytes");
		const artifact = b.sealArtifact({
			actor,
			artifactId: "pack",
			sourcePath,
			storeDir: join(directory, "objects"),
			idempotencyKey: "seal",
		}).event.payload;
		b.freezeArtifact({ actor, artifactId: "pack", version: 1, idempotencyKey: "freeze" });
		b.createBudget({ actor, budgetId: "gpu", limit: 1, idempotencyKey: "budget" });
		const budget = b.reserveBudget({ actor, budgetId: "gpu", idempotencyKey: "reserve" }).event
			.payload;
		const reservationId = Object.keys(budget.reservations)[0];
		const task = b.createTask({
			actor,
			owner: actor.roleId,
			title: "Consume pack",
			requiredInputs: [{ artifactId: "pack", version: 1, digest: artifact.digest }],
			idempotencyKey: "task",
		}).event.payload;
		b.transitionTask({
			actor,
			taskId: task.id,
			expectedRevision: 1,
			state: "ready",
			leases: [reservationId],
			idempotencyKey: "ready",
		});
		b.transitionTask({
			actor,
			taskId: task.id,
			expectedRevision: 2,
			state: "in_progress",
			idempotencyKey: "start",
		});
		const permit = b.issuePermit({
			actor,
			taskId: task.id,
			budgetId: "gpu",
			reservationId,
			operation: "gpu",
			idempotencyKey: "permit",
		}).event.payload;
		chmodSync(artifact.path!, 0o600);
		writeFileSync(artifact.path!, "alter bytes");
		assert.throws(
			() =>
				b.dispatchPermit({
					actor,
					permitId: permit.id,
					operation: "gpu",
					idempotencyKey: "consume",
				}),
			{ code: "ARTIFACT_TAMPERED" },
		);
		assert.equal(b.snapshot().permits[permit.id].state, "reserved");
		assert.equal(b.snapshot().budgets.gpu.reservations[reservationId].state, "reserved");
	} finally {
		rmSync(directory, { recursive: true, force: true });
	}
});
