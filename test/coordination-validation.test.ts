import assert from "node:assert/strict";
import { it } from "node:test";
import { CoordinationEventLog } from "../src/coordination/event-log.ts";
import { validEventPayload } from "../src/coordination/event-validation.ts";
import {
	createBudget,
	markReservationDispatched,
	releaseBudget,
	reserveBudget,
} from "../src/coordination/permits.ts";
import { createProjection } from "../src/coordination/projection.ts";
import { publicCoordinationError } from "../src/coordination/results.ts";

it("event payload validation rejects malformed states, accounting and unexpected credential fields before persistence", () => {
	let persisted = 0;
	const log = new CoordinationEventLog(createProjection("session", "branch"), {
		appendEntry() {
			persisted++;
		},
	});
	const actor = { roleId: "broker", runId: "broker", generation: 0 };
	for (const payload of [
		{ roleId: "worker", capabilities: "coordinator" },
		{ roleId: "worker", token: "secret" },
		{ roleId: "__proto__" },
		{ roleId: "worker", capabilities: [42] },
	]) {
		assert.throws(
			() => log.append("role.admitted", actor, payload, `invalid-${JSON.stringify(payload)}`),
			{ code: "BOUNDS_EXCEEDED" },
		);
	}
	assert.equal(persisted, 0);
	assert.equal(log.snapshot().seq, 0);
	assert.equal(validEventPayload("task.created", { id: "task" }), false);
	assert.equal(
		validEventPayload("budget.created", {
			id: "budget",
			limit: 1,
			used: 1,
			dispatchedUnknown: 0,
			reservations: JSON.parse('{"__proto__":{"amount":1,"state":"reserved"}}'),
		}),
		false,
	);
	assert.equal(
		validEventPayload("budget.created", {
			id: "budget",
			limit: 1,
			used: 0,
			dispatchedUnknown: 0,
			reservations: { charge: { amount: 1, state: "dispatched" } },
		}),
		false,
	);
	assert.throws(
		() =>
			log.append(
				"role.admitted",
				{ ...actor, generation: -1 },
				{ roleId: "worker" },
				"negative-generation",
			),
		{ code: "BOUNDS_EXCEEDED" },
	);
});

it("budget accounting invariants hold over deterministic randomized reserve/release/dispatch sequences", () => {
	for (let seed = 1; seed <= 20; seed++) {
		let random = seed;
		const next = () => {
			random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
			return random;
		};
		let budget = createBudget("gpu", 31);
		for (let index = 0; index < 150; index++) {
			const ids = Object.keys(budget.reservations);
			const reservationId = ids[next() % (ids.length || 1)];
			try {
				switch (next() % 3) {
					case 0:
						budget = reserveBudget(budget, 1 + (next() % 5), `reservation-${index}`);
						break;
					case 1:
						if (reservationId) budget = releaseBudget(budget, reservationId);
						break;
					case 2:
						if (reservationId)
							budget = markReservationDispatched(budget, reservationId, next() % 2 === 0);
						break;
				}
			} catch (error) {
				assert.ok(
					["BUDGET_EXHAUSTED", "LEASE_REQUIRED"].includes((error as { code: string }).code),
				);
			}
			assert.ok(validEventPayload("budget.reserved", budget));
			assert.ok(budget.used >= 0 && budget.used <= budget.limit);
			assert.ok(budget.dispatchedUnknown >= 0 && budget.dispatchedUnknown <= budget.used);
		}
	}
});

it("conflict errors expose stable codes and revisions, not private state", () => {
	const result = publicCoordinationError(
		Object.assign(new Error("Stale revision"), {
			code: "STALE_REVISION",
			current: {
				id: "task",
				revision: 7,
				state: "review",
				verification: ["PRIVATE_TRUTH"],
				path: "/private/input",
				checkpoint: "PRIVATE_PROMPT",
			},
		}),
	);
	assert.deepEqual(result, {
		error: "Stale revision",
		code: "STALE_REVISION",
		current: { id: "task", revision: 7, state: "review" },
	});
});
