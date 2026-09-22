import { performance } from "node:perf_hooks";
import { CoordinationBroker } from "../../src/coordination/broker.ts";

const started = performance.now();
const broker = new CoordinationBroker("bench-session", "bench-branch", undefined, 1000);
broker.admitRole({
	roleId: "coordinator",
	capabilities: ["coordinator", "worker", "reviewer", "evaluator"],
	idempotencyKey: "role:coordinator",
});
broker.bindRole({
	roleId: "coordinator",
	runId: "run-coordinator",
	generation: 1,
	capabilities: ["coordinator", "worker", "reviewer", "evaluator"],
	idempotencyKey: "bind:coordinator",
});
const actor = { roleId: "coordinator", runId: "run-coordinator", generation: 1 };
for (let i = 1; i <= 100; i++) {
	broker.admitRole({
		actor,
		roleId: `role-${i}`,
		capabilities: ["worker"],
		idempotencyKey: `role:${i}`,
	});
	broker.bindRole({
		roleId: `role-${i}`,
		runId: `run-${i}`,
		generation: 1,
		idempotencyKey: `bind:${i}`,
	});
}
for (let i = 0; i < 32; i++)
	broker.createChannel({
		actor,
		name: `channel-${i}`,
		purpose: "bounded benchmark",
		members: Array.from({ length: 20 }, (_, member) => ({
			roleId: `role-${((i + member) % 100) + 1}`,
			mode: "participate" as const,
		})),
		idempotencyKey: `channel:${i}`,
	});
for (let i = 0; i < 100; i++)
	broker.createTask({
		actor,
		title: `task-${i}`,
		owner: `role-${(i % 100) + 1}`,
		idempotencyKey: `task:${i}`,
	});
for (let i = 0; i < 1000; i++)
	broker.emitSignal({
		actor,
		kind: "attention",
		severity: "info",
		summary: `routine-${i}`,
		targets: [{ roleId: `role-${(i % 100) + 1}` }],
		idempotencyKey: `signal:${i}`,
	});
const built = performance.now();
const snapshot = broker.snapshot();
const digestStart = performance.now();
let digestLines = 0;
for (let i = 1; i <= 100; i++)
	digestLines += broker.digest({ roleId: `role-${i}`, runId: `run-${i}`, generation: 1 }, 0, 20)
		.lines.length;
const finished = performance.now();
console.log(
	JSON.stringify({
		benchmark: "coordination-maxima",
		agents: Object.keys(snapshot.roles).length - 1,
		rolesIncludingCoordinator: Object.keys(snapshot.roles).length,
		channelMembers: 20,
		signals: Object.keys(snapshot.signals).length,
		tasks: Object.keys(snapshot.tasks).length,
		channels: Object.keys(snapshot.channels).length,
		retainedAudit: snapshot.audit.length,
		digestLines,
		timingsMs: {
			build: built - started,
			digestFanout: finished - digestStart,
			total: finished - started,
		},
	}),
);
