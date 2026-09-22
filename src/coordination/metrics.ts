import { readFileSync } from "node:fs";

export interface CoordinationReplayMetrics {
	parentRelays: number;
	fakeChannelPrefixes: number;
	supersededFacts: number;
	invalidProtectedDispatches: number;
	wakeCount: number;
	timeToUnblock: number;
	budgetUnknownReuses: number;
	channelRecreationsNeededForMembership: number;
}
export const EMPTY_REPLAY_METRICS: CoordinationReplayMetrics = Object.freeze({
	parentRelays: 0,
	fakeChannelPrefixes: 0,
	supersededFacts: 0,
	invalidProtectedDispatches: 0,
	wakeCount: 0,
	timeToUnblock: 0,
	budgetUnknownReuses: 0,
	channelRecreationsNeededForMembership: 0,
});

export function computeCharacterizationMetrics(fixture: any): CoordinationReplayMetrics {
	const metrics = { ...EMPTY_REPLAY_METRICS };
	const events = Array.isArray(fixture?.events) ? fixture.events : [];
	let firstBlocked = 0;
	let firstUnblocked = 0;
	for (const event of events) {
		const text = String(event.text ?? "");
		if (event.from === "coordinator" && event.kind === "v1.message") metrics.parentRelays++;
		if (/^\[#[-\w]+\]/.test(text)) metrics.fakeChannelPrefixes++;
		if (event.supersedes || /CORRECTION|superseded/i.test(text)) metrics.supersededFacts++;
		if (event.kind === "v1.protected_dispatch" && event.valid === false)
			metrics.invalidProtectedDispatches++;
		if (event.kind === "v1.message" || event.kind === "v1.delivery_uncertain") metrics.wakeCount++;
		if (event.kind === "v1.budget_reserve" && event.valid === false) metrics.budgetUnknownReuses++;
		if (/membership is stale|old-reviewer/.test(JSON.stringify(event)))
			metrics.channelRecreationsNeededForMembership = 1;
		if (!firstBlocked && /BLOCKER|blocked/i.test(text)) firstBlocked = Number(event.t ?? 0);
		if (!firstUnblocked && /GO|unblocked/i.test(text)) firstUnblocked = Number(event.t ?? 0);
	}
	metrics.timeToUnblock =
		firstBlocked && firstUnblocked ? Math.max(0, firstUnblocked - firstBlocked) : 0;
	return metrics;
}

export interface ReplayObservation {
	kind:
		| "dispatch"
		| "wake"
		| "relay"
		| "direct_message"
		| "supersession"
		| "unknown_reuse"
		| "channel_recreation"
		| "unblocked";
	valid?: boolean;
	text?: string;
	duration?: number;
}
export function computeReplayMetrics(
	observations: readonly ReplayObservation[],
): CoordinationReplayMetrics {
	if (!Array.isArray(observations))
		throw new Error(
			"Replay metrics require observed replay operations, not expected fixture outcomes.",
		);
	const metrics = { ...EMPTY_REPLAY_METRICS };
	for (const observation of observations) {
		if (observation.kind === "dispatch" && observation.valid === false)
			metrics.invalidProtectedDispatches++;
		if (observation.kind === "wake") metrics.wakeCount++;
		if (observation.kind === "relay") metrics.parentRelays++;
		if (observation.kind === "direct_message" && /^\\[#[-\\w]+\\]/.test(observation.text ?? ""))
			metrics.fakeChannelPrefixes++;
		if (observation.kind === "supersession") metrics.supersededFacts++;
		if (observation.kind === "unknown_reuse") metrics.budgetUnknownReuses++;
		if (observation.kind === "channel_recreation") metrics.channelRecreationsNeededForMembership++;
		if (observation.kind === "unblocked") metrics.timeToUnblock += observation.duration ?? 0;
	}
	return metrics;
}

export function loadCharacterizationFixture(path: string): any {
	return JSON.parse(readFileSync(path, "utf8"));
}
