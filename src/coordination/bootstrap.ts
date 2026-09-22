import { buildDigest } from "./delivery.ts";
import { redactProjection } from "./policy.ts";
import type { CoordinationProjection } from "./protocol.ts";

export const MAX_BOOTSTRAP_BYTES = 12_000;

export function buildCoordinationBootstrap(
	projection: CoordinationProjection,
	roleId: string,
	cursor = 0,
	workflowMode: "fast" | "strict" = "strict",
): string {
	const view = redactProjection(projection, roleId);
	const role = view.roles[roleId];
	if (!role) throw new Error(`Unknown coordination role ${roleId}`);
	const tasks = Object.values(view.tasks).filter(
		(task) => task.owner === roleId && !["completed", "failed", "cancelled"].includes(task.state),
	);
	const channels = Object.values(view.channels).filter(
		(channel) =>
			channel.status === "open" && channel.members.some((member) => member.roleId === roleId),
	);
	const peerChannels = channels.filter((channel) => channel.review?.reviewTaskId);
	const peerReviewer = peerChannels.some((channel) => channel.review!.reviewerRoleId === roleId);
	const peerProducer = peerChannels.some((channel) =>
		channel.review!.taskIds.some((id) => view.tasks[id]?.owner === roleId),
	);
	const blockers = Object.values(view.signals).filter(
		(signal) =>
			signal.state === "open" &&
			(signal.requiresAck.includes(roleId) ||
				signal.targets.some(
					(target) =>
						target.roleId === roleId ||
						(target.taskId && tasks.some((task) => task.id === target.taskId)) ||
						(target.channelId && channels.some((channel) => channel.id === target.channelId)),
				)),
	);
	const digest = buildDigest(view, roleId, cursor, 12);
	const lines: string[] = [];
	let bytes = 0;
	let omitted = 0;
	const footerReserve = 256;
	const add = (line: string) => {
		const safe = line.replace(/[\u0000-\u001f\u007f-\u009f]/g, " ");
		const size = Buffer.byteLength(safe) + 1;
		if (size > 2000 || bytes + size > MAX_BOOTSTRAP_BYTES - footerReserve) {
			omitted++;
			return;
		}
		lines.push(safe);
		bytes += size;
	};
	add("Coordination bootstrap.");
	add(
		`Identity: roleId=${role.roleId}; runId=${role.runId ?? "offline"}; generation=${role.generation}.`,
	);
	if (workflowMode === "fast")
		add(
			"Fast workflow: spawn binds/starts ungated assignments; do not duplicate setup. Ordinary work needs no lease or permission (shell is unprotected by default); use subagent_permit prepare only for configured protected operations. Holds/dependencies still apply. Read bound contracts and respect file ownership. Publish freezes outputs; submit with output refs, verification and nextActions:[] when none. Approval is independent; never self-approve. Stop after handoff. Do not poll, echo receipts, narrate transitions or duplicate final summaries.",
		);
	else
		add(
			"Rules: use subagent_channel for shared context, not fake channel prefixes. Use subagent_signal for blockers/readiness/decisions/corrections and explicit critical acknowledgements. Do not start a task until subagent_task accepts start. Use subagent_artifact to seal and freeze exact inputs. Use subagent_permit to reserve, issue and arm each protected attempt; the execution hook consumes it. Unknown outcomes stay charged. Stop after an accepted handoff. Complete only with current output refs, verification evidence, satisfied review and explicit next actions.",
		);
	if (peerProducer || peerReviewer) {
		add(
			"Peer lifecycle: ask/answer real questions and share findings in the channel below; peers wake directly, without coordinator relay. Default channel delivery wakes peers; digest is FYI. End your turn when waiting or after submission: the runtime parks you and closes you when released. No polling, manual resume or subagent_done is needed. Escalate failed runs/exhausted correction limits with a targeted blocked signal. Do not acknowledge signals unless requiresAck names you.",
		);
		if (peerProducer)
			add(
				"Your producer workflow: implement/test owned files, publish each output, submit frozen refs + verification, then end the turn. Stay available for review via automatic park/wake. On changes_requested: call subagent_task start with the current revision BEFORE correcting/resubmitting; no ready step is needed in fast mode. Publish corrected files under the SAME artifactId for automatic next versions, then submit current refs. Do NOT call artifact supersede/invalidate: those are privileged consumer-control operations, not replacement uploads.",
			);
		if (peerReviewer)
			add(
				"Your reviewer workflow: discuss interfaces/test coverage while gated; wait for automatic Review GO. The broker starts your assignment and binds exact source refs—do not start/bind it yourself. Verify bytes and tests; approve sources or request genuine owner-specific corrections within the limit. Decisions require reason and current revision; approve also needs nextActions:[] when none. Reasons are delivered automatically; do not repeat them. End the turn for renewed GO after corrections. Once all sources are approved, publish/submit your own report without reviewSatisfied:true. Independent coordinator approval follows; report submission releases producers.",
			);
	} else {
		add(
			"Run closure: after submitting frozen outputs and verification, finish your final summary and exit; do not wait for review approval. Autonomous assignments auto-exit at settlement; explicit/manual sessions use subagent_done. Review starts from frozen submissions/Review GO, never from process-exit notifications.",
		);
		if (role.capabilities.includes("reviewer"))
			add(
				"Review: approve/changes_requested requires reason and current revision; fast approve also needs explicit nextActions ([] if none) and completes atomically. Review-channel decisions post their reasons to peers automatically; no duplicate handoff is needed.",
			);
	}
	if (role.capabilities.includes("coordinator"))
		add(
			"Team setup: declare the shared review channel, then launch authorized roles with short task briefs (outcome + owned files), model authorization and frozen inputs. Normally omit systemPrompt. Peer channels provide coordination instructions automatically; no long per-agent protocol prompt or ordinary relay/resume is needed.",
		);
	add(
		`Cursor: ${view.seq}. Tasks=${tasks.length}; channels=${channels.length}; pending signals=${blockers.length}.`,
	);
	for (const signal of blockers)
		add(
			`Pending: ${signal.id} ${signal.kind}/${signal.severity} rev=${signal.revision}; requiresAck=${signal.requiresAck.join(",") || "none"}.`,
		);
	for (const task of tasks) {
		add(
			`Task: ${task.id} (${task.title}) ${task.state} rev=${task.revision}; dependencies=${task.dependsOn.join(",") || "none"}; leases=${task.leases.join(",") || "none"}.`,
		);
		for (const ref of task.requiredInputs)
			add(`Input for ${task.id}: ${ref.artifactId}@${ref.version} sha256=${ref.digest}.`);
	}
	for (const budget of Object.values(view.budgets))
		add(
			`Budget: ${budget.id} used=${budget.used}/${budget.limit}; outcome_unknown=${budget.dispatchedUnknown}.`,
		);
	for (const channel of channels) {
		add(
			`Channel: #${channel.name} (${channel.id}) purpose=${channel.purpose}; rev=${channel.revision}; delivery=${channel.delivery}.`,
		);
		if (channel.review?.reviewTaskId) {
			add(
				`Peers in ${channel.id}: ${channel.members
					.filter((m) => m.mode === "participate")
					.map((m) => m.roleId)
					.join(", ")}.`,
			);
			add(
				`Peer review assignment: ${channel.review.reviewTaskId}; correction limit per source=${channel.review.maxCorrections ?? 2}.`,
			);
			for (const id of channel.review.taskIds)
				add(`Source task: ${id}; owner=${view.tasks[id]?.owner ?? "not assigned yet"}.`);
		}
		if (channel.review)
			add(
				`Review group: ${channel.id}; reviewer=${channel.review.reviewerRoleId}; tasks=${channel.review.taskIds.join(",")}; status=${channel.readiness?.ready ? "GO" : "waiting for frozen submissions"}. Review decisions are delivered to this channel automatically.`,
			);
	}
	for (const line of digest.lines) add(`Event: ${line}`);
	omitted += digest.truncated;
	lines.push(
		`Digest cursor: ${digest.cursor}. ${omitted ? `${omitted} entries omitted. ` : ""}Use subagent_team for scoped state and paginated changes; never poll for completion.`,
	);
	return lines.join("\n");
}
