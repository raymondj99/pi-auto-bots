---
name: coordination
description: Plan and coordinate substantial multi-part implementation, investigation, or review from an ordinary user request. Use before deciding whether delegation is worthwhile, and before setting up an authorized coordination team. Select solo work or a minimal team, define ownership/interfaces, create peer channels and let the runtime handle review/corrections. Coordinator only; this skill does not authorize spawning.
---

# Coordination from an ordinary request

The user supplies an outcome, not an orchestration script. You choose an economical workflow and handle setup. Children receive short work briefs; their runtime supplies coordination mechanics. Do not ask the user to design roles, channels, task IDs or tool calls.

## 1. Decide whether a team helps

Inspect the relevant project instructions, code and current work before planning ownership. Prefer doing the work yourself when it is small, tightly coupled, unclear, or cheaper than communicating a contract. Do not spawn agents just because the tool exists.

Delegate only substantial, separable work with clear outputs. Examples:
- One focused worker, with coordinator verification, for an isolated delegated task.
- Independent implementation owners plus an independent reviewer when interfaces or integration genuinely need peer collaboration.
- Independent research workers with coordinator synthesis when retained peer review adds no value.

Choose domain-specific roles, not a mandatory worker count or a fixed Parser/Report/Reviewer template. Give each writable file one owner; serialize overlapping work rather than creating competing writers. Keep shared decisions explicit and route them to the people who need them.

## 2. Check authorization before setup

Read the applicable agent/delegation policy skill before choosing models or delegating. A build request, this skill, an example, a contract, or the availability of a tool is **not** spawn authorization.

- Use only the count, scope, roles and model/account explicitly authorized by the user or an applicable standing user policy. Reviewers count as agents. Resumes also need authorization under the current policy.
- If useful delegation lacks authorization, ask once with the proposed count, responsibilities and permitted model/account. Do not ask the user to supply coordination details. Work solo if delegation is unnecessary or declined.
- Always pass the exact permitted provider-qualified model explicitly. Never inherit a preset model or substitute another provider/account/CLI. If unavailable, report the blocker.
- Do not authorize nested delegation or extra runs yourself. Children should perform their assignments in their existing runs.

These rules do not establish or modify standing authorization. If you are already a child agent, follow your assignment and its peer bootstrap instead of orchestrating a new team.

## 3. Turn the outcome into a small shared contract

Record the necessary interfaces, acceptance criteria, file ownership, verification commands and unresolved decisions with a named decision owner. Do not invent requirements or pre-decide everything merely to avoid peer discussion. Ask the user only about genuine product ambiguity or authority boundaries.

For a peer workflow, publish the shared contract as a frozen artifact before launch. Include shared execution/authorization limits once in it, rather than repeating long policy prompts for every child. Keep coordination-only files in a temporary location unless the project expects them checked in. Never overwrite an existing project document or another owner's changes.

Provide children a readable working path for the contract as well as its exact frozen input ref; an artifact ID is not a filesystem path. They must check bytes against the ref when consuming that copy.

## 4. Set up once

Read [the setup recipe](references/setup.md) when creating a team; it contains current call shapes and a compact example.

For retained peer collaboration, use the **fast** workflow:
1. Choose fresh, consistent role/task identifiers for this work. Reuse a compatible existing assignment only when intentional; don't collide with completed work or bind over an active run.
2. Admit authorized owners and an independent reviewer with minimal capabilities.
3. Create the shared channel **before launching**, with every owner/reviewer participating and the coordinator watching. Declare source task IDs, reviewer role, `reviewTaskId` and a small correction limit.
4. Launch each authorized participant once during setup, with explicit role, task ID, model, working directory, frozen inputs and a short `task` brief. Spawn creates/binds assignments; do not duplicate that work.

Use outcome + owned files + task-specific dependency in each brief. Do not copy this skill or the protocol into child prompts. Normally omit `systemPrompt`, agent presets and redundant lifecycle flags. Do not use conversation forks just to transmit coordination instructions. The runtime supplies peers, input gates, producer/reviewer procedures, publication/correction instructions and park/wake behavior.

Reviewer launch is part of setup, not a coordinator callback waiting for GO. The runtime retains launched runs; it does not schedule future launches or invent permission to recover failed processes. If fast-workflow support is unavailable, report the limitation or choose a permitted simpler workflow. Do not silently install, reconfigure or reload anything.

## 5. Let the team progress

- Peers ask and answer substantive questions in their shared channel. Default peer delivery wakes participants; digest is FYI. Avoid status chatter, receipt echoes and duplicate handoffs.
- Producers publish and submit. Valid frozen submissions automatically activate the reviewer with exact source refs.
- Corrections wake their owners; corrected submissions automatically activate re-review in the same runs. Producers remain available until the review report is submitted.
- End your turn or perform independent work while waiting. No polling, repeated status queries, coordinator message relays, manual review start/bind/GO, or routine permission requests.
- Do not acknowledge signals unless `requiresAck` names you. Do not approve your own work or weaken gates to achieve a green result.

Read [the exceptions guide](references/exceptions.md) only when a real error or authority exception requires action. Use scoped state queries to answer specific questions, never as a completion loop.

## 6. Independently finish

After the final report is submitted, verify the actual result against the user's acceptance criteria, project-required checks and frozen evidence. Do not duplicate the reviewer's source approvals; independently approve the review report only after verification. Process closure is not approval.

Summarize the outcome, verification and any remaining risk concisely. Separate application correctness, autonomous collaboration and protocol failures. Report rejected calls, user intervention or unexercised paths honestly; eventual recovery does not erase them. Keep private transcripts, credentials and session identities out of published evidence.

Never commit, push, deploy, expand scope or spend additional delegated runs merely because the team finished successfully. Follow the user's authorization for those actions separately.
