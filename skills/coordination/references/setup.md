# Authorized team setup

Use current loaded tool schemas. Coordination mutations take `{request:{action:..., ...}}`; `subagent` and `subagent_team` remain flat. Mutation calls require a unique `idempotencyKey` per logical operation. Reuse a key only for the exact same request after inspecting an uncertain outcome—not for a changed request.

This is a recipe, not permission to spawn. Read the applicable delegation policy first. On this installation it currently requires explicit authorization and `openai-codex/gpt-5.6-sol` through the user's OpenAI account. Do not assume this example grants authorization or overrides a later policy.

## Select the appropriate pattern

- **Solo:** do the task directly. No roles, artifacts or channels solely for ceremony.
- **One isolated worker:** authorized delegation with a short brief and explicit model; coordinator verifies the submitted result. No retained peer-review group is required if no peer interaction is needed.
- **Peer implementation/review:** the recipe below. Use only as many owners as the work justifies, and count the reviewer in the authorized total.

## Prepare the shared contract

Publish one readable contract file using `subagent_artifact`:

```json
{"request":{"action":"publish","idempotencyKey":"WORK-contract","artifactId":"WORK-contract","sourcePath":"/absolute/path/to/contract.md"}}
```

Replace `WORK` with this work's unique scope and paths with actual paths. Retain the returned exact `{artifactId,version,digest}`; never invent a digest. The readable working copy must match it. The contract defines interfaces, acceptance criteria, ownership and common execution limits, not pages of tool instructions.

## Admit roles and create the peer channel

For each authorized implementation owner, use `subagent_role` with `action:"admit"`, its chosen role ID, `capabilities:["worker"]` and an operation-specific idempotency key. For the independent reviewer, use `["worker","reviewer"]`. Do not grant children coordinator capability. Role admission does not launch a process; `subagent` binds the run, so do not manually rebind first.

Use fresh identifiers or verify that intentional existing assignments are compatible. Role names normalize; task IDs do not. Example channel arguments for two owners:

```json
{
  "request": {
    "action": "create",
    "idempotencyKey": "WORK-channel",
    "name": "WORK-integration",
    "purpose": "Resolve shared interfaces and integration findings directly",
    "members": [
      {"roleId":"coordinator","mode":"watch"},
      {"roleId":"WORK-storage","mode":"participate"},
      {"roleId":"WORK-domain","mode":"participate"},
      {"roleId":"WORK-reviewer","mode":"participate"}
    ],
    "review": {
      "taskIds": ["WORK-storage-task","WORK-domain-task"],
      "reviewerRoleId": "WORK-reviewer",
      "reviewTaskId": "WORK-review-task",
      "maxCorrections": 2
    }
  }
}
```

Lowercase `WORK` when choosing actual role IDs, and use the canonical IDs returned by admission. Create the channel before launching. Source task IDs may be declared before their assignments exist. The reviewer must be independent of all source owners; all owners/reviewer must participate. The optional correction limit is per source (0–10, default 2), not an authorization for new runs.

## Launch during setup

Each `subagent` call needs:

- `name`: human-readable role name.
- `role`: the admitted canonical role ID.
- `taskId`: its source task ID, or the channel's exact review task ID.
- `task`: short outcome, owned files and any application-specific dependency.
- `model`: the exact authorized provider-qualified model, always explicit.
- `cwd`: the actual working directory.
- `requiredInputs`: the exact frozen contract ref returned by publication.

Normally omit `agent`, `systemPrompt`, `fork`, `interactive`, `autoExit`, `skills` and `tools`. Bare runs use autonomous defaults; optional presets can change defaults and introduce unnecessary instructions. Respect explicit lifecycle/tool restrictions when the user has requested them.

Example brief—not a fixed application template:

> Implement storage.mjs and storage.test.mjs against /absolute/path/to/contract.md. Own those files; resolve persistence-boundary questions directly with Domain.

Reviewer brief:

> Own integration.test.mjs and REVIEW.md. Independently verify Storage and Domain against /absolute/path/to/contract.md, discuss integration risks, and report the verified result.

Include exact frozen inputs separately from the brief. Do not copy the contract or lifecycle into `task`. Prefer `task`; the supported `systemPrompt`-only compatibility path is recovery for a misfiled brief, not the recommended API. A task ID is not a task description.

Launch all authorized participants as part of setup; do not defer reviewer spawning until a coordinator sees GO. Launch order is flexible. Spawn creates/binds the assignments, starts ungated sources, and leaves early reviewers gated. No duplicate task creation, manual start or input-binding call is needed for this pattern.

## After setup

The runtime handles submission-driven binding/activation, parked runs, direct questions, same-run corrections, renewed GO and release. The coordinator need not relay, start, resume or acknowledge routine activity.

When the reviewer submits its final report, independently verify the result and approve that report with the current revision, a substantive reason, explicit `nextActions` (`[]` if none), and an idempotency key. Do not supply `reviewSatisfied:true` in a child submission or approve source tasks already handled by the independent reviewer.

For an unusual dependency graph or strict mode, consult the [runtime documentation](../../../docs/coordination.md) rather than guessing call shapes or silently changing configuration.
