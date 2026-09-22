# Coordination control plane

The coordination control plane replaces prose-only workflow coordination with typed,
enforceable state. It is event-sourced: every mutation appends an immutable, authenticated
envelope to a bounded ledger stored outside model context, and all live state is a
projection of that ledger.

Audience: anyone changing coordination behaviour, or debugging why the broker rejected a
call. For the day-to-day coordinator workflow, read `skills/coordination/SKILL.md` instead.

## Configuration

Copy `config.json.example` to `config.json`:

```json
{
  "coordination": {
    "workflowMode": "fast",
    "maxEvents": 1000,
    "maxDigestLines": 20,
    "maxPayloadBytes": 131072,
    "acknowledgementDeadlineMs": 300000,
    "permitTtlMs": 300000,
    "protectedOperations": ["costly_call", "deploy"],
    "defaultDelivery": "digest",
    "roleCapabilities": {}
  }
}
```

The parser rejects unknown keys and out-of-range bounds. Every key is optional; omitting
`config.json` entirely uses the defaults above.

`workflowMode` defaults to **fast**. Select `"strict"` for the lease-gated start protocol
and shell protection; omitting an explicit `protectedOperations` list then uses that
profile's defaults (`costly_call`, `shell`, `deploy`). An explicit list always wins.
`PI_SUBAGENT_COORDINATION_WORKFLOW_MODE` overrides the profile for tests and one-off runs;
children receive the coordinator's actual policy over their authenticated transport.

## Fast workflow

- **Spawn once.** Spawning binds the stable role and, if there is no existing assignment,
  creates the default task and starts it when ungated; otherwise it stays draft so the
  worker can receive its dependency. Existing assignments are reused, never duplicated or
  silently started across dependencies.
- For other work, `subagent_task create` with `start: true` validates and starts
  atomically, moving directly from draft or blocked to in-progress. No lease is needed for
  ordinary work; explicit dependencies, holds, acknowledgements and frozen input checks
  still apply.
- Shell and file work needs no coordination permission by default. Only configured
  protected **tool operations** are metered. This is not a shell-command classifier or an
  OS sandbox.
- `subagent_artifact publish` seals and freezes exact bytes in one event from `artifactId`,
  `sourcePath` and `idempotencyKey`. Storage and version allocation are automatic; optional
  `version` asserts the expected next version and optional `reason` records provenance.
  Relative paths resolve from the caller's working directory. Freezing is byte immutability,
  not review approval — workers still cannot self-approve completion.
- `subagent_permit prepare` collapses protected-work setup into one call: given task ID and
  revision, budget ID and operation, it reuses an unused assigned lease or atomically
  reserves, attaches and issues a permit, then arms it. No reservation is committed if issue
  validation fails; a failure during arming leaves a reserved, undispatched permit to retry
  or cancel.
- Authors may resolve their own typed signals in fast mode, with provenance retained in
  history; other actors still need reviewer authority. Required acknowledgements remain
  enforced.
- Send live decisions, blockers and handoffs through channels or role-targeted signals.
  Final assistant summaries are delivered automatically — do not send a duplicate
  final-summary message. There is no free-text `subagent_message` tool. Routine context is
  injected automatically; queries are for specific missing state, never polling.

Strict mode retains the stricter transition and privilege rules. `create start:true` and
`publish` also work there once all strict prerequisites are satisfied. The low-level broker
constructor defaults to strict; runtime wiring supplies the configured mode.

## Autonomous peer workflows

In fast mode, declare a peer workflow at channel creation:

```json
{
  "review": {
    "taskIds": ["parser-task", "report-task"],
    "reviewerRoleId": "reviewer",
    "reviewTaskId": "review-task",
    "maxCorrections": 1
  }
}
```

Admit the roles and include every owner and the reviewer as participating channel members.
Launch each authorized role once during setup, selecting the declared reviewer task ID.
These are dynamic role names, not agent presets. `reviewTaskId` opts into the peer
lifecycle; without it, the submission-notification behaviour below applies instead.

- Reviewers may launch before or after producers. Before all submissions exist they can
  discuss interfaces, then end their turn. The runtime stays alive and idle — no polling,
  model turn or cache warming is needed to wait. It retains **already launched** runs; it
  does not invent permission to spawn, schedule future launches or restart failed processes.
- Valid frozen submissions automatically bind the source outputs to the review assignment,
  preserve its base inputs and start it. **Review GO wakes the reviewer directly**, with no
  coordinator launch, start or bind call. Activation requires a current authorized run
  binding. Combined review inputs are bounded to 100 refs.
- `subagent_channel send` carries genuine questions, answers and findings. By default
  peer-channel messages wake participating peers, not the coordinator observer. Use
  `delivery: "digest"` for FYI context that need not start a turn. Typed signals and
  decisions retain control semantics; channel prose cannot approve or unblock a task.
- Submitted producers stay available until the final review report is submitted, even after
  their own approval. `changes_requested` posts the reviewer's owner-specific reason and
  wakes that owner, who starts, corrects, republishes and resubmits; the broker replaces the
  reviewer's old source bindings and issues renewed GO. No manual resume or coordinator relay
  is involved. `maxCorrections` bounds accepted correction rounds **per source task** (0–10,
  default 2).
- The designated reviewer must independently approve every source before submitting its
  report. Report submission releases producers for once-only closure; channel closure or
  report cancellation also releases them, without implying success. The report itself still
  needs independent coordinator approval. Non-coordinator roles cannot grant
  `reviewSatisfied: true` even with reviewer capability. Process exit never supplies it.
- Holds, current-byte checks, unsettled-attempt charges and generation fences remain.
  Exhausted correction bounds, crashed peers and invalid evidence are genuine exceptions to
  escalate, not reasons for polling or implicit spawn authorization. Explicit interactive
  sessions keep next-turn delivery and manual closure.

Child startup negotiates lifecycle support before the first task turn, queues early
reconnect traffic without prematurely starting a model, and refreshes the caller bootstrap
after compaction.

### Short briefs, not coordination scripts

Once the peer channel is declared, put only the desired outcome, file ownership and
application-specific constraints in `task`. The child bootstrap discovers its peers and
supplies its producer or reviewer procedure, correction recipe and park/wake behaviour.
Normally omit `systemPrompt`, `autoExit`, `interactive` and `fork` for bare autonomous runs;
explicit and named-preset lifecycle choices still win. Keep the authorized model explicit
where user policy requires it.

A sufficient brief looks like: *Implement parser.mjs and parser.test.mjs against the bound
contract; choose and publish ROW-SCHEMA.md and answer Report's schema question.* The
remaining fields identify role, task, working directory, model and frozen inputs.

A non-blank `systemPrompt` with no usable `task` is moved to `task` before validation, and
is not duplicated into the persona, so a preset identity cannot swallow the work brief. When
both are supplied they stay separate. Missing both fails clearly: `taskId` is an identifier,
not permission to invent instructions.

## Submission-driven review and autonomous closure

- Role references are canonicalized consistently at admission, channel membership, signal
  routing and launch (`Storage` becomes `storage`). Authenticated actors and task, artifact
  and channel IDs are never rewritten.
- `subagent` accepts explicit `taskId` and exact frozen `requiredInputs`. Inputs, access and
  bytes are checked before role rebinding and process launch; a newly created task binds
  them in its creation or start event. Existing assignments are reused only with matching
  inputs, never silently rebound after start. The launch receipt includes the canonical role
  and accepted assignment.
- Bare assignments default to autonomous auto-exit. Explicit `interactive`/`autoExit`
  settings, named presets and bare fork behaviour remain authoritative. Closure waits for
  `agent_settled`, not an intermediate `agent_end`; aborted, deferred or pending turns stay
  open. Outside configured peer workflows, workers submit then return normally without
  waiting for approval. In peer workflows, returning a turn parks the run until released.
- At channel creation, declare
  `review: {taskIds: ["storage-task", "domain-task"], reviewerRoleId: "reviewer"}`. Roles
  must already be admitted; named tasks may be created later. The reviewer and implementation
  owners must participate. A group is bounded to 20 distinct tasks; overlapping groups and
  reviewer removal are rejected.
- The broker persists one `review.readiness` event and a typed **Review GO** to the
  coordinator and reviewer once every named task has a valid frozen submission, verification,
  satisfied input gates and settled permits, verifying bytes before each new GO. GO is not an
  acknowledgement obligation or permission to spawn more agents.
- Readiness fingerprints suppress duplicate GO on approval, worker exit or replay.
  Corrections, supersession and gate changes withdraw readiness and resolve stale deliveries;
  a new valid submission can produce renewed GO. Complete-but-invalid submissions produce an
  actionable not-ready notice. Raw external file changes generate no broker events; start and
  approval still revalidate bytes.
- A reviewer scoped by the channel can read those source tasks' verification, not unrelated
  task evidence. `approve` and `changes_requested` atomically attach a reviewer-authored
  channel handoff containing the reason and affected task, so source owners receive queued
  peer deliveries and the dashboard retains the same notice. Approval cannot silently skip a
  closed handoff channel. Reviewers cannot approve their own report.

Mutation schemas use an object-root `{request: {action: ..., ...}}` with action-specific
required and allowed fields. `subagent`, `subagent_team` and other non-action tools stay
flat. Flat mutation shapes are still accepted, subject to current required fields, and
missing or unsupported fields are reported directly before generic union validation. All
approval calls include explicit `nextActions` (`[]` allowed).

## State and recovery

State is stored as bounded `subagent-coordination-event` custom session entries, outside
model context. Events carry monotonic sequence numbers, mandatory idempotency keys,
authenticated actors and immutable payloads. Reload reads only `SessionManager.getBranch()`,
restores projections offline, and requires a new run binding before mutation. A fork has a
different session ID and cannot inherit live roles, credentials, leases or permits. Corrupt
entries fail closed; a valid bounded checkpoint can accelerate replay. `/tree` is rejected
while non-coordinator runs are active.

Credentials, socket paths, capability tokens and private prompts are never persisted. Role
projections strip broker idempotency results and filesystem paths. Private artifacts, private
roles, hidden channels and evaluator verification evidence are filtered from unauthorized
views.

## Roles, tasks and signals

A stable `roleId` is bound to one ephemeral `runId` and a monotonically increasing
generation. Rebind fences the old generation and moves eligible task responsibility and
role-based channel subscriptions. Old generations cannot mutate, acknowledge, receipt,
checkpoint, query private state or consume permits.

Tasks use `draft → ready → in_progress → blocked → submitted → review → completed`, with
terminal `failed` and `cancelled` states. Entering ready requires resolved dependencies and
blockers, current-generation acknowledgements and exact latest frozen inputs. In strict mode,
starting additionally requires a live lease. Readiness neither spends nor requires a budget
reservation. Typed holds targeting the task, its owner, or a channel where the owner
participates apply without manually copying blocker IDs. Explicit task targets define gate
scope; additional role and channel targets route notifications without blocking unrelated
tasks.

Completion requires current frozen output references, verification evidence, satisfied review
and explicit current next actions. A reviewer uses `subagent_task` with `approve` or
`changes_requested`, the expected revision and a reason. In fast mode `approve` requires
explicit `nextActions`, verifies current outputs and gates, and completes atomically; strict
mode keeps a separate completion transition. Workers may clear approval with
`reviewSatisfied: false`, never grant it. Changing outputs or verification invalidates prior
approval.

Typed signals — not prose — represent GO/HOLD, blockers, decisions, corrections, reviews and
approvals. `subagent_signal supersede` requires a summary, creates a successor and atomically
redirects explicit task dependencies and blockers, resetting acknowledgements. Supplied
targets, acknowledgement roles, kind, severity and artifact references replace the
predecessor's values; omitted fields retain them.

`subagent_task handoff` requires the current revision, a different active owner, a reason and
explicit next actions. It transfers responsibility and returns the task to draft, so the new
owner must revalidate prerequisites before starting. The former owner must stop, and
outstanding permits must first be cancelled or reconciled. Handoff is distinct from
submitting outputs for review.

When a fast-mode worker exits, unapproved submitted work becomes explicitly awaiting review
and unfinished work becomes blocked for resume or handoff. Only already-approved work with
current verified bytes and settled obligations closes automatically. The final delivery
includes current task IDs and revisions plus pending review actions, and the dashboard
identifies offline owners. **Exit is not approval.** Watchers carry the run identity, so an
older watcher cannot detach a replacement run.

## Artifacts, budgets and permits

Publication defaults to
`${PI_CODING_AGENT_DIR:-~/.pi/agent}/artifacts/coordination/<session-hash>`; an explicit
`storeDir` overrides it. Tool paths are resolved before child transport, so a worker's
relative path cannot accidentally resolve in the coordinator's directory.

Sealing rejects symlinks, reads through an `O_NOFOLLOW` descriptor, verifies a stable source,
writes into a private temporary file, and atomically publishes a read-only content-addressed
copy using a no-clobber hard link. An existing destination is verified rather than
overwritten; each artifact is limited to 64 MiB. Freeze verifies bytes again. Corrections
create successor versions. Supersession and explicit `invalidate` hold pending consumers;
active consumers follow `cancel`, `finish_as_invalid`, or audited coordinator override policy.
A coordinator can use `subagent_task bind_inputs` with an expected revision and reason to bind
verified current frozen successors once no dispatched or unknown attempt is active.

Task creation accepts an explicit `taskId` and pre-reserved `leases`. Alternatively,
`subagent_permit reserve` with `taskId` and `expectedRevision` reserves and attaches a lease
atomically, returning the updated task revision — use this to attach more execution budget to
an in-progress task rather than cycling task state. Reserving without a task returns an
unattached lease.

Prefer `subagent_permit prepare`; the lower-level API remains available. Protected attempts
reserve budget, issue a single-use permit bound to task revision, role generation, frozen
digests, signal revisions, acknowledgements, operation and expiry, then arm it before invoking
the protected tool normally. The shared execution hook matches the actual operation and marks
the permit dispatched before the side effect, so built-in tool schemas need no extra permit
argument. Bytes and gates are verified again at dispatch. Dispatched and outcome-unknown
reservations stay charged across disconnect and rebind, and never migrate automatically.
Coordinator-only `subagent_permit reconcile` requires a reason and can settle an
old-generation outcome without refunding a dispatched charge.

Enforcement covers supported Pi paths routed through the broker. Arbitrary external processes
running outside Pi cannot be universally intercepted.

## Delivery semantics

Delivery states are deliberately distinct:

| State | Meaning |
|---|---|
| `queued` | the broker accepted the event |
| `delivered` | the target Pi session confirmed injection using the delivery ID |
| `acknowledged` | the agent explicitly acknowledged it |
| `resolved` | the underlying obligation closed |

None of them means "read."

Critical and blocking signal revisions wake an autonomous target at most once unless
explicitly retried. Routine traffic rides bounded inboxes on the next natural user turn or
useful tool result, without waking the model or requiring a query. Inbox receipts happen only
after observed injection, including error results; large or foreign-shaped tool results defer
delivery. Resolved and superseded notifications are omitted, and signal sends do not echo to
their author unless acknowledgement is explicitly required. Urgent delivery remains immediate.
Interactive agents use next-turn delivery by default. Backpressure and partial fan-out stay
explicit. Signal text includes its ID, revision, predecessor and acknowledgement obligation.
Digests have a 20 KiB text budget and appear before query data in tool results, so truncation
cannot hide receipted digest lines.

`subagent_team` returns an overview by default without replaying audit history. Supply
`sinceCursor` explicitly for an audit digest. Select `kind` (for example `tasks`, `signals`,
`artifacts` or `budgets`), optionally `id`, and page with `offset`/`limit`. Responses provide
`seq`, `total` and `nextOffset`; supply `expectedSeq` to detect changes between pages.
Oversized records return explicit omission hints. Use `field` (such as `details`,
`requiredInputs` or `reservations`) for bounded field pages; large text fields come back as
ordered fragments.

Caller bootstraps are generated server-side and capped at 12 KB. They enter ordinary context
once per binding, reload or compaction instead of rewriting the leading system prompt or
duplicating the task prompt. Mutation content contains compact actionable receipts; full
redacted events remain in tool details and the ledger.

## Transport

`src/transport.ts` is the authenticated local socket layer between one coordinator session
and its spawned runs. It is deliberately thin: it authenticates members, delivers coordination
events and tunnels coordination requests to the coordinator's broker. Workflow state lives in
the broker, never in the transport.

Members authenticate with a single-use token bound to their exact session file, so a forked
session inheriting the environment cannot adopt another run's credentials. Frames are bounded
to 128 KiB with explicit backpressure. The transport accepts exactly four actions: `hello`,
`message`, `delivery_receipt` and `coordination`.

## Verification

The deterministic worked examples run real Pi runtimes and actual tools and hooks, with
scripted tool calls instead of a model or external service. They retain temporary session
files and JSON step reports, and the root example performs an actual `ctx.reload()` before
verifying restored state. These are not live-model spawn or resume certification.

```bash
npm run check                    # lint + typecheck + unit tests
npm run test:integration         # provider-free real-Pi suites
npm run test:coordination-example  # the worked examples only
npm run bench                    # replay maxima
```

`test/integration` also holds `subagent-lifecycle.test.ts` and `mux-surface.test.ts`, which
need a real terminal multiplexer and are deliberately excluded from `test:integration`.

After a crash or reload, inspect the offline projection, reconcile any dispatched or
outcome-unknown attempts without releasing their charge, then explicitly bind a fresh run
generation. Never reuse an old permit or credential.
