# pi-auto-bots

A [pi](https://github.com/badlogic/pi-mono) extension for coordinated sub-agents.

Spawn agents into real terminal panes, watch them work in a live widget, and let them
coordinate through a typed, event-sourced control plane instead of prose. A local browser
dashboard shows who said what to whom.

```
┌─ Subagents ──────────────────────── 2 running ─┐
│ 01:24  Parser (worker)      active · bash 3s   │
│ 00:41  Reviewer (reviewer)  waiting 12s        │
└────────────────────────────────────────────────┘
```

## Why

The usual way agents coordinate is by writing messages to each other. Messages carry
intentions — `BLOCKER`, `HANDOFF`, `GO`, `DECISION` — that nothing enforces. A worker can
claim it finished, approve its own work, or start on inputs that changed underneath it, and
the transport will happily deliver all of it.

Here, those intentions are typed state. A task has an owner, a revision and a state machine.
Inputs are content-addressed and frozen; starting work revalidates the bytes. Approval is a
privileged transition a worker cannot grant itself. Protected operations need a single-use
permit bound to the exact task revision and role generation. Everything appends to a ledger
the session can replay after a crash.

## Install

```bash
pi install git:github.com/raymondj99/pi-auto-bots
```

You also need one supported multiplexer:

```bash
cmux pi
tmux new -A -s pi 'pi'
zellij --session pi     # then run: pi
                        # or just run pi inside WezTerm
```

Override the backend with `PI_SUBAGENT_MUX=cmux|tmux|zellij|wezterm`.

## Quick start

```
> Implement the parser and have someone independently review it.
```

The coordinator reads `skills/coordination/SKILL.md`, decides whether delegating is actually
cheaper than doing the work, and if so admits roles, declares a review channel and launches
each agent once with a short brief. Producers publish frozen outputs and submit; the reviewer
wakes automatically on valid submissions, requests corrections, and submits a report. No
polling, no relay through the coordinator.

Watch it happen:

```
/auto-bots
```

## Tools

### `subagent` — spawn an agent

| Parameter | Type | Description |
|---|---|---|
| `name` | string | Display name |
| `task` | string | Short brief: outcome, owned files, task-specific constraints |
| `role` | string | Stable coordination role to bind or rebind |
| `taskId` | string | Coordination assignment ID, e.g. a predeclared review dependency |
| `requiredInputs` | array | Exact frozen inputs (`artifactId`, `version`, `digest`) bound before launch |
| `agent` | string | Agent definition to load defaults from |
| `model` | string | Model override |
| `tools` | string | Comma-separated native pi tools |
| `skills` | string | Comma-separated skills to auto-load |
| `cwd` | string | Working directory; the agent picks up its local `.pi/` config |
| `fork` | boolean | Inherit the caller's conversation |
| `interactive` | boolean | User drives this agent in its own pane; don't wake the parent on stalls |
| `autoExit` | boolean | Exit after the final settled response |
| `systemPrompt` | string | Persona override. Put work instructions in `task` |
| `resumeSessionId` | string | Resume a previous Claude Code session |

### Other tools

| Tool | Purpose |
|---|---|
| `subagents_list` | List discoverable agent definitions |
| `subagent_interrupt` | Send Escape to a running agent's pane |
| `subagent_resume` | Resume a finished session, rebinding its role |
| `subagent_team` | Query scoped coordination state and paginated changes |
| `subagent_task` | Create and transition typed tasks; handoff, approve, request changes |
| `subagent_channel` | Create, send to, update or close a channel; declare peer review |
| `subagent_role` | Coordinator-only role admission and generation-fenced rebind |
| `subagent_signal` | Emit, acknowledge, resolve or supersede typed signals |
| `subagent_artifact` | Publish, freeze, supersede and invalidate content-addressed artifacts |
| `subagent_permit` | Reserve, issue, arm and settle permits for protected operations |
| `subagent_checkpoint` | Persist a bounded recovery checkpoint |
| `caller_ping` | Child-to-parent help request without ending the run |
| `subagent_done` | Child signals completion when not auto-exiting |

## Commands

| Command | Purpose |
|---|---|
| `/auto-bots` | Open the local browser dashboard. `url`, `clear`, `stop` |
| `/subagent <name>` | Spawn an agent by name |
| `/iterate` | Fork the current session into a subagent |
| `/plan` | Start the planning workflow |

## Configuration

Copy `config.json.example` to `config.json`. Every key is optional.

| Key | Default | Meaning |
|---|---|---|
| `coordination.workflowMode` | `"fast"` | `fast` or `strict` (lease-gated starts, shell protection) |
| `coordination.maxEvents` | `1000` | Ledger bound |
| `coordination.maxDigestLines` | `20` | Digest lines per delivery |
| `coordination.maxPayloadBytes` | `131072` | Mutation payload bound |
| `coordination.acknowledgementDeadlineMs` | `300000` | Acknowledgement deadline |
| `coordination.permitTtlMs` | `300000` | Permit lifetime |
| `coordination.protectedOperations` | `["costly_call","deploy"]` | Operations requiring a permit |
| `coordination.defaultDelivery` | `"digest"` | `urgent`, `digest` or `next_turn` |
| `coordination.roleCapabilities` | `{}` | Capabilities granted per role |
| `status.enabled` | `true` | Live status supervision in the widget |

The parser rejects unknown keys and out-of-range values.

## Custom agents

Put a `.md` file in `.pi/agents/` (project) or `~/.pi/agent/agents/` (global). Project
definitions shadow global ones.

```markdown
---
name: reviewer
description: Independently reviews frozen submissions
model: anthropic/claude-sonnet-4-6
tools: read, bash, grep, find
auto-exit: true
spawning: false
---

You review work you did not write...
```

| Field | Type | Description |
|---|---|---|
| `name` | string | Used as `agent: "name"` |
| `description` | string | Shown in `subagents_list` |
| `model` | string | Default model |
| `thinking` | string | `minimal`, `medium` or `high` |
| `tools` | string | Native pi tools: `read`, `bash`, `edit`, `write`, `grep`, `find`, `ls` |
| `skills` | string | Skills to auto-load |
| `session-mode` | string | `standalone`, `lineage-only` or `fork` |
| `spawning` | boolean | `false` denies every spawning tool |
| `deny-tools` | string | Extension tool names to deny |
| `auto-exit` | boolean | Shut down after the turn settles; also sets the `interactive` default |
| `interactive` | boolean | Whether stall and recovery transitions wake the parent |
| `cwd` | string | Default working directory |
| `cli` | string | `claude` to back this agent with the Claude Code CLI |
| `disable-model-invocation` | boolean | Hide from discovery; still invokable by exact name |

**`session-mode`** — `standalone` starts a fresh unlinked session; `lineage-only` starts a
fresh session that records parent lineage without copying turns; `fork` seeds the child with
the caller's conversation. `fork: true` on a spawn forces fork mode for that call.

**`auto-exit`** — the session shuts down once the turn settles, so no `subagent_done` call is
needed. Any user input before the agent finishes permanently disables auto-exit and hands the
session to the user. Bare assignments default to autonomous auto-exit; bare `/iterate` and
`fork: true` sessions stay interactive.

**`interactive`** — defaults to the inverse of `auto-exit`. Interactive agents run while the
user reads and types in their pane, so stall pings are noise; autonomous agents get them.

## Architecture

| File | Contents |
|---|---|
| `src/index.ts` | Extension entry: spawning, watching, widget, tools and commands |
| `src/transport.ts` | Authenticated local socket transport between coordinator and runs |
| `src/child-coordination.ts` | Child side: connect, bootstrap, register tools, confirm receipts |
| `src/events.ts` | The coordination event vocabulary shared by transport and chat |
| `src/mux.ts` | cmux, tmux, zellij and WezTerm backends; surface lifecycle |
| `src/session.ts` | Reading and seeding child session files |
| `src/status.ts` | Status classification and transition supervision |
| `src/activity.ts` | Reading child activity snapshots |
| `src/subagent-done.ts` | Child control tools and auto-exit |
| `src/spawn-prompt.ts` | Spawn argument normalization |
| `src/plugin/` | Claude Code plugin supplying the Stop hook for `cli: claude` agents |
| `src/coordination/broker.ts` | The control plane: roles, tasks, channels, signals, artifacts, permits |
| `src/coordination/projection.ts` | Event reduction into current state |
| `src/coordination/policy.ts` | Transitions, capabilities, redaction |
| `src/coordination/event-log.ts` | Bounded persisted ledger and replay |
| `src/coordination/tools.ts` | The coordination tool surface |
| `src/coordination/delivery.ts` | Queued, delivered, acknowledged, resolved |
| `src/coordination/artifacts.ts` | Sealing, freezing, supersession |
| `src/coordination/permits.ts` | Budgets, reservations, single-use permits |
| `src/coordination/execution.ts` | The protected-operation hook |
| `src/coordination/review.ts` | Peer review readiness and run disposition |
| `src/chat/records.ts` | Chat record building and threading |
| `src/chat/extension.ts` | Chat persistence, goal tagging, archives, `/auto-bots` |
| `src/chat/server.ts` | The local dashboard HTTP and SSE server |
| `src/dashboard/` | Static dashboard assets |

Long-form references: [docs/coordination.md](docs/coordination.md) for the control plane,
[docs/dashboard.md](docs/dashboard.md) for the dashboard.

## Limitations

- Permits meter configured **tool operations** routed through the broker. This is not a
  shell-command classifier and not an OS sandbox; processes started outside Pi are not
  intercepted.
- Subagents run with the caller's privileges. File ownership is coordination, not isolation.
- The dashboard is read-only and local. It adds no message, shell, approval or agent-control
  route.
- There is no live-model spawn or resume certification. The worked examples drive real Pi
  runtimes with scripted tool calls, not a model.

## Development

```bash
npm install
npm run check              # lint + typecheck + unit tests
npm run test:integration   # provider-free real-Pi suites
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Acknowledgements

Status supervision and turn-only interruption were inspired by
[RepoPrompt](https://repoprompt.com/)'s sub-agent snapshot polling and run cancellation.

Peer coordination was inspired by the identity-tagged inbound queues and asynchronous
steering in
[grok-bot-0.18-reconstructed](https://github.com/b-nnett/grok-bot-0.18-reconstructed). This
implementation uses Pi's supported extension messaging APIs and an independent, session-local
ownership ledger.

Originally derived from [pi-interactive-subagents](https://github.com/HazAT/pi-interactive-subagents)
by HazAT.

## License

MIT
