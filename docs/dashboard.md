# The auto-bots dashboard

`/auto-bots` opens a local, read-only browser dashboard showing every named agent in the
session and the coordination traffic between them.

Audience: anyone changing the dashboard, its HTTP contract, or how coordination events become
conversations. The control plane itself is documented in [coordination.md](coordination.md).

It is an **observer**. Opening, navigating, searching, filtering and closing it never sends an
agent message, starts a model turn, acknowledges delivery, changes task ownership or
interrupts an agent. There is no message, shell, approval or agent-control route.

## Commands

| Command | Effect |
|---|---|
| `/auto-bots` | Start the dashboard if needed, print the URL, and open a browser |
| `/auto-bots <query>` | Same, seeding the conversation search box with `<query>` |
| `/auto-bots url` | Print the URL without opening a browser |
| `/auto-bots clear` | Archive current history. Non-destructive: session records are kept |
| `/auto-bots stop` | Shut the local server down. Agents are unaffected |

Newly installed UI changes require `/reload` in an already-running Pi session. Defer that
until active workers have finished, so their caller transport is not interrupted.

## Conversation model

- **Agents** list opens `agent:<exact-id>` timelines directly. Direct messages, assignments,
  handoffs and completions involving that agent appear there with direction labels. There are
  no synthetic DM threads and no aggregate "all activity" view.
- **Conversations** contains `channel:<exact-channel-id>` entries only — explicit channels
  created by the coordinator, with names, member list and an activity preview. Channels are
  real shared communication spaces, not display groupings.
- Agent timelines place the selected agent's outgoing traffic on the right and incoming on the
  left; channels place coordinator traffic on the right. Full direction labels stay available
  to screen readers.
- Technical IDs, goals, and queue-versus-read details are opt-in through **Chat options →
  Show details**. Operational coordination state sits in a nested disclosure there, not in a
  persistent banner.

Coordination ledger events are projected into the same record shape as transport messages, so
channel messages, signals, task transitions and review decisions all render as conversations.
Task events owned solely by the coordinator are treated as operational state, not conversation,
and are not shown as messages.

The roster combines live process names and statuses with persisted roles, so a resumed run
keeps its conversation identity. The dashboard shows coordination messages, never private
model reasoning.

## Goals and archives

- The `/goal` lifecycle is authoritative. Goal identity is the session-scoped `startedAt`, not
  the goal text, and *paused* is not *completed*. On completion or cancellation, only the
  records, members and channels associated with that goal are archived. There is no model
  inference of "goal complete."
- The default **Active** view hides finished and archived agents, archived entities, events and
  goals, and closed channels. **Show archived** reveals history with badges. The coordinator
  stays accessible.
- **Clear chats** asks for confirmation, then archives current records, channels and finished
  members. This is non-destructive — the session JSONL is untouched and Show archived reveals
  the history. The header **Archive** action archives the selected agent or channel.
- New unrelated traffic can unarchive an agent, but traffic bound to a completed goal stays
  archived.
- Archive metadata persists as custom session entries isolated by session ID. A same-session
  reload restores it; forks and new sessions do not inherit another team's archive metadata.

On reload, existing ledger history is backfilled into the chat store with stable event IDs to
prevent duplicates. Archived or offline agents may need the archived-history filter enabled to
appear.

## HTTP contract

The server binds to `127.0.0.1` on an ephemeral port and mints a capability token carried in
the URL fragment. The fragment is never sent to the server by the browser; the page reads it
and presents it as a bearer token.

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | The dashboard page |
| `/app.js`, `/style.css` | GET | Static assets |
| `/api/snapshot` | GET | Current members, records, goals and archive state |
| `/api/events` | GET | Server-sent events stream for live updates |
| `/api/archive` | POST | Archive a scope (`all`, an agent, a channel or a goal) |

Every route requires the bearer capability. `/api/archive` additionally requires a matching
`Origin`, so a page on another origin cannot drive it. The capability is invalidated when the
server stops or the session changes, and a replacement session never serves a previous
session's history.

`buildOperationalView()` supplies the bounded, redacted task, blocker, acknowledgement,
artifact-version, budget, delivery and audit projections the dashboard renders.

## Tests

| File | Covers |
|---|---|
| `test/coordination-chat.test.ts` | Record building, threading and dedupe |
| `test/coordination-chat-projection.test.ts` | Ledger events to conversation records |
| `test/coordination-chat-extension.test.ts` | Persistence, goal tagging, archive lifecycle |
| `test/coordination-chat-server.test.ts` | Capability, origin and shutdown guards |
| `test/coordination-chat-web.test.ts` | Static asset and rendering invariants |
| `test/integration/coordination-chat-browser.test.ts` | Headless Chrome, live SSE, search, mobile |

Set `PI_TEST_CHROME` if Chrome is not at the standard platform path.
