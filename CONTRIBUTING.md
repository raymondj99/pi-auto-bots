# Contributing

## Getting set up

```bash
npm install
npm run check   # lint + typecheck + unit tests, what CI runs
```

The integration suites drive a real `pi` binary and are separate:

```bash
npm run test:integration          # provider-free real-Pi suites
npm run test:coordination-example # the deterministic worked examples only
```

`test/integration` also contains `subagent-lifecycle.test.ts` and `mux-surface.test.ts`,
which need a real terminal multiplexer (cmux, tmux, zellij or WezTerm). They are
deliberately excluded from `test:integration`; run them by name when changing mux code.

## Quality bar

- Read a file in full before making wide-ranging changes to it. Don't rely on search
  snippets.
- No `any` unless genuinely necessary. Check `node_modules` for external API types rather
  than guessing.
- Top-level imports only — no `await import()` or dynamic type imports.
- Match the surrounding style; it is enforced by biome (`biome.json`). `npm run lint:fix`
  applies most of it.
- Prefer deleting code to adding a flag. Ask before removing functionality that looks
  intentional.
- Every claim in a PR must be checkable from the diff or from a command you actually ran.
  Paste real test output, not "tests pass".

## Where things live

| Path | Contents |
|---|---|
| `src/index.ts` | Extension entry: spawning, watching, widget, tools and commands |
| `src/coordination/` | The event-sourced control plane — broker, projection, policy, tools |
| `src/transport.ts` | Authenticated local socket transport between coordinator and runs |
| `src/child-coordination.ts` | Child-side wiring: connect, bootstrap, register tools, receipt |
| `src/chat/` | Chat archive, goal tagging and the local dashboard server |
| `src/dashboard/` | Static dashboard assets served by `src/chat/server.ts` |
| `src/mux.ts` | Terminal multiplexer backends and surface lifecycle |
| `skills/coordination/` | The coordinator-facing skill |
| `docs/` | Long-form references; see the table below |

## Documentation

Read the file covering a surface before changing its behaviour, and update it in the same
change.

| File | Covers |
|---|---|
| `README.md` | User-facing reference: install, tools, commands, settings, agent frontmatter |
| `docs/coordination.md` | The control plane: config, workflows, roles, tasks, artifacts, permits, delivery |
| `docs/dashboard.md` | `/auto-bots`, the conversation model, goals and archives, the HTTP contract |
| `CHANGELOG.md` | Keep a Changelog format; new entries under `## [Unreleased]` |

Renaming a tool, a persisted entry type, an RPC route or a config key is a documentation
change too.

## Git

Commit and push are the maintainer's call. Keep one logical change per pull request, and
state explicitly what the change does *not* cover.
