# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

First release of `pi-auto-bots`, reworked from the recovered
`pi-interactive-subagents` 3.7.2 source. The coordination control plane is the product; the
older prose-coordination protocol it was built beside has been removed.

> **⚠️ Breaking: this is not a drop-in successor to `pi-interactive-subagents`.** The
> package name, the selectable protocol version, the free-text coordination tools, the
> terminal chat view and the bundled agent definitions are all gone. Sessions written by the
> old package cannot be resumed: the persisted coordination entry type changed from
> `subagent-coordination-v2-event` to `subagent-coordination-event`, and old entries are
> ignored rather than migrated.

### Removed

- **The legacy coordination protocol.** `coordination.protocolVersion` and the
  `PI_SUBAGENT_COORDINATION_PROTOCOL_VERSION` environment variable no longer exist, and the
  event-sourced control plane is always active. The prose tools that protocol registered —
  `subagent_message`, `subagent_handoff`, and the broker's own task, channel and roster
  actions — are gone; their typed equivalents live on `subagent_task`, `subagent_channel` and
  `subagent_signal`. This removed the dual-mode branching that ran through every call site.
- **The `team: "caller" | "children"` routing and its cross-extension RPC bridge**
  (`pi-subagents:children-coordination`). A session reaches its own broker directly and a
  child reaches its coordinator's over the authenticated transport; there is no third path.
- **The terminal chat view.** `/subagent-chat tui`, the `CoordinationChatView` component and
  its viewport are removed. The browser dashboard is the only chat surface.
- **Bundled agent definitions.** The `agents/` directory and the `"package"` agent source are
  gone. Agents resolve from `.pi/agents/` (project) and `~/.pi/agent/agents/` (global) only.
- **Development narrative documentation.** Eleven session-report and design-history files,
  plus `GOAL.md` and the original plan document, are replaced by two references:
  `docs/coordination.md` and `docs/dashboard.md`.

### Changed

- **`/subagent-chat` is now `/auto-bots`.** It opens the browser dashboard directly and keeps
  the `url`, `clear` and `stop` subcommands.
- **`v2` is gone from every identifier, string, filename and document.**
  `CoordinationV2Broker` is `CoordinationBroker`, `registerCoordinationV2Tools` is
  `registerCoordinationTools`, `docs/coordination-v2.md` is `docs/coordination.md`, and tool
  labels read "Coordination Task" rather than "Coordination V2 Task".
- **The socket layer is now named for what it does.** `coordination.ts` is `transport.ts`,
  exporting `TransportBroker` and `TransportClient`, and is reduced to authentication,
  message delivery, delivery receipts and the coordination tunnel. The tunnel action is
  `coordination` rather than `v2`. It gained `isAvailable(id)` for member liveness.
- **The coordination event vocabulary moved to `src/events.ts`**, shared by the transport,
  the chat archive and the ledger-to-conversation projection.
- **Repository layout.** `pi-extension/subagents/` is `src/`, with `src/coordination/`,
  `src/chat/` and `src/dashboard/` beneath it.
- **Source formatting is enforced.** Several modules were written as single logical lines of
  400–800 characters; biome now formats and lints `src/` and `test/`.

### Added

- **Linting, formatting and CI.** `biome.json`, `npm run check` (lint, typecheck, test), and
  a GitHub Actions workflow running them on every push and pull request.
- **Contributor scaffolding.** `CONTRIBUTING.md`, `SECURITY.md`, `AGENTS.md` and `.npmignore`.

### Fixed

- **`npm run test:integration` could never pass.** `npm run` prepends `node_modules/.bin` to
  `PATH`, shadowing the installed `pi` with the devDependency's older bundled CLI, which
  rejects `--no-context-files`. All nineteen integration tests failed with
  `Unknown option: --no-context-files` unless run outside npm. `test/integration/pi-cli.ts`
  now resolves the CLI explicitly, skipping any `node_modules` bin directory, and honours
  `PI_TEST_CLI`.
- **A stale unit test asserted bootstrap wording that existed nowhere in the source.**
  `coordination-v2-friction.test.ts` required the caller bootstrap to match
  `/final assistant summary is delivered automatically/`; the prompt had been rewritten and
  the assertion never updated, so the suite shipped one permanent failure. It now asserts the
  wording the bootstrap actually emits.
- **`PI_SUBAGENT_COORDINATION_WORKFLOW_MODE` was silently inert without a `config.json`.**
  The override was applied only on the file-reading path, but `config.json` is gitignored,
  so it is absent in CI and in a fresh clone. A run asking for strict mode quietly stayed in
  fast mode with shell unprotected. `loadOptionalCoordinationConfig()` now applies
  environment overrides on both paths. Pre-existing; local development masked it because a
  `config.json` was always present.
- **The shipped `config.json` declared `protocolVersion`,** which the parser rejects as an
  unknown key once the setting was removed, breaking every child bootstrap.

### Testing

283 unit tests and 8 provider-free integration suites pass; lint and strict typecheck are
clean. Coverage changed with the removals:

- The legacy broker's task, channel, roster and observer tests were replaced by
  `test/transport.test.ts` and `test/transport-observer.test.ts`, covering the retained
  surface: authentication, isolation, message routing, the coordination tunnel, receipt
  confirmation and shutdown.
- `test/integration/coordination-chat-runtime.test.ts` was **removed, not ported**. It drove
  the chat archive through the deleted RPC bridge by having the coordinator assign a task to
  itself — traffic the coordination projection deliberately classifies as operational state
  rather than conversation, so there is no equivalent scenario. Goal-scoped archiving and the
  dashboard's access guards remain covered by `coordination-chat-extension.test.ts`,
  `coordination-chat-server.test.ts` and `coordination-chat-browser.test.ts`; real-Pi chat
  persistence across reload remains covered by `coordination-worked-example.test.ts`.
- The terminal-chat integration test lost its live-injection tail, which also relied on the
  RPC bridge. Live record delivery into an open view is covered by
  `coordination-chat-browser.test.ts`.

[Unreleased]: https://github.com/raymondj99/pi-auto-bots/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/raymondj99/pi-auto-bots/releases/tag/v0.1.0
