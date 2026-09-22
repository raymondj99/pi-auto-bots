# Agent instructions

Read `CONTRIBUTING.md` first; it holds the quality bar, the layout table and the
documentation map. This file only adds what an agent working in this repo needs on top.

## Before changing behaviour

- Run `npm run check` after any code change and fix everything it reports.
- If you create or modify a test, run it and iterate until it passes. Mutation-check new
  assertions: break the line under test, confirm red, restore.
- Never weaken an assertion or delete a test to make a suite green. If a test encodes
  behaviour that genuinely no longer exists, say so explicitly and explain the coverage
  delta.

## Traps in this codebase

- **Two brokers.** `TransportBroker` (`src/transport.ts`) is the socket layer.
  `CoordinationBroker` (`src/coordination/broker.ts`) is the event-sourced control plane.
  They are different objects with similar names; check which one a call site means.
- **`npm run` shadows `pi`.** It prepends `node_modules/.bin` to PATH, which hides the
  installed pi behind the devDependency's older CLI. `test/integration/pi-cli.ts` resolves
  around this — use `PI_CLI`, never a bare `"pi"`.
- **`test:integration` is an explicit file list, not a glob.** `test/integration` also holds
  suites needing a real multiplexer. A glob would sweep them in; the guard in
  `test/coordination-autonomy.test.ts` enforces this.
- **The package's own `config.json` is read by tests.** It is gitignored, so a local edit can
  change unit-test behaviour. Keep it minimal.
- **Coordination is always on** for a session that is not itself a spawned child. There is no
  protocol-version switch.

## Git

Never commit, push, tag or create branches unless asked. Never run history- or
worktree-destroying commands. Leave files you did not change exactly as you found them.
