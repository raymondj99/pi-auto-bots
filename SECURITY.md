# Security

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub's
[security advisory](../../security/advisories/new) form rather than a public issue.
Include a description, affected version, and reproduction steps. Expect an initial
response within a week.

## Scope

This extension spawns agent processes and brokers coordination between them on the local
machine. The following are in scope:

- Escaping the capability model: acting as a role you are not bound to, mutating state from
  a fenced generation, or reaching another session's roles, credentials or leases.
- Transport authentication: reusing revoked credentials, adopting another run's identity, or
  a forked session inheriting credentials it should not hold.
- Artifact handling: escaping the content-addressed store, overwriting frozen bytes, or
  symlink and TOCTOU attacks against sealing.
- Dashboard access control: reaching `/api/*` without the capability token, or cross-origin
  requests driving the archive endpoint.
- Leaking credentials, capability tokens, socket paths or private prompts into persisted
  session entries, chat records or the dashboard.

## Not in scope

These are documented limitations, not vulnerabilities:

- **Protected operations are not a sandbox.** Permits meter configured tool operations
  routed through the broker. Arbitrary processes started outside Pi are not intercepted, and
  this is not a shell-command classifier or an OS-level sandbox.
- **Subagents run with the caller's privileges.** Spawned agents inherit the user's
  filesystem and network access. Ownership rules are coordination, not isolation.
- **The dashboard is local and read-only.** It binds `127.0.0.1` with an ephemeral port and
  a capability token. Anyone who can already read the user's process list or terminal can
  read that token.
- **Agent instructions are not a trust boundary.** Task briefs, channel messages and
  artifacts authored by agents are data, not privileged instructions, and a model may ignore
  them.
