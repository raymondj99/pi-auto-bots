import { existsSync, statSync } from "node:fs";
import { delimiter, join } from "node:path";

/**
 * Resolve the `pi` CLI the integration tests should drive.
 *
 * `npm run` prepends `node_modules/.bin` to PATH, which shadows the installed
 * pi with the devDependency's bundled CLI. That copy pins the compile-time type
 * baseline, not the runtime the extension actually targets, and it rejects
 * flags these tests rely on (`--no-context-files`). Resolving explicitly — and
 * skipping any `node_modules` bin directory — makes `npm run test:integration`
 * behave the same as running `node --test` directly.
 *
 * Set `PI_TEST_CLI` to pin a specific binary.
 */
function resolvePiCli(): string {
	if (process.env.PI_TEST_CLI) return process.env.PI_TEST_CLI;
	for (const entry of (process.env.PATH ?? "").split(delimiter)) {
		if (!entry || entry.includes("node_modules")) continue;
		const candidate = join(entry, "pi");
		try {
			if (existsSync(candidate) && statSync(candidate).isFile()) return candidate;
		} catch {
			/* Unreadable PATH entries are simply skipped. */
		}
	}
	return "pi";
}

export const PI_CLI = resolvePiCli();
