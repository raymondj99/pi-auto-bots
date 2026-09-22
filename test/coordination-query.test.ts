import assert from "node:assert/strict";
import { it } from "node:test";
import { buildDigest, formatSignalDelivery } from "../src/coordination/delivery.ts";
import {
	createProjection,
	makeEnvelope,
	reduceCoordinationEvent,
} from "../src/coordination/projection.ts";
import { buildQueryPage } from "../src/coordination/query.ts";
import { registerCoordinationWorkflowTools } from "../src/coordination/tools.ts";

it("large signal queries are bounded and their detail pages reconstruct all text", () => {
	const projection = createProjection("session", "branch");
	const details = '漢字\\"'.repeat(1600);
	for (let index = 0; index < 100; index++)
		projection.signals[`signal-${index}`] = {
			id: `signal-${index}`,
			kind: "attention",
			severity: "info",
			summary: "Context",
			details,
			state: "open",
			revision: 1,
			targets: [{ roleId: "worker" }],
			artifactRefs: [],
			requiresAck: [],
			acknowledgements: [],
		};
	const page = buildQueryPage(projection, { kind: "signals", limit: 100 }, 2048);
	assert.ok(Buffer.byteLength(JSON.stringify(page)) <= 2048);
	assert.ok(page.nextOffset);
	assert.ok((page.items[0] as any).omittedFields.includes("details"));
	const fragments: string[] = [];
	let offset = 0;
	for (;;) {
		const next = buildQueryPage(
			projection,
			{ kind: "signals", id: "signal-0", field: "details", offset, limit: 100 },
			2048,
		);
		assert.ok(Buffer.byteLength(JSON.stringify(next)) <= 2048);
		fragments.push(...next.items.map((item: any) => item.text));
		if (next.nextOffset === undefined) break;
		assert.ok(next.nextOffset > offset);
		offset = next.nextOffset;
	}
	assert.equal(fragments.join(""), details);
	assert.throws(() => buildQueryPage(projection, { expectedSeq: 1 }), { code: "STALE_REVISION" });
	for (const invalid of [-1, NaN, Infinity, 0.5])
		assert.throws(() => buildQueryPage(projection, { offset: invalid }), {
			code: "BOUNDS_EXCEEDED",
		});
});

it("UTF-8 digest pages preserve omitted events and remain injectable before large query results", async () => {
	let projection = createProjection("session", "branch");
	const actor = { roleId: "coordinator", runId: "run", generation: 1 };
	for (let index = 0; index < 100; index++)
		projection = reduceCoordinationEvent(
			projection,
			makeEnvelope(
				projection,
				"signal.emitted",
				actor,
				{ id: `signal-${index}`, summary: "漢".repeat(500), targets: [{ roleId: "worker" }] },
				`event-${index}`,
			),
		);
	const digest = buildDigest(projection, "worker", 0, 100);
	assert.ok(digest.truncated > 0);
	assert.ok(Buffer.byteLength(digest.lines.join("")) <= 20 * 1024);
	const remainder = buildDigest(projection, "worker", digest.cursor, 100);
	assert.ok(remainder.cursor > digest.cursor);
	const definitions = new Map<string, any>();
	registerCoordinationWorkflowTools(
		{
			registerTool(definition: any) {
				definitions.set(definition.name, definition);
			},
		} as any,
		() => ({
			actor,
			broker: { digest: () => digest, query: () => ({ items: ["x".repeat(100000)] }) } as any,
		}),
	);
	const result = await definitions.get("subagent_team").execute("query", { sinceCursor: 0 });
	assert.match(result.content[0].text, /truncated/);
	assert.ok(
		result.content[0].text.includes(JSON.stringify(digest.lines.at(-1))),
		"every receipted digest line must precede truncation",
	);
});

it("urgent signal text identifies the revision, correction and acknowledgement obligation", () => {
	const text = formatSignalDelivery(
		{
			id: "new",
			supersedes: "old",
			revision: 2,
			kind: "hold",
			severity: "critical",
			state: "open",
			summary: "Corrected contract",
			details: "x".repeat(8000),
			targets: [{ roleId: "worker" }],
			requiresAck: ["worker"],
			artifactRefs: [],
			acknowledgements: [],
		},
		"worker",
	);
	assert.match(text, /new@2/);
	assert.match(text, /Supersedes signal old/);
	assert.match(text, /expectedRevision=2/);
	assert.match(text, /Details omitted/);
	assert.ok(text.length <= 8000);
});
