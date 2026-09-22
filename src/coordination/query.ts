import { CoordinationPolicyError } from "./policy.ts";
import type { CoordinationProjection } from "./protocol.ts";

export const QUERY_KINDS = [
	"overview",
	"roles",
	"channels",
	"tasks",
	"signals",
	"artifacts",
	"budgets",
	"permits",
	"deliveries",
	"audit",
] as const;
export type QueryKind = (typeof QUERY_KINDS)[number];
export const QUERY_FIELDS = [
	"capabilities",
	"members",
	"dependsOn",
	"blockers",
	"requiredInputs",
	"outputs",
	"requiredAcknowledgements",
	"leases",
	"verification",
	"nextActions",
	"permits",
	"details",
	"targets",
	"artifactRefs",
	"requiresAck",
	"acknowledgements",
	"consumers",
	"reservations",
	"sealedInputs",
	"requiredSignals",
	"acknowledgedBy",
] as const;
export interface QueryOptions {
	kind?: QueryKind;
	id?: string;
	field?: (typeof QUERY_FIELDS)[number];
	offset?: number;
	limit?: number;
	expectedSeq?: number;
}
export interface QueryPage {
	kind: QueryKind;
	seq: number;
	total: number;
	offset: number;
	nextOffset?: number;
	items: unknown[];
}

function identity(value: Record<string, unknown>): unknown {
	return value.id ?? value.roleId ?? value.artifactId ?? value.eventId;
}

/** Page an already-authorized projection. Large fields require explicit field pages;
 * a summary always tells the caller what was omitted rather than silently dropping it.
 */
export function buildQueryPage(
	projection: CoordinationProjection,
	options: QueryOptions = {},
	maxBytes = 120000,
): QueryPage {
	const kind = options.kind ?? "overview",
		offset = options.offset ?? 0,
		limit = options.limit ?? 20;
	if (
		!QUERY_KINDS.includes(kind) ||
		(options.field !== undefined && !QUERY_FIELDS.includes(options.field)) ||
		!Number.isSafeInteger(offset) ||
		offset < 0 ||
		!Number.isSafeInteger(limit) ||
		limit < 1 ||
		limit > 100 ||
		(options.id !== undefined &&
			(typeof options.id !== "string" || !options.id || options.id.length > 200))
	)
		throw new CoordinationPolicyError(
			"BOUNDS_EXCEEDED",
			"Invalid query kind, field, ID or page bounds.",
		);
	if (options.expectedSeq !== undefined && options.expectedSeq !== projection.seq)
		throw new CoordinationPolicyError("STALE_REVISION", "Projection changed between query pages.", {
			revision: projection.seq,
		});
	let rows: unknown[];
	if (kind === "overview")
		rows = QUERY_KINDS.filter((name) => name !== "overview").map((name) => ({
			kind: name,
			count:
				name === "artifacts"
					? Object.values(projection.artifacts).reduce((sum, versions) => sum + versions.length, 0)
					: Object.keys(projection[name]).length,
		}));
	else if (kind === "artifacts") rows = Object.values(projection.artifacts).flat();
	else rows = Object.values(projection[kind]);
	if (options.id)
		rows = rows.filter((value) => identity(value as Record<string, unknown>) === options.id);
	if (options.field) {
		if (!options.id || kind === "overview")
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Field pages require an entity ID.");
		const field = options.field;
		rows = rows.flatMap((value) => {
			const record = value as Record<string, unknown>;
			const selected = record[field];
			const values =
				typeof selected === "string"
					? [selected]
					: Array.isArray(selected)
						? selected
						: selected && typeof selected === "object"
							? Object.entries(selected).map(([id, entry]) => ({ id, ...(entry as object) }))
							: [];
			return values.flatMap((entry, index) => {
				if (typeof entry !== "string") return [{ index, value: entry }];
				const parts: unknown[] = [];
				for (let at = 0; at < entry.length; at += 512)
					parts.push({ index, textOffset: at, text: entry.slice(at, at + 512) });
				return parts;
			});
		});
	}
	const page: QueryPage = { kind, seq: projection.seq, total: rows.length, offset, items: [] };
	const size = (items: unknown[]) =>
		Buffer.byteLength(JSON.stringify({ ...page, items, nextOffset: rows.length }));
	for (const row of rows.slice(offset, offset + limit)) {
		let candidate = row;
		if (size([candidate]) > maxBytes) {
			const record = row as Record<string, unknown>;
			candidate = {
				id: identity(record),
				summary: Object.fromEntries(
					Object.entries(record).filter(([key]) =>
						[
							"title",
							"state",
							"revision",
							"version",
							"owner",
							"generation",
							"used",
							"limit",
							"dispatchedUnknown",
						].includes(key),
					),
				),
				omittedFields: Object.keys(record),
				hint: "Query this ID with a field and offset to retrieve omitted data.",
			};
			if (size([candidate]) > maxBytes)
				throw new CoordinationPolicyError(
					"BOUNDS_EXCEEDED",
					"Record exceeds query bounds; use a narrower field page.",
				);
		}
		if (size([...page.items, candidate]) > maxBytes) break;
		page.items.push(candidate);
	}
	if (offset + page.items.length < rows.length) page.nextOffset = offset + page.items.length;
	return page;
}
