import { CoordinationPolicyError } from "./policy.ts";

/** The same identity grammar at admission, routing and process launch. */
export function normalizeCoordinationRoleId(value: string): string {
	return value
		.trim()
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, "-")
		.replace(/^-|-$/g, "")
		.slice(0, 100);
}

export function normalizeRoleReferences(params: Record<string, unknown>): Record<string, unknown> {
	const role = (value: unknown) => {
		if (typeof value !== "string") return value;
		const normalized = normalizeCoordinationRoleId(value);
		if (!normalized || ["__proto__", "constructor", "prototype"].includes(normalized))
			throw new CoordinationPolicyError("BOUNDS_EXCEEDED", "Invalid role identity.");
		return normalized;
	};
	const result = { ...params };
	// Never rewrite an authenticated actor or a run/task/artifact identity.
	for (const key of ["roleId", "owner", "reviewerRoleId"])
		if (result[key] !== undefined) result[key] = role(result[key]);
	for (const key of ["requiredAcknowledgements", "requiresAck", "privateTo"])
		if (Array.isArray(result[key])) result[key] = result[key].map(role);
	for (const key of ["members", "targets"])
		if (Array.isArray(result[key]))
			result[key] = result[key].map((item) =>
				item && typeof item === "object" && "roleId" in item
					? { ...item, roleId: role(item.roleId) }
					: item,
			);
	if (result.review && typeof result.review === "object")
		result.review = {
			...result.review,
			reviewerRoleId: role((result.review as any).reviewerRoleId),
		};
	return result;
}
