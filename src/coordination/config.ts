import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export interface CoordinationConfig {
	workflowMode: "fast" | "strict";
	maxEvents: number;
	maxDigestLines: number;
	maxPayloadBytes: number;
	acknowledgementDeadlineMs: number;
	permitTtlMs: number;
	protectedOperations: string[];
	defaultDelivery: "urgent" | "digest" | "next_turn";
	roleCapabilities: Record<string, string[]>;
}

export const DEFAULT_COORDINATION_CONFIG: CoordinationConfig = Object.freeze({
	workflowMode: "fast",
	maxEvents: 1000,
	maxDigestLines: 20,
	maxPayloadBytes: 131072,
	acknowledgementDeadlineMs: 300000,
	permitTtlMs: 300000,
	protectedOperations: ["costly_call", "deploy"],
	defaultDelivery: "digest",
	roleCapabilities: {},
});

/** A fresh mutable copy of the defaults; the exported default is frozen. */
export function defaultCoordinationConfig(): CoordinationConfig {
	return {
		...DEFAULT_COORDINATION_CONFIG,
		protectedOperations: [...DEFAULT_COORDINATION_CONFIG.protectedOperations],
		roleCapabilities: {},
	};
}

const ALLOWED_KEYS = new Set([
	"workflowMode",
	"maxEvents",
	"maxDigestLines",
	"maxPayloadBytes",
	"acknowledgementDeadlineMs",
	"permitTtlMs",
	"protectedOperations",
	"defaultDelivery",
	"roleCapabilities",
]);

function object(value: unknown, field: string): Record<string, unknown> {
	if (!value || typeof value !== "object" || Array.isArray(value))
		throw new Error(`${field} must be an object`);
	return value as Record<string, unknown>;
}

function integer(value: unknown, field: string, min: number, max: number): number {
	if (!Number.isInteger(value) || (value as number) < min || (value as number) > max)
		throw new Error(`${field} must be an integer between ${min} and ${max}`);
	return value as number;
}

function stringArray(value: unknown, field: string): string[] {
	if (
		!Array.isArray(value) ||
		value.length > 100 ||
		value.some((item) => typeof item !== "string" || !item || item.length > 100)
	)
		throw new Error(`${field} must be a bounded string array`);
	return [...new Set(value as string[])];
}

export function parseCoordinationConfig(raw: unknown): CoordinationConfig {
	const root = object(raw, "root");
	const coordination = root.coordination == null ? {} : object(root.coordination, "coordination");
	for (const key of Object.keys(coordination))
		if (!ALLOWED_KEYS.has(key)) throw new Error(`Unknown coordination setting: ${key}`);

	const config = defaultCoordinationConfig();

	if (coordination.workflowMode !== undefined) {
		if (coordination.workflowMode !== "fast" && coordination.workflowMode !== "strict")
			throw new Error("coordination.workflowMode is invalid");
		config.workflowMode = coordination.workflowMode;
	}
	if (config.workflowMode === "strict")
		config.protectedOperations = ["costly_call", "shell", "deploy"];
	if (coordination.maxEvents !== undefined)
		config.maxEvents = integer(coordination.maxEvents, "coordination.maxEvents", 100, 10000);
	if (coordination.maxDigestLines !== undefined)
		config.maxDigestLines = integer(
			coordination.maxDigestLines,
			"coordination.maxDigestLines",
			1,
			100,
		);
	if (coordination.maxPayloadBytes !== undefined)
		config.maxPayloadBytes = integer(
			coordination.maxPayloadBytes,
			"coordination.maxPayloadBytes",
			4096,
			131072,
		);
	if (coordination.acknowledgementDeadlineMs !== undefined)
		config.acknowledgementDeadlineMs = integer(
			coordination.acknowledgementDeadlineMs,
			"coordination.acknowledgementDeadlineMs",
			1000,
			86400000,
		);
	if (coordination.permitTtlMs !== undefined)
		config.permitTtlMs = integer(
			coordination.permitTtlMs,
			"coordination.permitTtlMs",
			1000,
			86400000,
		);
	if (coordination.protectedOperations !== undefined)
		config.protectedOperations = stringArray(
			coordination.protectedOperations,
			"coordination.protectedOperations",
		);
	if (coordination.defaultDelivery !== undefined) {
		if (!["urgent", "digest", "next_turn"].includes(coordination.defaultDelivery as string))
			throw new Error("coordination.defaultDelivery is invalid");
		config.defaultDelivery = coordination.defaultDelivery as CoordinationConfig["defaultDelivery"];
	}
	if (coordination.roleCapabilities !== undefined)
		for (const [role, capabilities] of Object.entries(
			object(coordination.roleCapabilities, "coordination.roleCapabilities"),
		)) {
			if (!role || role.length > 100) throw new Error("roleCapabilities role is invalid");
			config.roleCapabilities[role] = stringArray(
				capabilities,
				`coordination.roleCapabilities.${role}`,
			);
		}
	return config;
}

export function loadCoordinationConfig(path: string): CoordinationConfig {
	const raw = JSON.parse(readFileSync(path, "utf8"));
	const mode = process.env.PI_SUBAGENT_COORDINATION_WORKFLOW_MODE;
	if (mode) raw.coordination = { ...raw.coordination, workflowMode: mode };
	return parseCoordinationConfig(raw);
}

export function loadLocalCoordinationConfig(): CoordinationConfig {
	const path = fileURLToPath(new URL("../../config.json", import.meta.url));
	return existsSync(path) ? loadCoordinationConfig(path) : parseCoordinationConfig({});
}
