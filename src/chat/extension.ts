import { type FSWatcher, watch } from "node:fs";
import { basename, dirname } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import type { CoordinationOperationalView } from "../coordination/operational.ts";
import {
	type CoordinationChatRecord,
	CoordinationChatStore,
	validateCoordinationChatRecord,
} from "./records.ts";
import {
	type CoordinationArchiveRequest,
	type CoordinationChatArchive,
	type CoordinationChatGoal,
	type CoordinationDashboardMember,
	type CoordinationDashboardServer,
	startCoordinationDashboard,
} from "./server.ts";

export const CHAT_ENTRY_TYPE = "subagent-chat-event";
export const CHAT_ARCHIVE_ENTRY_TYPE = "subagent-chat-archive";
const GOAL_ENTRY_TYPE = "goal-state";
const MAX_METADATA_ITEMS = 2_000;

export interface CoordinationChatOptions {
	getMembers?: () => readonly CoordinationDashboardMember[];
	subscribeMembers?: (listener: () => void) => () => void;
	getOperational?: () => CoordinationOperationalView | undefined;
	subscribeOperational?: (listener: () => void) => () => void;
}

type GoalState = CoordinationChatGoal;
type SessionEntryLike = { type?: unknown; customType?: unknown; data?: unknown };

function emptyArchive(): CoordinationChatArchive {
	return { goalIds: [], eventIds: [], memberIds: [], channelIds: [] };
}

function stringList(value: unknown): string[] | undefined {
	if (!Array.isArray(value) || value.length > MAX_METADATA_ITEMS) return undefined;
	const result: string[] = [];
	for (const item of value) {
		if (typeof item !== "string" || !item || item.length > 300) return undefined;
		if (!result.includes(item)) result.push(item);
	}
	return result;
}

function validateArchive(value: unknown): CoordinationChatArchive | undefined {
	if (!value || typeof value !== "object") return undefined;
	const input = value as Record<string, unknown>;
	const goalIds = stringList(input.goalIds);
	const eventIds = stringList(input.eventIds);
	const memberIds = stringList(input.memberIds);
	const channelIds = stringList(input.channelIds);
	return goalIds && eventIds && memberIds && channelIds
		? { goalIds, eventIds, memberIds, channelIds }
		: undefined;
}

function validateGoal(value: unknown, ownerSession: string): GoalState | undefined {
	if (!value || typeof value !== "object") return undefined;
	const input = value as Record<string, unknown>;
	const title = typeof input.goal === "string" ? input.goal : input.title;
	if (
		typeof title !== "string" ||
		!title.trim() ||
		title.length > 8_000 ||
		!Number.isSafeInteger(input.startedAt) ||
		(input.startedAt as number) < 0 ||
		typeof input.status !== "string" ||
		!input.status ||
		input.status.length > 40
	)
		return undefined;
	const startedAt = input.startedAt as number;
	return { id: `${ownerSession}:${startedAt}`, title, status: input.status, startedAt };
}

function validateGoals(value: unknown, ownerSession: string): GoalState[] | undefined {
	if (!Array.isArray(value) || value.length > MAX_METADATA_ITEMS) return undefined;
	const result: GoalState[] = [];
	for (const item of value) {
		const goal = validateGoal(item, ownerSession);
		if (!goal || result.some((existing) => existing.id === goal.id)) return undefined;
		result.push(goal);
	}
	return result;
}

function entries(ctx: ExtensionContext): readonly SessionEntryLike[] {
	const manager = ctx.sessionManager as unknown as {
		getBranch?: () => readonly SessionEntryLike[];
		getEntries: () => readonly SessionEntryLike[];
	};
	return manager.getBranch?.() ?? manager.getEntries();
}

function involved(record: CoordinationChatRecord): Set<string> {
	const result = new Set<string>([record.event.from]);
	if (record.event.channel) {
		const participants =
			record.event.kind === "channel_message"
				? (record.event.recipients ?? [])
				: record.event.channel.members;
		for (const id of participants) result.add(id);
	} else result.add(record.event.to);
	return result;
}

/** Observe only the team owned by this session; history never restores live routing. */
export function installCoordinationChat(
	pi: ExtensionAPI,
	options: CoordinationChatOptions = {},
): (record: CoordinationChatRecord) => void {
	const store = new CoordinationChatStore();
	const knownMembers = new Map<string, CoordinationDashboardMember>();
	const memberGoals = new Map<string, string>();
	const channelGoals = new Map<string, string>();
	const taskGoals = new Map<string, string>();
	const goals = new Map<string, GoalState>();
	const listeners = new Set<() => void>();
	let archive = emptyArchive();
	let latestGoal: GoalState | undefined;
	let sessionId: string | undefined;
	let context: ExtensionContext | undefined;
	let persistenceWarning = false;
	let active: { close?: () => void } | undefined;
	let dashboard: Promise<CoordinationDashboardServer> | undefined;
	let unsubscribeMembers: (() => void) | undefined;
	let goalWatcher: FSWatcher | undefined;
	let goalRefreshQueued = false;

	function warnPersistence() {
		if (!persistenceWarning && context?.hasUI) {
			persistenceWarning = true;
			context.ui.notify(
				"Coordination chat history could not be saved; live observation continues.",
				"warning",
			);
		}
	}

	function boundMetadata() {
		const retained = store.snapshot().records;
		const retainedEvents = new Set(retained.map((record) => record.event.id));
		archive.eventIds = archive.eventIds.filter((id) => retainedEvents.has(id));
		for (const key of ["goalIds", "memberIds", "channelIds"] as const) {
			if (archive[key].length > MAX_METADATA_ITEMS)
				archive[key] = archive[key].slice(-MAX_METADATA_ITEMS);
		}
		while (goals.size > MAX_METADATA_ITEMS) goals.delete(goals.keys().next().value!);
	}

	function persistMetadata() {
		if (!sessionId) return;
		boundMetadata();
		try {
			pi.appendEntry(CHAT_ARCHIVE_ENTRY_TYPE, {
				sessionId,
				archive: {
					...archive,
					goalIds: [...archive.goalIds],
					eventIds: [...archive.eventIds],
					memberIds: [...archive.memberIds],
					channelIds: [...archive.channelIds],
				},
				goals: [...goals.values()].map((goal) => ({ ...goal })),
			});
		} catch {
			warnPersistence();
		}
	}

	function rememberNames(record: CoordinationChatRecord) {
		const labels = new Map<string, string>([[record.event.from, record.fromName]]);
		if (!record.event.channel) labels.set(record.event.to, record.toName);
		for (const id of involved(record)) {
			if (id === "parent" || knownMembers.has(id)) continue;
			knownMembers.set(id, {
				id,
				name: labels.get(id) ?? id,
				status: "archived",
				backend: "pi",
				...(memberGoals.get(id) ? { goalId: memberGoals.get(id) } : {}),
			});
		}
	}

	function associate(record: CoordinationChatRecord) {
		if (!record.goalId) return;
		const { event } = record;
		if (event.channel && !channelGoals.has(event.channel.id))
			channelGoals.set(event.channel.id, record.goalId);
		if (event.task && !taskGoals.has(event.task.id)) taskGoals.set(event.task.id, record.goalId);

		// A coordinator message/assignment is an explicit retask. Other traffic may
		// fill an unknown association, but must not drag established peers backward
		// when a late message arrives from an older goal.
		if (
			!event.channel &&
			event.from === "parent" &&
			(event.kind === "message" || event.kind === "assign")
		) {
			if (event.to !== "parent") memberGoals.set(event.to, record.goalId);
			return;
		}
		for (const id of involved(record)) {
			if (id !== "parent" && !memberGoals.has(id)) memberGoals.set(id, record.goalId);
		}
	}

	function inferredGoal(
		record: CoordinationChatRecord,
		current = latestGoal,
	): GoalState | undefined {
		const { event } = record;
		const channelGoal = event.channel ? channelGoals.get(event.channel.id) : undefined;
		if (channelGoal) return goals.get(channelGoal);
		const taskGoal = event.task ? taskGoals.get(event.task.id) : undefined;
		if (taskGoal) return goals.get(taskGoal);
		const currentIsOpen = current && (current.status === "active" || current.status === "paused");
		if (
			event.from === "parent" &&
			!event.channel &&
			(event.kind === "message" || event.kind === "assign") &&
			currentIsOpen
		)
			return current;
		const senderGoal = event.from === "parent" ? undefined : memberGoals.get(event.from);
		if (senderGoal && goals.has(senderGoal)) return goals.get(senderGoal);
		const peerGoal = [...involved(record)]
			.filter((id) => id !== "parent")
			.map((id) => memberGoals.get(id))
			.find((id): id is string => !!id && goals.has(id));
		if (peerGoal) return goals.get(peerGoal);
		return currentIsOpen ? current : undefined;
	}

	function tagged(record: CoordinationChatRecord, current = latestGoal): CoordinationChatRecord {
		if (record.goalId) return record;
		const goal = inferredGoal(record, current);
		return goal ? { ...record, goalId: goal.id, goalTitle: goal.title } : record;
	}

	function archiveCompletedGoals(persist: boolean): boolean {
		let changed = false;
		const records = store.snapshot().records;
		for (const goal of goals.values()) {
			if (goal.status !== "completed" && goal.status !== "cancelled") continue;
			const associatedMembers = [...memberGoals]
				.filter(([, id]) => id === goal.id)
				.map(([member]) => member);
			const associatedChannels = [...channelGoals]
				.filter(([, id]) => id === goal.id)
				.map(([channel]) => channel);
			const relevant =
				associatedMembers.length > 0 ||
				associatedChannels.length > 0 ||
				records.some((record) => record.goalId === goal.id);
			if (!relevant) continue;
			if (!archive.goalIds.includes(goal.id)) {
				archive.goalIds.push(goal.id);
				changed = true;
			}
			for (const member of associatedMembers) {
				if (!archive.memberIds.includes(member)) {
					archive.memberIds.push(member);
					changed = true;
				}
			}
			for (const channel of associatedChannels) {
				if (!archive.channelIds.includes(channel)) {
					archive.channelIds.push(channel);
					changed = true;
				}
			}
		}
		if (changed && persist) persistMetadata();
		return changed;
	}

	function refreshGoals(persist = true): boolean {
		if (!context || !sessionId) return false;
		const latestById = new Map<string, GoalState>();
		let nextLatest: GoalState | undefined;
		for (const entry of entries(context)) {
			if (entry.type !== "custom" || entry.customType !== GOAL_ENTRY_TYPE) continue;
			const goal = validateGoal(entry.data, sessionId);
			if (!goal) continue;
			latestById.set(goal.id, goal);
			nextLatest = goal;
		}
		let changed = false;
		for (const goal of latestById.values()) {
			const previous = goals.get(goal.id);
			if (!previous || previous.title !== goal.title || previous.status !== goal.status) {
				goals.set(goal.id, goal);
				changed = true;
			}
		}
		latestGoal = nextLatest;
		// Bind newly observed silent children while the goal is open. Existing
		// bindings survive later goals until the coordinator explicitly retasks.
		if (nextLatest && (nextLatest.status === "active" || nextLatest.status === "paused")) {
			for (const member of options.getMembers?.() ?? []) {
				if (member.id !== "parent" && !memberGoals.has(member.id))
					memberGoals.set(member.id, nextLatest.id);
			}
		}
		const archived = archiveCompletedGoals(false);
		if ((changed || archived) && persist) persistMetadata();
		if (changed || archived) notify();
		return changed || archived;
	}

	function closeGoalWatcher() {
		goalWatcher?.close();
		goalWatcher = undefined;
		goalRefreshQueued = false;
	}

	function watchGoalEntries(ctx: ExtensionContext) {
		closeGoalWatcher();
		const file = (
			ctx.sessionManager as { getSessionFile?: () => string | undefined }
		).getSessionFile?.();
		if (!file) return;
		const watchedSession = sessionId;
		try {
			goalWatcher = watch(dirname(file), { persistent: false }, (_event, filename) => {
				if (filename === null || basename(String(filename)) !== basename(file) || goalRefreshQueued)
					return;
				goalRefreshQueued = true;
				setImmediate(() => {
					goalRefreshQueued = false;
					if (sessionId === watchedSession && context === ctx) refreshGoals();
				});
			});
			goalWatcher.on("error", closeGoalWatcher);
		} catch {
			/* Goal integration remains available through lifecycle hooks. */
		}
	}

	function members(): CoordinationDashboardMember[] {
		const running = options.getMembers?.() ?? [];
		const live = new Set(running.map((member) => member.id));
		for (const [id, member] of knownMembers) {
			if (!live.has(id) && member.status !== "archived" && member.status !== "finished") {
				knownMembers.set(id, {
					...member,
					status: "finished",
					...(memberGoals.get(id) ? { goalId: memberGoals.get(id) } : {}),
				});
			}
		}
		const openGoal =
			latestGoal && (latestGoal.status === "active" || latestGoal.status === "paused")
				? latestGoal
				: undefined;
		for (const member of running) {
			if (member.id !== "parent" && openGoal && !memberGoals.has(member.id))
				memberGoals.set(member.id, openGoal.id);
			knownMembers.set(member.id, {
				id: member.id,
				name: member.name,
				status: member.status,
				role: member.role,
				backend: member.backend,
				interactive: member.interactive,
				...(memberGoals.get(member.id) ? { goalId: memberGoals.get(member.id) } : {}),
			});
		}
		return [
			{
				id: "parent",
				name: "Coordinator",
				status: context?.isIdle() ? "idle" : "active",
				backend: "coordinator",
				...(latestGoal ? { goalId: latestGoal.id } : {}),
			},
			...knownMembers.values(),
		].map((member) => ({ ...member }));
	}

	function notify() {
		members();
		for (const listener of [...listeners]) {
			try {
				listener();
			} catch {
				/* Observation must not disrupt agents. */
			}
		}
	}

	function unarchiveForNewTraffic(record: CoordinationChatRecord): boolean {
		if (record.goalId && archive.goalIds.includes(record.goalId)) return false;
		const beforeMembers = archive.memberIds.length;
		const participants = involved(record);
		archive.memberIds = archive.memberIds.filter((id) => !participants.has(id));
		const channelId = record.event.channel?.id;
		const beforeChannels = archive.channelIds.length;
		if (channelId) archive.channelIds = archive.channelIds.filter((id) => id !== channelId);
		return (
			beforeMembers !== archive.memberIds.length || beforeChannels !== archive.channelIds.length
		);
	}

	function archiveRequest(request: CoordinationArchiveRequest) {
		const snapshot = store.snapshot();
		const add = (key: keyof CoordinationChatArchive, id: string) => {
			if (!archive[key].includes(id)) archive[key].push(id);
			if (archive[key].length > MAX_METADATA_ITEMS) archive[key].shift();
		};
		if (request.scope === "all") {
			for (const record of snapshot.records) add("eventIds", record.event.id);
			for (const record of snapshot.records)
				if (record.event.channel) add("channelIds", record.event.channel.id);
			for (const member of members())
				if (
					member.id !== "parent" &&
					(member.status === "finished" || member.status === "archived")
				)
					add("memberIds", member.id);
		} else if (request.scope === "agent" && request.id) {
			add("memberIds", request.id);
			for (const record of snapshot.records)
				if (involved(record).has(request.id)) add("eventIds", record.event.id);
		} else if (request.scope === "channel" && request.id) {
			add("channelIds", request.id);
			for (const record of snapshot.records)
				if (record.event.channel?.id === request.id) add("eventIds", record.event.id);
		} else if (request.scope === "goal" && request.id && goals.has(request.id)) {
			add("goalIds", request.id);
		} else throw new Error("Unknown archive target.");
		persistMetadata();
		notify();
	}

	function closeDashboard(): Promise<void> {
		const previous = dashboard;
		dashboard = undefined;
		return previous
			? previous.then(
					(server) => server.close(),
					() => {},
				)
			: Promise.resolve();
	}

	function ensureDashboard(): Promise<CoordinationDashboardServer> {
		if (!dashboard) {
			const starting = startCoordinationDashboard({
				snapshot: () => ({
					...store.snapshot(),
					members: members(),
					sessionName: pi.getSessionName?.() || "Subagent team",
					archive: {
						...archive,
						goalIds: [...archive.goalIds],
						eventIds: [...archive.eventIds],
						memberIds: [...archive.memberIds],
						channelIds: [...archive.channelIds],
					},
					goals: [...goals.values()].map((goal) => ({ ...goal })),
					operational: options.getOperational?.(),
				}),
				subscribe: (listener) => {
					listeners.add(listener);
					const unsubscribe = store.subscribe(listener);
					const unsubscribeOperational = options.subscribeOperational?.(listener);
					return () => {
						listeners.delete(listener);
						unsubscribe();
						unsubscribeOperational?.();
					};
				},
				archive: archiveRequest,
			});
			dashboard = starting;
			void starting.catch(() => {
				if (dashboard === starting) dashboard = undefined;
			});
		}
		return dashboard;
	}

	pi.on("session_start", async (_event, ctx) => {
		active?.close?.();
		active = undefined;
		closeGoalWatcher();
		await closeDashboard();
		context = ctx;
		sessionId = ctx.sessionManager.getSessionId();
		persistenceWarning = false;
		knownMembers.clear();
		memberGoals.clear();
		channelGoals.clear();
		taskGoals.clear();
		goals.clear();
		latestGoal = undefined;
		archive = emptyArchive();
		store.clear();

		const branch = entries(ctx);
		for (const entry of branch) {
			if (entry.type !== "custom" || entry.customType !== CHAT_ARCHIVE_ENTRY_TYPE) continue;
			const data = entry.data as
				| { sessionId?: unknown; archive?: unknown; goals?: unknown }
				| undefined;
			if (data?.sessionId !== sessionId) continue;
			const restoredArchive = validateArchive(data.archive);
			const restoredGoals = validateGoals(data.goals, sessionId);
			if (restoredArchive && restoredGoals) {
				archive = restoredArchive;
				goals.clear();
				for (const goal of restoredGoals) goals.set(goal.id, goal);
			}
		}

		let walkingGoal: GoalState | undefined;
		for (const entry of branch) {
			if (entry.type !== "custom") continue;
			if (entry.customType === GOAL_ENTRY_TYPE) {
				const goal = validateGoal(entry.data, sessionId);
				if (goal) {
					goals.set(goal.id, goal);
					walkingGoal = goal;
					latestGoal = goal;
				}
				continue;
			}
			if (entry.customType !== CHAT_ENTRY_TYPE) continue;
			const data = entry.data as { sessionId?: unknown; record?: unknown } | undefined;
			if (data?.sessionId !== sessionId) continue;
			const validated = validateCoordinationChatRecord(data.record);
			if (!validated) continue;
			const restored = tagged(validated, walkingGoal);
			if (store.add(restored)) {
				associate(restored);
				rememberNames(restored);
			}
		}
		const metadataChanged = archiveCompletedGoals(false);
		if (metadataChanged) persistMetadata();
		unsubscribeMembers?.();
		unsubscribeMembers = options.subscribeMembers?.(notify);
		watchGoalEntries(ctx);
		notify();
	});

	pi.on("before_agent_start", () => {
		refreshGoals();
	});
	pi.on("tool_execution_end", () => {
		refreshGoals();
	});

	pi.on("session_shutdown", async () => {
		closeGoalWatcher();
		sessionId = undefined;
		context = undefined;
		unsubscribeMembers?.();
		unsubscribeMembers = undefined;
		active?.close?.();
		active = undefined;
		await closeDashboard();
		listeners.clear();
		knownMembers.clear();
		memberGoals.clear();
		channelGoals.clear();
		taskGoals.clear();
		goals.clear();
		latestGoal = undefined;
		archive = emptyArchive();
		store.clear();
	});

	pi.registerCommand("auto-bots", {
		description:
			"Open the local auto-bots chat dashboard in your browser (url: link only; clear: archive history; stop: close server)",
		handler: async (args, ctx) => {
			const input = args.trim();
			if (input === "clear") {
				archiveRequest({ scope: "all" });
				ctx.ui.notify(
					"Current chat history archived. Session records and agent state were not deleted.",
					"info",
				);
				return;
			}
			if (input === "stop") {
				await closeDashboard();
				ctx.ui.notify("Auto-bots dashboard stopped. Agents are unchanged.", "info");
				return;
			}
			const openingSession = sessionId;
			if (!openingSession) {
				ctx.ui.notify("Auto-bots chats are unavailable outside an active session.", "warning");
				return;
			}
			try {
				const server = await ensureDashboard();
				if (sessionId !== openingSession) return;
				const query = input === "url" ? "" : input;
				const url = server.url + (query ? `&q=${encodeURIComponent(query)}` : "");
				ctx.ui.notify(`Auto-bots chats: ${url}`, "info");
				const mode = (ctx as ExtensionContext & { mode?: string }).mode;
				if (input === "url" || (mode && mode !== "tui") || !ctx.hasUI) return;
				const result =
					process.platform === "darwin"
						? await pi.exec("open", [url], { timeout: 5000 })
						: process.platform === "win32"
							? await pi.exec("cmd", ["/c", "start", "", url], { timeout: 5000 })
							: await pi.exec("xdg-open", [url], { timeout: 5000 });
				if (result.code !== 0)
					ctx.ui.notify(
						"Could not open your browser automatically. Open the local URL above; the dashboard is running.",
						"warning",
					);
			} catch (error) {
				if (sessionId === openingSession)
					ctx.ui.notify(
						`Auto-bots dashboard: ${(error as Error).message}. If a URL was printed, you can open it manually.`,
						"warning",
					);
			}
		},
	});

	return (record) => {
		if (!sessionId) return;
		const validated = validateCoordinationChatRecord(record);
		if (!validated) return;
		refreshGoals();
		const accepted = tagged(validated);
		const previous = knownMembers.size;
		if (!store.add(accepted)) return;
		associate(accepted);
		rememberNames(accepted);
		const unarchived = unarchiveForNewTraffic(accepted);
		if (knownMembers.size !== previous || unarchived) notify();
		try {
			pi.appendEntry(CHAT_ENTRY_TYPE, { sessionId, record: accepted });
			if (unarchived) persistMetadata();
		} catch {
			warnPersistence();
		}
	};
}
