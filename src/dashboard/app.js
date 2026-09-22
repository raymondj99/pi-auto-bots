const MAX_RENDERED_MESSAGES = 100;
const clockFormatter = new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" });
const dateFormatter = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });

function string(value, fallback = "") { return typeof value === "string" ? value : fallback; }
function strings(value) { return Array.isArray(value) ? value.filter((entry) => typeof entry === "string") : []; }

function validRecord(record) {
  return !!record && typeof record === "object" && !!record.event && typeof record.event === "object" &&
    typeof record.event.id === "string" && typeof record.event.kind === "string" &&
    typeof record.event.from === "string" && typeof record.event.to === "string" &&
    typeof record.event.text === "string" && Number.isSafeInteger(record.timestamp) &&
    record.timestamp >= 0 && record.timestamp <= 8_640_000_000_000_000;
}
function validMember(member) {
  return !!member && typeof member === "object" && typeof member.id === "string" &&
    typeof member.name === "string" && typeof member.status === "string";
}
function validGoal(goal) {
  return !!goal && typeof goal === "object" && typeof goal.id === "string" &&
    typeof goal.title === "string" && typeof goal.status === "string" && Number.isSafeInteger(goal.startedAt);
}
function normalizeArchive(value) {
  const source = value && typeof value === "object" ? value : {};
  return {
    goalIds: strings(source.goalIds), eventIds: strings(source.eventIds),
    memberIds: strings(source.memberIds), channelIds: strings(source.channelIds),
  };
}

/** Normalize untrusted snapshot data while retaining only records with stable event IDs. */
export function normalizeDashboardSnapshot(value) {
  const source = value && typeof value === "object" ? value : {};
  const seen = new Set();
  const records = [];
  for (const record of Array.isArray(source.records) ? source.records : []) {
    if (!validRecord(record) || seen.has(record.event.id)) continue;
    seen.add(record.event.id);
    records.push(record);
  }
  return {
    sessionName: string(source.sessionName, "Current session"), records,
    dropped: Number.isSafeInteger(source.dropped) && source.dropped >= 0 ? source.dropped : 0,
    members: (Array.isArray(source.members) ? source.members : []).filter(validMember),
    archive: normalizeArchive(source.archive),
    goals: (Array.isArray(source.goals) ? source.goals : []).filter(validGoal),
    operational: source.operational && typeof source.operational === "object" ? source.operational : undefined,
  };
}

/** Apply SSE deltas without mutating the prior snapshot. Archive and goal fields are full replacements. */
export function applyDashboardUpdates(snapshot, updates) {
  let next = normalizeDashboardSnapshot(snapshot);
  for (const update of updates) {
    if (!update || typeof update !== "object") continue;
    const removed = new Set(strings(update.removedIds));
    const records = next.records.filter((record) => !removed.has(record.event.id));
    const ids = new Set(records.map((record) => record.event.id));
    for (const record of Array.isArray(update.records) ? update.records : []) {
      if (validRecord(record) && !ids.has(record.event.id)) { records.push(record); ids.add(record.event.id); }
    }
    next = normalizeDashboardSnapshot({
      sessionName: typeof update.sessionName === "string" ? update.sessionName : next.sessionName,
      records, dropped: Number.isSafeInteger(update.dropped) && update.dropped >= 0 ? update.dropped : next.dropped,
      members: Array.isArray(update.members) ? update.members : next.members,
      archive: Object.prototype.hasOwnProperty.call(update, "archive") ? update.archive : next.archive,
      goals: Object.prototype.hasOwnProperty.call(update, "goals") ? update.goals : next.goals,
      operational: Object.prototype.hasOwnProperty.call(update, "operational") ? update.operational : next.operational,
    });
  }
  return next;
}

function recordChannel(record) {
  const channel = record.event.channel;
  return channel && typeof channel === "object" && typeof channel.id === "string" && channel.id &&
    typeof channel.name === "string" && Array.isArray(channel.members) ? channel : undefined;
}
function recordSearchText(record) {
  const event = record.event;
  const task = event.task;
  const channel = recordChannel(record);
  return [event.id, event.kind, event.from, event.to, event.text, record.fromName, record.toName,
    record.goalId, record.goalTitle, ...strings(event.recipients), channel?.id, channel?.name,
    ...strings(channel?.members), task?.id, task?.title, task?.context, task?.nextSteps,
    task?.owner, task?.createdBy, task?.status, task?.revision].filter((part) => part !== undefined).join(" ").toLocaleLowerCase();
}
function recordInvolves(record, id) {
  const event = record.event;
  return event.from === id || event.to === id || strings(event.recipients).includes(id) ||
    event.task?.owner === id || event.task?.createdBy === id;
}
function isFinishedStatus(status) { return status === "finished" || status === "archived" || status === "completed" || status === "cancelled"; }

/** Build named-agent timelines and explicit coordinator channels. No synthetic activity, DM, or task rows. */
export function projectDashboard(snapshot, options = {}) {
  const data = normalizeDashboardSnapshot(snapshot);
  const showArchived = options.showArchived === true;
  const query = string(options.query).trim().toLocaleLowerCase();
  const archivedGoals = new Set(data.archive.goalIds);
  const archivedEvents = new Set(data.archive.eventIds);
  const archivedMembers = new Set(data.archive.memberIds);
  const archivedChannels = new Set(data.archive.channelIds);
  const names = new Map(data.members.map((member) => [member.id, member.name]));
  for (const record of data.records) {
    if (!names.has(record.event.from)) names.set(record.event.from, string(record.fromName, record.event.from));
    if (!recordChannel(record) && !names.has(record.event.to)) names.set(record.event.to, string(record.toName, record.event.to));
  }
  const idsByName = new Map();
  for (const [id, name] of names) { const ids = idsByName.get(name) || []; ids.push(id); idsByName.set(name, ids); }
  const labels = new Map([...names].map(([id, name]) => {
    const duplicates = idsByName.get(name) || [];
    if (duplicates.length < 2) return [id, name];
    let length = Math.min(6, id.length);
    while (length < id.length && duplicates.some((other) => other !== id && other.slice(0, length) === id.slice(0, length))) length++;
    return [id, `${name} (${id.slice(0, length)})`];
  }));
  const recordArchived = (record) => archivedEvents.has(record.event.id) ||
    (typeof record.goalId === "string" && archivedGoals.has(record.goalId));
  const visibleRecords = showArchived ? data.records : data.records.filter((record) => !recordArchived(record));

  const allMembers = data.members.map((member) => ({
    ...member,
    archived: member.id !== "parent" && (archivedMembers.has(member.id) || isFinishedStatus(member.status.toLocaleLowerCase()) ||
      (typeof member.goalId === "string" && archivedGoals.has(member.goalId))),
  }));
  const members = showArchived ? allMembers : allMembers.filter((member) => !member.archived || member.id === "parent");
  const threads = [];
  allMembers.forEach((member, order) => {
    if (!showArchived && member.archived && member.id !== "parent") return;
    const records = visibleRecords.filter((record) => recordInvolves(record, member.id));
    const matches = query ? records.filter((record) => recordSearchText(record).includes(query)) : [];
    const titleMatch = query && `${member.name} ${member.role || ""} ${member.status}`.toLocaleLowerCase().includes(query);
    if (query && !titleMatch && matches.length === 0) return;
    const last = records.at(-1);
    threads.push({ id: `agent:${member.id}`, kind: "agent", entityId: member.id, title: member.name,
      subtitle: [member.role, member.backend, member.status].filter(Boolean).join(" · "), records,
      latestTimestamp: last?.timestamp || 0, preview: timelinePreview(last, member.id, labels), order,
      archived: member.archived, matchIds: matches.map((record) => record.event.id), matchCount: matches.length });
  });

  const channels = new Map();
  data.records.forEach((record, order) => {
    const channel = recordChannel(record);
    if (!channel) return;
    let item = channels.get(channel.id);
    if (!item) item = { id: `channel:${channel.id}`, kind: "channel", entityId: channel.id, channel,
      allRecords: [], order, originatingGoalId: typeof record.goalId === "string" ? record.goalId : undefined };
    item.channel = channel;
    item.allRecords.push(record);
    item.order = order;
    if (!item.originatingGoalId && typeof record.goalId === "string") item.originatingGoalId = record.goalId;
    channels.set(channel.id, item);
  });
  for (const item of channels.values()) {
    const channel = item.channel;
    const archived = archivedChannels.has(channel.id) || channel.status === "closed" ||
      (!!item.originatingGoalId && archivedGoals.has(item.originatingGoalId));
    if (!showArchived && archived) continue;
    const records = item.allRecords.filter((record) => showArchived || !recordArchived(record));
    const memberText = strings(channel.members).map((id) => labels.get(id) || id).join(", ");
    const titleMatch = query && `${channel.name} ${channel.id} ${memberText}`.toLocaleLowerCase().includes(query);
    const matches = query ? records.filter((record) => recordSearchText(record).includes(query)) : [];
    if (query && !titleMatch && matches.length === 0) continue;
    const last = records.at(-1) || item.allRecords.at(-1);
    threads.push({ id: item.id, kind: "channel", entityId: channel.id, title: channel.name,
      subtitle: memberText || "No current members", participantIds: [...strings(channel.members)], records,
      latestTimestamp: last?.timestamp || 0, preview: timelinePreview(last, "", labels), order: item.order,
      archived, status: channel.status, matchIds: matches.map((record) => record.event.id), matchCount: matches.length });
  }
  threads.sort((a, b) => b.latestTimestamp - a.latestTimestamp || b.order - a.order || a.id.localeCompare(b.id));
  return { ...data, records: data.records, names: labels, allMembers, members, threads };
}

export function initials(name) {
  const parts = string(name, "?").trim().split(/\s+/).filter(Boolean);
  return (parts.length > 1 ? `${parts[0][0]}${parts.at(-1)[0]}` : parts[0]?.slice(0, 2) || "?").toLocaleUpperCase();
}
export function avatarHue(id) {
  let hash = 0;
  for (const character of string(id)) hash = ((hash << 5) - hash + character.codePointAt(0)) | 0;
  return Math.abs(hash) % 360;
}
function taskVerb(kind) {
  if (kind === "assign") return "Assigned";
  if (kind === "handoff") return "Handed off";
  if (kind === "complete") return "Completed";
  return "Task update";
}
function timelineLabel(record, agentId, names) {
  const event = record.event;
  const channel = recordChannel(record);
  if (agentId) {
    if (event.from === agentId) return `Messaged ${channel ? `#${channel.name}` : names.get(event.to) || event.to}`;
    return `Message from ${names.get(event.from) || string(record.fromName, event.from)}${channel ? ` · #${channel.name}` : ""}`;
  }
  return channel ? `Message from ${names.get(event.from) || record.fromName} · #${channel.name}` : event.text;
}
function timelinePreview(record, agentId, names) {
  if (!record) return "No messages yet";
  const prefix = timelineLabel(record, agentId, names);
  const text = record.event.task ? `${taskVerb(record.event.kind)} · ${string(record.event.task.title, record.event.text)}` : record.event.text.replace(/\s+/g, " ");
  return `${prefix}: ${text}`;
}
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function button(className, label, action) {
  const node = el("button", className); node.type = "button"; node.setAttribute("aria-label", label); node.addEventListener("click", action); return node;
}
function createAvatar(id, name, channel = false) {
  const node = el("span", `avatar${channel ? " channel" : ` color-${avatarHue(id) % 10}`}`, channel ? "#" : initials(name));
  node.setAttribute("aria-hidden", "true"); return node;
}
function formatTime(timestamp) {
  if (!Number.isFinite(timestamp)) return "";
  const date = new Date(timestamp); return date.toDateString() === new Date().toDateString() ? clockFormatter.format(date) : dateFormatter.format(date);
}

function startDashboard() {
  const dom = {
    app: document.querySelector("#app"), session: document.querySelector("#session-name"), agents: document.querySelector("#agent-list"),
    threads: document.querySelector("#thread-list"), count: document.querySelector("#conversation-count"), search: document.querySelector("#search"),
    channels: document.querySelector("#channels-section"), archived: document.querySelector("#show-archived"), clear: document.querySelector("#clear-chats"),
    options: document.querySelector("#chat-options"), operations: document.querySelector("#operations-panel"),
    archiveSelected: document.querySelector("#archive-selected"), emptyInbox: document.querySelector("#empty-inbox"), title: document.querySelector("#conversation-title"),
    subtitle: document.querySelector("#conversation-subtitle"), details: document.querySelector("#details"), banner: document.querySelector("#connection-banner"),
    dot: document.querySelector("#connection-dot"), connectionLabel: document.querySelector("#connection-label"), transcript: document.querySelector("#transcript"),
    messages: document.querySelector("#messages"), emptyConversation: document.querySelector("#empty-conversation"), loadOlder: document.querySelector("#load-older"),
    jump: document.querySelector("#jump-latest"), back: document.querySelector("#back"), mutationStatus: document.querySelector("#mutation-status"),
    operational: document.querySelector("#operational-summary"),
  };
  let snapshot = normalizeDashboardSnapshot({});
  let selectedId = "", query = "", showArchived = false, showDetails = false, visibleLimit = MAX_RENDERED_MESSAGES;
  let loaded = false, pendingUpdates = [], frame = 0, renderedThreadId = "", windowFirstId = "", windowHasLater = false;
  let agentsSignature = "", threadsSignature = "", capability = "", archiving = false, renderedQuery = "";
  const unread = new Map();
  const projection = () => projectDashboard(snapshot, { query, showArchived });
  const selectedThread = (view) => view.threads.find((thread) => thread.id === selectedId);

  function setConnection(kind, message = "") {
    dom.dot.className = `connection-dot ${kind}`;
    dom.connectionLabel.textContent = kind === "connected" ? "Live" : kind === "reconnecting" ? "Reconnecting" : kind === "error" ? "Offline" : "Connecting";
    dom.dot.title = dom.connectionLabel.textContent;
    dom.banner.className = `connection-banner${message ? " visible" : ""}${kind === "error" ? " error" : ""}`; dom.banner.textContent = message;
  }
  function choose(id, mobile = true) { selectedId = id; visibleLimit = MAX_RENDERED_MESSAGES; unread.delete(id); dom.options.open = false; dom.mutationStatus.textContent = ""; render({ follow: true, searchMatch: true }); if (mobile) dom.app.classList.add("mobile-conversation"); }
  function chooseAgent(id) { choose(`agent:${id}`); }
  function render(options = {}) {
    const view = projection();
    if (selectedId && !view.threads.some((thread) => thread.id === selectedId)) selectedId = "";
    if (!selectedId) selectedId = view.threads.find((thread) => thread.kind === "channel")?.id || view.threads[0]?.id || "";
    dom.session.textContent = view.sessionName; renderOperational(); renderAgents(view); renderThreads(view); renderConversation(view, options);
    dom.emptyInbox.hidden = view.threads.length > 0;
    dom.emptyInbox.textContent = query ? "No chats match your search." : "No chats yet.";
  }
  function renderOperational() {
    const state = snapshot.operational;
    dom.operations.hidden = !state;
    dom.operational.hidden = !state;
    if (!state) { dom.operational.replaceChildren(); return; }
    if (!dom.operations.open) return;
    const tasks = Array.isArray(state.tasks) ? state.tasks : [], blockers = Array.isArray(state.blockers) ? state.blockers : [], artifacts = Array.isArray(state.artifacts) ? state.artifacts : [], budgets = Array.isArray(state.budgets) ? state.budgets : [];
    const pending = blockers.reduce((sum, blocker) => sum + (Array.isArray(blocker.pendingAcknowledgements) ? blocker.pendingAcknowledgements.length : 0), 0);
    const dependencies = tasks.reduce((sum, task) => sum + (Array.isArray(task.dependencies) ? task.dependencies.length : 0), 0);
    const artifactVersions = artifacts.slice(0, 8).map((artifact) => `${String(artifact.artifactId)}@${Number(artifact.version)} ${String(artifact.state)}`).join(", ") || "none";
    const leases = budgets.slice(0, 8).map((budget) => `${String(budget.id)} ${Number(budget.used)}/${Number(budget.limit)}`).join(", ") || "none";
    const expanded = new Set([...dom.operational.querySelectorAll("details[open]")].map((node) => node.dataset.group));
    const text = (value) => String(value ?? "").slice(0, 300);
    const list = (values) => Array.isArray(values) ? values.slice(0, 20).map(text).join(", ") || "none" : "none";
    const section = (name, values, describe) => {
      const detail = document.createElement("details"); detail.dataset.group = name; detail.open = expanded.has(name);
      detail.append(el("summary", "", `${name} (${values.length})`));
      const rows = document.createElement("ul");
      for (const value of values.slice(0, 100)) rows.append(el("li", "", describe(value)));
      if (values.length > 100) rows.append(el("li", "", `${values.length - 100} more omitted`));
      detail.append(rows); return detail;
    };
    dom.operational.hidden = false;
    dom.operational.replaceChildren(
      el("strong", "", "Coordination operations · read-only"),
      el("span", "", `${tasks.filter((task) => task.state === "ready").length} ready · ${dependencies} dependencies · ${blockers.length} blockers`),
      el("span", "", `${pending} pending acks · artifacts ${artifactVersions}`),
      el("span", "", `Leases ${leases} · audit ${Number.isSafeInteger(state.cursor) ? state.cursor : 0}`),
      section("Tasks", tasks, (task) => `${text(task.id)} · ${text(task.state)}${task.pendingAction ? ` (${text(task.pendingAction)})` : ""} · owner ${text(task.owner)}${task.ownerOnline === false ? " (offline)" : ""} · revision ${Number(task.revision)} · dependencies: ${list(task.dependencyStates?.map((edge) => `${edge.id}=${edge.state}`) ?? task.dependencies)} · blockers: ${list(task.blockers)}`),
      section("Blockers and acknowledgements", blockers, (blocker) => `${text(blocker.id)} · ${text(blocker.severity)} · ${text(blocker.summary)} · pending: ${list(blocker.pendingAcknowledgements)}`),
      section("Artifact versions", artifacts, (artifact) => `${text(artifact.artifactId)}@${Number(artifact.version)} · ${text(artifact.state)} · ${text(artifact.digest)} · consumers: ${list(artifact.consumers)}`),
      section("Budgets", budgets, (budget) => `${text(budget.id)} · ${Number(budget.used)}/${Number(budget.limit)} charged · ${Number(budget.dispatchedUnknown)} outcome_unknown`),
      section("Delivery", Array.isArray(state.deliveries) ? state.deliveries : [], (delivery) => `${text(delivery.id)} → ${text(delivery.targetRoleId)} · ${text(delivery.state)}`),
      section("Audit", Array.isArray(state.audit) ? state.audit : [], (event) => `#${Number(event.seq)} · ${text(event.type)} · ${text(event.actorRoleId)}`),
    );
  }
  function renderAgents(view) {
    const timelines = new Map(view.threads.filter((thread) => thread.kind === "agent").map((thread) => [thread.entityId, thread]));
    const members = view.members.filter((member) => timelines.has(member.id));
    const signature = JSON.stringify([selectedId, showArchived, members, [...timelines.values()].map((thread) => [thread.preview, thread.latestTimestamp]), [...view.names], [...unread]]);
    if (signature === agentsSignature) return; agentsSignature = signature;
    const focused = dom.agents.contains(document.activeElement) ? document.activeElement.dataset.agentId : undefined;
    const fragment = document.createDocumentFragment();
    for (const member of members) {
      const id = `agent:${member.id}`, selected = selectedId === id, name = view.names.get(member.id) || member.name;
      const status = member.archived ? "Archived" : member.status;
      const hasUnread = (unread.get(id) || 0) > 0;
      const item = button(`agent-button${selected ? " selected" : ""}`, `Open ${name} timeline${hasUnread ? " · Unread messages" : ""}`, () => chooseAgent(member.id));
      item.setAttribute("aria-current", selected ? "page" : "false"); item.dataset.agentId = member.id;
      const thread = timelines.get(member.id), last = thread.records.at(-1);
      const avatar = el("span", "avatar-wrap"); avatar.append(createAvatar(member.id, name));
      const indicator = el("span", `status-pip ${member.status.toLocaleLowerCase()}`);
      indicator.setAttribute("aria-hidden", "true"); avatar.append(indicator);
      const copy = el("span", "agent-copy"), heading = el("span", "chat-row-heading");
      heading.append(el("span", "agent-name", name), el("time", "chat-time", thread.latestTimestamp ? formatTime(thread.latestTimestamp) : ""));
      const preview = last ? (last.event.task?.title || last.event.text).replace(/\s+/g, " ") : "No messages yet";
      copy.append(heading, el("span", "agent-meta", member.archived ? `Archived · ${preview}` : preview));
      item.title = `${name} · ${status}`;
      item.append(avatar, copy);
      if (hasUnread) item.append(el("span", "unread-dot"));
      fragment.append(item);
    }
    dom.agents.replaceChildren(fragment);
    if (focused !== undefined) [...dom.agents.children].find((node) => node.dataset.agentId === focused)?.focus({ preventScroll: true });
  }
  function renderThreads(view) {
    const channels = view.threads.filter((thread) => thread.kind === "channel");
    dom.channels.hidden = channels.length === 0;
    const signature = JSON.stringify([selectedId, channels.map((thread) => [thread.id, thread.title, thread.preview, thread.latestTimestamp, thread.subtitle, thread.archived, unread.get(thread.id)])]);
    if (signature === threadsSignature) return; threadsSignature = signature;
    const focused = dom.threads.contains(document.activeElement) ? document.activeElement.dataset.threadId : undefined;
    const fragment = document.createDocumentFragment();
    for (const thread of channels) {
      const selected = thread.id === selectedId;
      const item = button(`thread-button${selected ? " selected" : ""}`, `Open channel ${thread.title}`, () => choose(thread.id));
      item.setAttribute("aria-current", selected ? "page" : "false"); item.dataset.threadId = thread.id;
      const title = el("span", "thread-title", thread.title); if (thread.archived) title.append(el("span", "archive-badge", "Archived"));
      item.title = thread.subtitle;
      item.append(title, el("time", "chat-time", thread.latestTimestamp ? formatTime(thread.latestTimestamp) : ""));
      if ((unread.get(thread.id) || 0) > 0) {
        item.append(el("span", "unread-dot"));
        item.setAttribute("aria-label", `Open channel ${thread.title} · Unread messages`);
      }
      const last = thread.records.at(-1);
      item.append(el("span", "thread-preview", last ? (last.event.task?.title || last.event.text).replace(/\s+/g, " ") : "No messages yet"));
      fragment.append(item);
    }
    dom.threads.replaceChildren(fragment);
    if (focused !== undefined) [...dom.threads.children].find((node) => node.dataset.threadId === focused)?.focus({ preventScroll: true });
    dom.count.textContent = `${channels.length} ${channels.length === 1 ? "channel" : "channels"}`;
  }
  function renderConversation(view, options = {}) {
    const thread = selectedThread(view);
    dom.title.textContent = thread ? (thread.kind === "agent" ? view.names.get(thread.entityId) || thread.title : thread.title) : "Chats";
    const member = view.members.find((entry) => entry.id === thread?.entityId);
    const retention = view.dropped ? ` · ${view.dropped} older ${view.dropped === 1 ? "event" : "events"} not retained` : "";
    const subtitle = thread ? (showDetails ? `${thread.entityId} · ${thread.subtitle}` : thread.kind === "channel" ? thread.subtitle : member?.status || "") : "";
    dom.subtitle.textContent = `${thread?.archived ? "Archived · " : ""}${subtitle}${retention}`;
    dom.subtitle.title = dom.subtitle.textContent;
    dom.details.setAttribute("aria-pressed", String(showDetails)); dom.details.textContent = showDetails ? "Hide details" : "Show details";
    dom.archiveSelected.hidden = !thread || thread.archived || thread.entityId === "parent"; dom.archiveSelected.disabled = archiving; dom.clear.disabled = archiving;
    const records = thread?.records || []; dom.emptyConversation.hidden = records.length > 0;
    dom.emptyConversation.textContent = query && !thread ? "No chats match your search." : thread ? "No messages yet." : "Choose a chat to get started.";
    if (options.skipMessages) { updateJump(); return; }
    const changedThread = renderedThreadId !== thread?.id, changedQuery = renderedQuery !== query;
    renderedQuery = query;
    const oldHeight = dom.transcript.scrollHeight, oldTop = dom.transcript.scrollTop, top = dom.transcript.getBoundingClientRect().top;
    const anchor = [...dom.messages.children].find((node) => node.getBoundingClientRect().bottom > top), anchorId = anchor?.dataset.eventId, anchorOffset = anchor ? anchor.getBoundingClientRect().top - top : 0;
    const wasFollowing = changedThread || (options.follow ?? (!windowHasLater && oldHeight - oldTop - dom.transcript.clientHeight < 50));
    let first = Math.max(0, records.length - visibleLimit);
    if (!wasFollowing || options.prepended) {
      const existing = records.findIndex((record) => record.event.id === windowFirstId), surviving = records.findIndex((record) => record.event.id === anchorId);
      if (existing >= 0) first = options.prepended ? Math.max(0, existing - MAX_RENDERED_MESSAGES) : existing; else if (surviving >= 0) first = surviving; else first = 0;
    }
    const matchIndex = query && (changedQuery || changedThread || options.searchMatch) ? records.findIndex((record) => thread.matchIds.includes(record.event.id)) : -1;
    if (matchIndex >= 0) first = Math.max(0, matchIndex - 1);
    const windowRecords = records.slice(first, first + visibleLimit); windowFirstId = windowRecords[0]?.event.id || ""; windowHasLater = first + windowRecords.length < records.length; renderedThreadId = thread?.id || ""; dom.loadOlder.hidden = first === 0;
    const fragment = document.createDocumentFragment(); for (const record of windowRecords) fragment.append(renderMessage(record, view.names, thread)); dom.messages.replaceChildren(fragment);
    requestAnimationFrame(() => {
      if (matchIndex >= 0) dom.messages.querySelector(`[data-event-id]`)?.scrollIntoView({ block: "start" });
      else if (wasFollowing && !options.prepended) dom.transcript.scrollTop = dom.transcript.scrollHeight;
      else { const retained = [...dom.messages.children].find((node) => node.dataset.eventId === anchorId); if (!changedThread && retained) dom.transcript.scrollTop += retained.getBoundingClientRect().top - dom.transcript.getBoundingClientRect().top - anchorOffset; else if (options.prepended) dom.transcript.scrollTop = oldTop + dom.transcript.scrollHeight - oldHeight; else dom.transcript.scrollTop = oldTop; }
      updateJump();
    });
  }
  function renderMessage(record, names, thread) {
    const event = record.event, sender = names.get(event.from) || string(record.fromName, event.from), channel = recordChannel(record);
    const outgoing = thread?.kind === "agent" ? event.from === thread.entityId : event.from === "parent";
    const article = el("article", `message ${outgoing ? "outgoing" : "incoming"}`); article.dataset.eventId = event.id;
    const body = el("div", "message-body"), line = el("div", "message-line"), time = el("time", "timestamp", formatTime(record.timestamp)); time.dateTime = new Date(record.timestamp).toISOString();
    const direction = thread?.kind === "agent" ? timelineLabel(record, thread.entityId, names) : `Message from ${sender}`;
    article.setAttribute("aria-label", direction);
    const recipient = channel ? `#${channel.name}` : names.get(event.to) || event.to;
    const label = outgoing && thread?.kind === "agent" ? `To ${recipient}` : `${sender}${channel && thread?.kind === "agent" ? ` · #${channel.name}` : ""}`;
    const archived = !!thread?.archived || snapshot.archive.eventIds.includes(event.id) || (typeof record.goalId === "string" && snapshot.archive.goalIds.includes(record.goalId));
    line.append(el("span", "direction-label", label)); if (archived) line.append(el("span", "archive-badge", "Archived"));
    article.append(line);
    if (event.kind === "channel_create" || event.kind === "channel_close") body.append(el("div", "system-message", event.kind === "channel_create" ? "Channel created" : "Channel closed"));
    else if (event.task && typeof event.task === "object") {
      const card = el("div", `task-card${event.kind === "complete" ? " complete" : ""}`); card.append(el("div", "task-kind", taskVerb(event.kind)), el("div", "task-title", string(event.task.title, "Shared task")));
      const owner = names.get(event.task.owner) || event.task.owner, ownerRow = el("div", "task-row"); ownerRow.append(el("strong", "", event.kind === "complete" ? "Final owner  " : "Owner  "), document.createTextNode(owner)); card.append(ownerRow);
      if (event.task.context) { const row = el("div", "task-row"); row.append(el("strong", "", "Context  "), document.createTextNode(event.task.context)); card.append(row); }
      if (event.task.nextSteps && event.kind !== "complete") { const row = el("div", "task-row"); row.append(el("strong", "", "Next  "), document.createTextNode(event.task.nextSteps)); card.append(row); } body.append(card);
    } else {
      const text = el("div", "message-text");
      const needle = query.trim().toLocaleLowerCase(), source = event.text, lower = source.toLocaleLowerCase();
      let cursor = 0, index = needle ? lower.indexOf(needle) : -1;
      while (index >= 0) {
        text.append(document.createTextNode(source.slice(cursor, index)), el("mark", "", source.slice(index, index + needle.length)));
        cursor = index + needle.length; index = lower.indexOf(needle, cursor);
      }
      text.append(document.createTextNode(source.slice(cursor))); body.append(text);
    }
    if (showDetails && record.goalTitle) body.append(el("div", "goal-tag", `Goal · ${record.goalTitle}`));
    if (showDetails) { const lines = [`Event ${event.id}`, `Route ${event.from} → ${event.to}`]; if (channel) lines.push(`Channel ${channel.id} · ${channel.status}`); if (Array.isArray(event.recipients)) lines.push(`Accepted recipients ${event.recipients.join(", ") || "none"}`); if (record.goalId) lines.push(`Goal ${record.goalId}`); if (event.task) lines.push(`Task ${event.task.id} · revision ${event.task.revision} · ${event.task.status}`); lines.push("Delivery: accepted to queue; read status is not observed."); body.append(el("div", "audit", lines.join("\n"))); }
    article.append(body, time); return article;
  }
  function updateJump() { const away = dom.transcript.scrollHeight - dom.transcript.scrollTop - dom.transcript.clientHeight > 50; dom.jump.hidden = !(away || windowHasLater); }
  function scheduleUpdate(update) {
    pendingUpdates.push(update); if (frame) return;
    frame = requestAnimationFrame(() => {
      frame = 0; const updates = pendingUpdates; pendingUpdates = []; const before = snapshot, priorView = projection(), selectedBefore = selectedThread(priorView), following = !windowHasLater && dom.transcript.scrollHeight - dom.transcript.scrollTop - dom.transcript.clientHeight < 50;
      snapshot = applyDashboardUpdates(snapshot, updates); const nextView = projection(), previousIds = new Set(before.records.map((record) => record.event.id));
      for (const record of snapshot.records.filter((entry) => !previousIds.has(entry.event.id))) for (const thread of nextView.threads.filter((entry) => entry.records.some((item) => item.event.id === record.event.id))) if (thread.id !== selectedId || !following) unread.set(thread.id, (unread.get(thread.id) || 0) + 1);
      const selectedAfter = selectedThread(nextView), sameSelectedRecords = !!selectedBefore && !!selectedAfter && selectedBefore.records.length === selectedAfter.records.length && selectedBefore.records.every((record, index) => record.event.id === selectedAfter.records[index].event.id);
      const sameArchive = JSON.stringify(before.archive) === JSON.stringify(snapshot.archive) && selectedBefore?.archived === selectedAfter?.archived;
      const sameNames = JSON.stringify([...priorView.names]) === JSON.stringify([...nextView.names]);
      render({ follow: following, skipMessages: sameSelectedRecords && sameArchive && sameNames });
    });
  }
  async function archive(scope, id) {
    if (archiving) return;
    dom.options.open = false;
    const description = scope === "all" ? "Archive all current chats? You can find them under Show archived. Nothing is deleted." : `Archive ${selectedThread(projection())?.title || "this item"}? You can reveal it with Show archived.`;
    if (!window.confirm(description)) return;
    archiving = true; dom.mutationStatus.textContent = "Archiving…"; render({ skipMessages: true });
    try {
      const response = await fetch("/api/archive", { method: "POST", headers: { Authorization: `Bearer ${capability}`, "Content-Type": "application/json" }, body: JSON.stringify(id ? { scope, id } : { scope }) });
      if (!response.ok) throw new Error(`Archive request failed (${response.status})`);
      const result = await response.json(); if (!result || result.ok !== true) throw new Error("The archive response was invalid");
      dom.mutationStatus.textContent = "History archived.";
    } catch (error) { dom.mutationStatus.textContent = `${error instanceof Error ? error.message : "Archive failed"}. Nothing was changed in this view.`; }
    finally { archiving = false; render({ skipMessages: true }); }
  }

  dom.search.addEventListener("input", () => { query = dom.search.value; render(); });
  dom.operations.addEventListener("toggle", renderOperational);
  document.addEventListener("click", (event) => { if (!dom.options.contains(event.target)) dom.options.open = false; });
  dom.archived.addEventListener("change", () => { showArchived = dom.archived.checked; render(); });
  dom.clear.addEventListener("click", () => archive("all"));
  dom.archiveSelected.addEventListener("click", () => { const thread = selectedThread(projection()); if (thread) archive(thread.kind, thread.entityId); });
  dom.details.addEventListener("click", () => { showDetails = !showDetails; dom.options.open = false; render(); }); dom.back.addEventListener("click", () => dom.app.classList.remove("mobile-conversation"));
  dom.loadOlder.addEventListener("click", () => { visibleLimit += MAX_RENDERED_MESSAGES; render({ prepended: true }); }); dom.jump.addEventListener("click", () => { unread.delete(selectedId); dom.transcript.scrollTop = dom.transcript.scrollHeight; render({ follow: true }); }); dom.transcript.addEventListener("scroll", updateJump, { passive: true });
  document.addEventListener("keydown", (event) => {
    if (event.key === "/" && !event.ctrlKey && !event.metaKey && !event.altKey && document.activeElement !== dom.search) {
      event.preventDefault(); dom.app.classList.remove("mobile-conversation"); dom.search.focus();
    }
    if (event.key === "Escape") {
      if (dom.options.open) { dom.options.open = false; dom.options.querySelector("summary").focus(); }
      else if (document.activeElement === dom.search && query) { dom.search.value = ""; query = ""; render(); }
      else dom.app.classList.remove("mobile-conversation");
    }
  });

  const fragment = location.hash.slice(1), [tokenPart, ...parameters] = fragment.split("&");
  try { capability = decodeURIComponent(tokenPart || ""); } catch { setConnection("error", "This dashboard link is invalid. Reopen it from /auto-bots."); render(); return; }
  const params = new URLSearchParams(parameters.join("&")); query = params.get("q") || ""; dom.search.value = query;
  if (!capability) { setConnection("error", "This dashboard link is missing its local access capability. Reopen it from /auto-bots."); render(); return; }
  const events = new EventSource(`/api/events?token=${encodeURIComponent(capability)}`);
  events.addEventListener("open", () => setConnection("connected"));
  events.addEventListener("snapshot", (event) => { try { const following = !loaded || (!windowHasLater && dom.transcript.scrollHeight - dom.transcript.scrollTop - dom.transcript.clientHeight < 50); if (frame) cancelAnimationFrame(frame); frame = 0; pendingUpdates = []; snapshot = normalizeDashboardSnapshot(JSON.parse(event.data)); loaded = true; setConnection("connected"); render({ follow: following }); } catch { setConnection("error", "The local session returned unreadable data."); } });
  events.addEventListener("update", (event) => { try { scheduleUpdate(JSON.parse(event.data)); setConnection("connected"); } catch { setConnection("error", "A local update could not be read. Waiting for a fresh snapshot…"); } });
  events.addEventListener("error", () => setConnection("reconnecting", loaded ? "Connection paused. Reconnecting locally. If Pi ended or reloaded, run /auto-bots for a new link." : "Unable to connect. Retrying locally; reopen /auto-bots if this session has ended."));
  window.addEventListener("pagehide", () => { events.close(); if (frame) cancelAnimationFrame(frame); pendingUpdates = []; }, { once: true }); render();
}

if (typeof document !== "undefined") startDashboard();
