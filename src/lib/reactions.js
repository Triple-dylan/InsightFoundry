import { newId } from "./state.js";

function nowIso() {
  return new Date().toISOString();
}

function requireMessage(state, tenantId, messageId) {
  const message = state.chatMessages.find((item) => item.tenantId === tenantId && item.id === messageId);
  if (!message) {
    const err = new Error(`Chat message '${messageId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return message;
}

function normalizeEmoji(value) {
  const emoji = String(value ?? "").trim();
  if (!emoji) {
    const err = new Error("emoji is required");
    err.statusCode = 400;
    throw err;
  }
  return emoji.slice(0, 24);
}

export function listMessageReactions(state, tenantId, messageId, viewerUserId = null) {
  requireMessage(state, tenantId, messageId);
  const grouped = new Map();
  const records = state.messageReactions
    .filter((item) => item.tenantId === tenantId && item.messageId === messageId)
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));

  for (const reaction of records) {
    const key = reaction.emoji;
    if (!grouped.has(key)) {
      grouped.set(key, {
        emoji: reaction.emoji,
        count: 0,
        userIds: [],
        reactionIds: [],
        mineReactionId: null
      });
    }
    const entry = grouped.get(key);
    entry.count += 1;
    entry.userIds.push(reaction.userId);
    entry.reactionIds.push(reaction.id);
    if (viewerUserId && reaction.userId === viewerUserId && !entry.mineReactionId) {
      entry.mineReactionId = reaction.id;
    }
  }

  return [...grouped.values()];
}

export function addMessageReaction(state, tenantId, messageId, userId, emoji) {
  requireMessage(state, tenantId, messageId);
  const normalized = normalizeEmoji(emoji);
  const existing = state.messageReactions.find(
    (item) => item.tenantId === tenantId
      && item.messageId === messageId
      && item.userId === userId
      && item.emoji === normalized
  );
  if (existing) return existing;

  const reaction = {
    id: newId("reaction"),
    tenantId,
    messageId,
    userId,
    emoji: normalized,
    createdAt: nowIso()
  };
  state.messageReactions.push(reaction);
  return reaction;
}

export function removeMessageReaction(state, tenantId, messageId, reactionId, actorUserId, actorRole = "viewer") {
  requireMessage(state, tenantId, messageId);
  const index = state.messageReactions.findIndex(
    (item) => item.tenantId === tenantId && item.messageId === messageId && item.id === reactionId
  );
  if (index < 0) {
    const err = new Error(`Reaction '${reactionId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  const reaction = state.messageReactions[index];
  const privileged = ["owner", "admin", "operator"].includes(String(actorRole));
  if (!privileged && reaction.userId !== actorUserId) {
    const err = new Error("Cannot remove another user's reaction");
    err.statusCode = 403;
    throw err;
  }
  state.messageReactions.splice(index, 1);
  return reaction;
}
