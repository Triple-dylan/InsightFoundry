import crypto from "node:crypto";
import { newId } from "./state.js";

const TOKEN_SECRET = process.env.REALTIME_TOKEN_SECRET || crypto.randomBytes(24).toString("hex");
const TOKEN_TTL_MS = 30 * 60 * 1000;
const MAX_EVENTS_PER_TENANT = 5000;

function nowIso() {
  return new Date().toISOString();
}

function toBase64Url(value) {
  return Buffer.from(value).toString("base64url");
}

function fromBase64Url(value) {
  return Buffer.from(value, "base64url").toString("utf8");
}

function sign(payload) {
  return crypto
    .createHmac("sha256", TOKEN_SECRET)
    .update(payload)
    .digest("base64url");
}

export function issueRealtimeToken(tenantId, userId, channel = "web") {
  const payload = {
    tenantId: String(tenantId),
    userId: String(userId),
    channel: String(channel || "web"),
    exp: Date.now() + TOKEN_TTL_MS
  };
  const encoded = toBase64Url(JSON.stringify(payload));
  const signature = sign(encoded);
  return `${encoded}.${signature}`;
}

export function verifyRealtimeToken(token) {
  const parts = String(token || "").split(".");
  if (parts.length !== 2) return null;
  const [encoded, signature] = parts;
  if (sign(encoded) !== signature) return null;
  try {
    const payload = JSON.parse(fromBase64Url(encoded));
    if (!payload?.tenantId || !payload?.userId) return null;
    if (Number(payload.exp || 0) < Date.now()) return null;
    return payload;
  } catch {
    return null;
  }
}

function websocketAccept(key) {
  return crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");
}

function encodeFrame(payload, opcode = 0x1) {
  const body = Buffer.from(String(payload));
  const length = body.length;

  if (length < 126) {
    const frame = Buffer.allocUnsafe(2 + length);
    frame[0] = 0x80 | (opcode & 0x0f);
    frame[1] = length;
    body.copy(frame, 2);
    return frame;
  }

  if (length <= 0xffff) {
    const frame = Buffer.allocUnsafe(4 + length);
    frame[0] = 0x80 | (opcode & 0x0f);
    frame[1] = 126;
    frame.writeUInt16BE(length, 2);
    body.copy(frame, 4);
    return frame;
  }

  const frame = Buffer.allocUnsafe(10 + length);
  frame[0] = 0x80 | (opcode & 0x0f);
  frame[1] = 127;
  frame.writeUInt32BE(0, 2);
  frame.writeUInt32BE(length, 6);
  body.copy(frame, 10);
  return frame;
}

function decodeClientFrames(buffer) {
  const frames = [];
  let offset = 0;

  while (offset + 2 <= buffer.length) {
    const byte1 = buffer[offset];
    const byte2 = buffer[offset + 1];
    const opcode = byte1 & 0x0f;
    const masked = (byte2 & 0x80) !== 0;
    let payloadLength = byte2 & 0x7f;
    let headerLength = 2;

    if (payloadLength === 126) {
      if (offset + 4 > buffer.length) break;
      payloadLength = buffer.readUInt16BE(offset + 2);
      headerLength = 4;
    } else if (payloadLength === 127) {
      if (offset + 10 > buffer.length) break;
      const high = buffer.readUInt32BE(offset + 2);
      const low = buffer.readUInt32BE(offset + 6);
      if (high !== 0) break;
      payloadLength = low;
      headerLength = 10;
    }

    const maskLength = masked ? 4 : 0;
    const frameLength = headerLength + maskLength + payloadLength;
    if (offset + frameLength > buffer.length) break;

    let payload = buffer.subarray(offset + headerLength + maskLength, offset + frameLength);
    if (masked) {
      const mask = buffer.subarray(offset + headerLength, offset + headerLength + 4);
      const unmasked = Buffer.allocUnsafe(payload.length);
      for (let i = 0; i < payload.length; i += 1) {
        unmasked[i] = payload[i] ^ mask[i % 4];
      }
      payload = unmasked;
    }

    frames.push({ opcode, payload });
    offset += frameLength;
  }

  return frames;
}

export function createRealtimeHub() {
  return {
    clients: new Map()
  };
}

function unregisterClient(hub, clientId) {
  hub.clients.delete(clientId);
}

export function listRealtimeEvents(state, tenantId, since) {
  const sinceTs = since ? Date.parse(String(since)) : Number.NaN;
  return state.realtimeEvents
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (Number.isFinite(sinceTs) ? Date.parse(item.createdAt) > sinceTs : true))
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function publishRealtimeEvent(state, hub, payload = {}) {
  if (!payload.tenantId || !payload.type) return null;
  const event = {
    id: newId("realtime_event"),
    tenantId: String(payload.tenantId),
    type: String(payload.type),
    threadId: payload.threadId ? String(payload.threadId) : undefined,
    audienceUserIds: Array.isArray(payload.audienceUserIds) ? payload.audienceUserIds.map((id) => String(id)) : null,
    payload: payload.payload && typeof payload.payload === "object" ? payload.payload : {},
    createdAt: nowIso()
  };

  state.realtimeEvents.push(event);
  const tenantEvents = state.realtimeEvents.filter((item) => item.tenantId === event.tenantId);
  if (tenantEvents.length > MAX_EVENTS_PER_TENANT) {
    const overflow = tenantEvents.length - MAX_EVENTS_PER_TENANT;
    let removed = 0;
    state.realtimeEvents = state.realtimeEvents.filter((item) => {
      if (item.tenantId !== event.tenantId) return true;
      if (removed < overflow) {
        removed += 1;
        return false;
      }
      return true;
    });
  }

  const serialized = JSON.stringify({ event });
  for (const client of hub.clients.values()) {
    if (client.tenantId !== event.tenantId) continue;
    if (event.audienceUserIds?.length && !event.audienceUserIds.includes(client.userId)) continue;
    try {
      client.socket.write(encodeFrame(serialized, 0x1));
    } catch {
      unregisterClient(hub, client.id);
    }
  }

  return event;
}

export function handleRealtimeUpgrade(req, socket, head, hub) {
  const base = new URL(req.url ?? "/", "http://localhost");
  if (base.pathname !== "/v1/realtime/ws") return false;

  const token = base.searchParams.get("token") || "";
  const payload = verifyRealtimeToken(token);
  if (!payload) {
    socket.write("HTTP/1.1 401 Unauthorized\\r\\n\\r\\n");
    socket.destroy();
    return true;
  }

  const key = req.headers["sec-websocket-key"];
  const version = req.headers["sec-websocket-version"];
  if (!key || version !== "13") {
    socket.write("HTTP/1.1 400 Bad Request\\r\\n\\r\\n");
    socket.destroy();
    return true;
  }

  const acceptKey = websocketAccept(String(key));
  socket.write(
    [
      "HTTP/1.1 101 Switching Protocols",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Accept: ${acceptKey}`,
      "\\r\\n"
    ].join("\\r\\n")
  );

  if (head && head.length) {
    // ignore any buffered head bytes for now
  }

  const client = {
    id: newId("rt_client"),
    tenantId: payload.tenantId,
    userId: payload.userId,
    channel: payload.channel,
    socket,
    connectedAt: nowIso()
  };

  hub.clients.set(client.id, client);

  const cleanup = () => unregisterClient(hub, client.id);
  socket.on("close", cleanup);
  socket.on("error", cleanup);

  socket.on("data", (chunk) => {
    const frames = decodeClientFrames(Buffer.from(chunk));
    for (const frame of frames) {
      if (frame.opcode === 0x8) {
        try { socket.write(encodeFrame("", 0x8)); } catch {}
        socket.end();
        return;
      }
      if (frame.opcode === 0x9) {
        try { socket.write(encodeFrame(frame.payload, 0xA)); } catch {}
      }
    }
  });

  try {
    socket.write(encodeFrame(JSON.stringify({ event: {
      id: newId("realtime_event"),
      tenantId: payload.tenantId,
      type: "realtime.connected",
      payload: { userId: payload.userId, channel: payload.channel },
      createdAt: nowIso()
    } }), 0x1));
  } catch {
    cleanup();
    socket.destroy();
  }

  return true;
}
