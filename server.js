const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

// No global HTTP CORS — socket.io handles its own; HTTP routes are localhost-only
app.use(express.json());

const PORT = 3000;
const PROTOCOL_VERSION = 1;
const MAX_POSITION_MS  = 86400000;

// Fix 5: strict Spotify URI pattern, same as client
const SPOTIFY_URI_RE = /^spotify:[a-z]+:[A-Za-z0-9]{22}$/;

// rooms: Map<roomCode, { hostId: string|null, guests: Set<string>, cohostMode: boolean }>
const rooms = new Map();
// clients: Map<socketId, { role, username, roomCode }>
const clients = new Map();

const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

// Fix 11: max attempts to avoid infinite loop
function generateRoomCode() {
  for (let i = 0; i < 50; i++) {
    const code = Array.from({ length: 6 }, () =>
      ROOM_CODE_CHARS[crypto.randomInt(ROOM_CODE_CHARS.length)]
    ).join("");
    if (!rooms.has(code)) return code;
  }
  return null;
}

function finalizeHostRegistration(socket, code, safeUsername) {
  clients.set(socket.id, { role: "host", username: safeUsername, roomCode: code });
  socket.join(code);
  socket.emit("registered", { role: "host", username: safeUsername });
  socket.emit("room_created", { code });
  broadcastRoomUpdate(code);
}

// --- Registration rate limiting (5/min per IP) ---
const registerAttempts = new Map();
const REGISTER_LIMIT  = 5;
const REGISTER_WINDOW = 60_000;

function checkRateLimit(ip) {
  const now   = Date.now();
  const entry = registerAttempts.get(ip);
  if (!entry || now > entry.resetAt) {
    registerAttempts.set(ip, { count: 1, resetAt: now + REGISTER_WINDOW });
    return true;
  }
  if (entry.count >= REGISTER_LIMIT) return false;
  entry.count++;
  return true;
}

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of registerAttempts)
    if (now > entry.resetAt) registerAttempts.delete(ip);
}, REGISTER_WINDOW).unref();

// --- Fix 6: playback event rate limiting (10 events/s per socket) ---
const socketEventCounters = new Map();
const SOCKET_EVENT_RATE   = 10;
const SOCKET_EVENT_WINDOW = 1000;

function checkEventRate(socketId) {
  const now   = Date.now();
  const entry = socketEventCounters.get(socketId);
  if (!entry || now > entry.resetAt) {
    socketEventCounters.set(socketId, { count: 1, resetAt: now + SOCKET_EVENT_WINDOW });
    return true;
  }
  if (entry.count >= SOCKET_EVENT_RATE) return false;
  entry.count++;
  return true;
}

// Fix 8: safe position — must be finite number in [0, MAX_POSITION_MS]
function safePosition(v, fallback = 0) {
  return typeof v === "number" && isFinite(v) && v >= 0 && v <= MAX_POSITION_MS
    ? v
    : fallback;
}

// Fix 14: broadcast playback to room, excluding sender; if sender is guest co-host,
// also exclude the host (host controls their own playback independently).
function broadcastPlayback(socket, client, event, data) {
  const room     = rooms.get(client.roomCode);
  const excluded = [socket.id];
  if (client.role === "guest" && room?.hostId) excluded.push(room.hostId);
  io.to(client.roomCode).except(excluded).emit(event, data);
}

// Fix 7: /status is localhost-only
app.get("/status", (req, res) => {
  const ip = req.socket.remoteAddress;
  if (ip !== "127.0.0.1" && ip !== "::1" && ip !== "::ffff:127.0.0.1") {
    return res.status(403).end();
  }
  let hosts = 0, guests = 0;
  for (const c of clients.values()) c.role === "host" ? hosts++ : guests++;
  res.json({ connected: clients.size, hosts, guests });
});

io.on("connection", (socket) => {
  console.log(`[+] Client connected: ${socket.id}`);

  socket.on("register", (data) => {
    if (clients.has(socket.id)) return;
    if (!data || typeof data !== "object") return;
    if (data.version !== PROTOCOL_VERSION) {
      socket.emit("error", { message: `Version incompatible (got ${data.version}, expected ${PROTOCOL_VERSION}). Update your extension.` });
      return;
    }
    if (!checkRateLimit(socket.handshake.address)) {
      socket.emit("error", { message: "Too many attempts. Try again in a minute." });
      return;
    }
    const { role, username, roomCode } = data;
    const safeRole     = role === "host" ? "host" : "guest";
    const safeUsername = (typeof username === "string" ? username : "Anonymous").slice(0, 32);

    if (safeRole === "host") {
      const requested = (typeof data.requestedCode === "string" ? data.requestedCode : "").toUpperCase().trim();
      let code;
      if (requested && /^[ABCDEFGHJKLMNPQRSTUVWXYZ23456789]{6}$/.test(requested)) {
        if (rooms.has(requested)) {
          const existing = rooms.get(requested);
          if (existing.hostId !== null) {
            socket.emit("error", { message: "Room code already in use." });
            return;
          }
          existing.hostId    = socket.id;
          existing.cohostMode = false;
          finalizeHostRegistration(socket, requested, safeUsername);
          socket.emit("cohost_mode_changed", { enabled: false });
          console.log(`[~] ${safeUsername} reclaimed room ${requested}`);
          if (existing.guests.size > 0) socket.to(requested).emit("host_connected");
          // Fix 15: reset rate limit on successful registration
          registerAttempts.delete(socket.handshake.address);
          return;
        }
        code = requested;
      } else {
        // Fix 11: handle null from generateRoomCode
        code = generateRoomCode();
        if (!code) {
          socket.emit("error", { message: "Could not create room. Try again." });
          return;
        }
      }
      rooms.set(code, { hostId: socket.id, guests: new Set(), cohostMode: false });
      finalizeHostRegistration(socket, code, safeUsername);
      // Fix 15: reset rate limit on successful registration
      registerAttempts.delete(socket.handshake.address);
      console.log(`[~] ${safeUsername} created room ${code}`);
    } else {
      const safeCode = (typeof roomCode === "string" ? roomCode : "").toUpperCase().trim();
      const room = rooms.get(safeCode);
      if (!room) {
        socket.emit("error", { message: "Room not found. Check the code and try again." });
        return;
      }
      if (room.guests.size >= 10) {
        socket.emit("error", { message: "Room is full (max 10 guests)." });
        return;
      }
      room.guests.add(socket.id);
      clients.set(socket.id, { role: "guest", username: safeUsername, roomCode: safeCode });
      socket.join(safeCode);
      socket.emit("registered", { role: "guest", username: safeUsername });
      if (room.cohostMode) socket.emit("cohost_mode_changed", { enabled: true });
      if (!room.hostId) socket.emit("waiting_for_host");
      // Fix 15: reset rate limit on successful registration
      registerAttempts.delete(socket.handshake.address);
      console.log(`[~] ${safeUsername} joined room ${safeCode} as guest`);
      broadcastRoomUpdate(safeCode);
    }
  });

  socket.on("set_cohost_mode", (data) => {
    const client = clients.get(socket.id);
    if (!client || client.role !== "host") return;
    if (!checkEventRate(socket.id)) return;
    const room = rooms.get(client.roomCode);
    if (!room) return;
    room.cohostMode = Boolean(data?.enabled);
    console.log(`[co-host] room ${client.roomCode}: ${room.cohostMode ? "enabled" : "disabled"}`);
    io.to(client.roomCode).emit("cohost_mode_changed", { enabled: room.cohostMode });
  });

  socket.on("play", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    // Fix 5: strict URI validation
    if (typeof data.uri !== "string" || !SPOTIFY_URI_RE.test(data.uri)) return;
    // Fix 6: rate limit
    if (!checkEventRate(socket.id)) return;
    broadcastPlayback(socket, client, "play", {
      uri:        data.uri,
      position:   safePosition(data.position),
      contextUri: typeof data.contextUri === "string" && SPOTIFY_URI_RE.test(data.contextUri) ? data.contextUri : null,
    });
  });

  socket.on("pause", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    // Fix 6: rate limit
    if (!checkEventRate(socket.id)) return;
    // Fix 8: isFinite check via safePosition
    broadcastPlayback(socket, client, "pause", {
      position: safePosition(data.position),
    });
  });

  socket.on("seek", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    // Fix 8: strict finite check on position
    if (typeof data.position !== "number" || !isFinite(data.position)) return;
    // Fix 6: rate limit
    if (!checkEventRate(socket.id)) return;
    broadcastPlayback(socket, client, "seek", {
      position: safePosition(data.position),
      sentAt:   typeof data.sentAt === "number" && isFinite(data.sentAt) ? data.sentAt : Date.now(),
    });
  });

  socket.on("change_track", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    // Fix 5: strict URI
    if (typeof data.uri !== "string" || !SPOTIFY_URI_RE.test(data.uri)) return;
    // Fix 6: rate limit
    if (!checkEventRate(socket.id)) return;
    console.log(`[>>|] room ${client.roomCode} change_track: ${data.uri}`);
    broadcastPlayback(socket, client, "change_track", {
      uri:        data.uri,
      position:   safePosition(data.position),
      contextUri: typeof data.contextUri === "string" && SPOTIFY_URI_RE.test(data.contextUri) ? data.contextUri : null,
    });
  });

  socket.on("request_sync", () => {
    const client = clients.get(socket.id);
    if (!client) return;
    if (!checkEventRate(socket.id)) return;
    const room = rooms.get(client.roomCode);
    if (!room) return;
    if (room.hostId) {
      const hostSocket = io.sockets.sockets.get(room.hostId);
      if (hostSocket) {
        hostSocket.emit("sync_requested", { guestId: socket.id });
        return;
      }
    }
    socket.emit("waiting_for_host");
  });

  socket.on("sync_state", (data) => {
    const client = clients.get(socket.id);
    if (!client || client.role !== "host" || !data || typeof data !== "object") return;
    // Fix 5: strict URI
    if (typeof data.guestId !== "string" || typeof data.uri !== "string" || !SPOTIFY_URI_RE.test(data.uri)) return;
    const targetClient = clients.get(data.guestId);
    if (!targetClient || targetClient.roomCode !== client.roomCode) return;
    const target = io.sockets.sockets.get(data.guestId);
    if (target) {
      target.emit("sync_state", {
        uri:        data.uri,
        position:   safePosition(data.position),
        isPlaying:  Boolean(data.isPlaying),
        contextUri: typeof data.contextUri === "string" && SPOTIFY_URI_RE.test(data.contextUri) ? data.contextUri : null,
        sentAt:     typeof data.sentAt === "number" && isFinite(data.sentAt) ? data.sentAt : null,
      });
    }
  });

  socket.on("sync_ping", (data) => {
    const client = clients.get(socket.id);
    if (!client || client.role !== "host" || !data || typeof data !== "object") return;
    if (typeof data.uri !== "string" || !SPOTIFY_URI_RE.test(data.uri)) return;
    if (!checkEventRate(socket.id)) return;
    broadcastPlayback(socket, client, "sync_ping", {
      uri:       data.uri,
      position:  safePosition(data.position),
      isPlaying: Boolean(data.isPlaying),
      sentAt:    typeof data.sentAt === "number" && isFinite(data.sentAt) ? data.sentAt : Date.now(),
    });
  });

  socket.on("volume_change", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id)) return;
    const { volume } = data ?? {};
    if (typeof volume !== "number" || !isFinite(volume) || volume < 0 || volume > 1) return;
    // Fix 6: rate limit
    if (!checkEventRate(socket.id)) return;
    broadcastPlayback(socket, client, "volume_change", { volume });
  });

  socket.on("disconnect", () => {
    const client = clients.get(socket.id);
    // Fix 6: cleanup rate limit entry
    socketEventCounters.delete(socket.id);
    if (!client) return;
    console.log(`[-] ${client.username} (${client.role}) left room ${client.roomCode}`);
    const room = rooms.get(client.roomCode);
    clients.delete(socket.id);
    if (!room) return;

    if (client.role === "host") {
      room.hostId = null;
      if (room.cohostMode) {
        room.cohostMode = false;
        io.to(client.roomCode).emit("cohost_mode_changed", { enabled: false });
      }
      io.to(client.roomCode).emit("host_left");
      if (room.guests.size === 0) {
        rooms.delete(client.roomCode);
        console.log(`[x] Room ${client.roomCode} closed`);
      } else {
        broadcastRoomUpdate(client.roomCode);
      }
    } else {
      room.guests.delete(socket.id);
      if (!room.hostId && room.guests.size === 0) {
        rooms.delete(client.roomCode);
        console.log(`[x] Room ${client.roomCode} closed`);
      } else {
        broadcastRoomUpdate(client.roomCode);
      }
    }
  });
});

function canControl(socketId) {
  const c = clients.get(socketId);
  if (!c) return false;
  const room = rooms.get(c.roomCode);
  if (!room) return false;
  return c.role === "host" || (c.role === "guest" && room.cohostMode);
}

function broadcastRoomUpdate(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  io.to(roomCode).emit("room_update", {
    connected: (room.hostId ? 1 : 0) + room.guests.size,
    hosts:  room.hostId ? 1 : 0,
    guests: room.guests.size,
  });
}

server.listen(PORT, () => {
  console.log(`Spicetify sync server running on http://localhost:${PORT}`);
  console.log(`Status endpoint: http://localhost:${PORT}/status`);
});
