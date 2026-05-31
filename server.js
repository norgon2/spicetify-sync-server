const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.use(cors());
app.use(express.json());

const PORT = 3000;

// rooms: Map<roomCode, { hostId: string|null, guests: Set<string>, cohostMode: boolean }>
const rooms = new Map();
// clients: Map<socketId, { role, username, roomCode }>
const clients = new Map();

const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
function generateRoomCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () =>
      ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)]
    ).join("");
  } while (rooms.has(code));
  return code;
}

app.get("/status", (req, res) => {
  res.json({
    totalClients: clients.size,
    rooms: [...rooms.entries()].map(([code, r]) => ({
      code,
      hosts: r.hostId ? 1 : 0,
      guests: r.guests.size,
      cohostMode: r.cohostMode,
    })),
  });
});

io.on("connection", (socket) => {
  console.log(`[+] Client connected: ${socket.id}`);

  socket.on("register", (data) => {
    if (!data || typeof data !== "object") return;
    const { role, username, roomCode } = data;
    const safeRole = role === "host" ? "host" : "guest";
    const safeUsername = (typeof username === "string" ? username : "Anonymous").slice(0, 32);

    if (safeRole === "host") {
      const requested = (typeof data.requestedCode === "string" ? data.requestedCode : "").toUpperCase().trim();
      let code;
      if (requested && /^[A-Z0-9]{6}$/.test(requested)) {
        if (rooms.has(requested)) {
          socket.emit("error", { message: "Room code already in use." });
          return;
        }
        code = requested;
      } else {
        code = generateRoomCode();
      }
      rooms.set(code, { hostId: socket.id, guests: new Set(), cohostMode: false });
      clients.set(socket.id, { role: "host", username: safeUsername, roomCode: code });
      socket.join(code);
      socket.emit("registered", { role: "host", username: safeUsername });
      socket.emit("room_created", { code });
      console.log(`[~] ${safeUsername} created room ${code}`);
      broadcastRoomUpdate(code);
    } else {
      const safeCode = (typeof roomCode === "string" ? roomCode : "").toUpperCase().trim();
      const room = rooms.get(safeCode);
      if (!room) {
        socket.emit("error", { message: "Room not found. Check the code and try again." });
        return;
      }
      room.guests.add(socket.id);
      clients.set(socket.id, { role: "guest", username: safeUsername, roomCode: safeCode });
      socket.join(safeCode);
      socket.emit("registered", { role: "guest", username: safeUsername });
      socket.emit("cohost_mode_changed", { enabled: room.cohostMode });
      if (!room.hostId) socket.emit("waiting_for_host");
      console.log(`[~] ${safeUsername} joined room ${safeCode} as guest`);
      broadcastRoomUpdate(safeCode);
    }
  });

  socket.on("set_cohost_mode", (data) => {
    const client = clients.get(socket.id);
    if (!client || client.role !== "host") return;
    const room = rooms.get(client.roomCode);
    if (!room) return;
    room.cohostMode = Boolean(data?.enabled);
    console.log(`[co-host] room ${client.roomCode}: ${room.cohostMode ? "enabled" : "disabled"}`);
    io.to(client.roomCode).emit("cohost_mode_changed", { enabled: room.cohostMode });
  });

  socket.on("play", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    if (typeof data.uri !== "string") return;
    socket.to(client.roomCode).emit("play", {
      uri:        data.uri,
      position:   typeof data.position === "number" ? data.position : 0,
      contextUri: typeof data.contextUri === "string" ? data.contextUri : null,
    });
  });

  socket.on("pause", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    socket.to(client.roomCode).emit("pause", {
      position: typeof data.position === "number" ? data.position : 0,
    });
  });

  socket.on("seek", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    if (typeof data.position !== "number") return;
    socket.to(client.roomCode).emit("seek", {
      position: data.position,
      sentAt:   typeof data.sentAt === "number" ? data.sentAt : Date.now(),
    });
  });

  socket.on("change_track", (data) => {
    const client = clients.get(socket.id);
    if (!client || !canControl(socket.id) || !data || typeof data !== "object") return;
    if (typeof data.uri !== "string") return;
    console.log(`[>>|] room ${client.roomCode} change_track: ${data.uri}`);
    socket.to(client.roomCode).emit("change_track", {
      uri:        data.uri,
      position:   typeof data.position === "number" ? data.position : 0,
      contextUri: typeof data.contextUri === "string" ? data.contextUri : null,
    });
  });

  socket.on("request_sync", () => {
    const client = clients.get(socket.id);
    if (!client) return;
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
    if (typeof data.guestId !== "string" || typeof data.uri !== "string") return;
    const target = io.sockets.sockets.get(data.guestId);
    if (target) {
      target.emit("sync_state", {
        uri:        data.uri,
        position:   typeof data.position === "number" ? data.position : 0,
        isPlaying:  Boolean(data.isPlaying),
        contextUri: typeof data.contextUri === "string" ? data.contextUri : null,
        sentAt:     typeof data.sentAt === "number" ? data.sentAt : null,
      });
    }
  });

  socket.on("disconnect", () => {
    const client = clients.get(socket.id);
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
