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

const clients = new Map();
let cohostMode = false; // toggled by host, broadcast to all

app.get("/status", (req, res) => {
  res.json({
    connected: clients.size,
    hosts: getHostCount(),
    guests: getGuestCount(),
    cohostMode,
  });
});

io.on("connection", (socket) => {
  console.log(`[+] Client connected: ${socket.id}`);

  socket.on("register", (data) => {
    if (!data || typeof data !== "object") return;
    const { role, username } = data;
    const safeRole = role === "host" ? "host" : "guest";
    const safeUsername = (typeof username === "string" ? username : "Anonymous").slice(0, 32);

    if (safeRole === "host") {
      const existingHost = [...clients.values()].find((c) => c.role === "host");
      if (existingHost) {
        socket.emit("error", { message: "A host is already connected." });
        return;
      }
    }

    clients.set(socket.id, { role: safeRole, username: safeUsername });
    socket.join(safeRole === "host" ? "hosts" : "guests");
    socket.emit("registered", { role: safeRole, username: safeUsername });

    // Tell new guest the current co-host state
    if (safeRole === "guest") {
      socket.emit("cohost_mode_changed", { enabled: cohostMode });
    }

    if (safeRole === "host") {
      socket.to("guests").emit("host_connected");
    }

    console.log(`[~] ${safeUsername} registered as ${safeRole}`);
    broadcastRoomUpdate();
  });

  // Host toggles co-host mode
  socket.on("set_cohost_mode", (data) => {
    if (!isHost(socket.id)) return;
    cohostMode = Boolean(data?.enabled);
    console.log(`[co-host] mode ${cohostMode ? "enabled" : "disabled"}`);
    io.emit("cohost_mode_changed", { enabled: cohostMode });
  });

  // Playback events: accepted from host always, from guests only in co-host mode.
  // socket.broadcast sends to everyone EXCEPT sender — prevents self-echo.

  socket.on("play", (data) => {
    if (!canControl(socket.id) || !data || typeof data !== "object") return;
    if (typeof data.uri !== "string") return;
    socket.broadcast.emit("play", {
      uri:        data.uri,
      position:   typeof data.position === "number" ? data.position : 0,
      contextUri: typeof data.contextUri === "string" ? data.contextUri : null,
    });
  });

  socket.on("pause", (data) => {
    if (!canControl(socket.id) || !data || typeof data !== "object") return;
    socket.broadcast.emit("pause", {
      position: typeof data.position === "number" ? data.position : 0,
    });
  });

  socket.on("seek", (data) => {
    if (!canControl(socket.id) || !data || typeof data !== "object") return;
    if (typeof data.position !== "number") return;
    socket.broadcast.emit("seek", {
      position: data.position,
      sentAt:   typeof data.sentAt === "number" ? data.sentAt : Date.now(),
    });
  });

  socket.on("change_track", (data) => {
    if (!canControl(socket.id) || !data || typeof data !== "object") return;
    if (typeof data.uri !== "string") return;
    console.log(`[>>|] change_track: ${data.uri}`);
    socket.broadcast.emit("change_track", {
      uri:        data.uri,
      position:   typeof data.position === "number" ? data.position : 0,
      contextUri: typeof data.contextUri === "string" ? data.contextUri : null,
    });
  });

  socket.on("request_sync", () => {
    const host = [...io.sockets.sockets.values()].find(
      (s) => clients.get(s.id)?.role === "host"
    );
    if (host) {
      host.emit("sync_requested", { guestId: socket.id });
    } else {
      socket.emit("waiting_for_host");
    }
  });

  socket.on("sync_state", (data) => {
    if (!isHost(socket.id) || !data || typeof data !== "object") return;
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
    console.log(`[-] ${client.username} (${client.role}) disconnected`);
    const wasHost = client.role === "host";
    clients.delete(socket.id);
    if (wasHost) {
      if (cohostMode) {
        cohostMode = false;
        io.to("guests").emit("cohost_mode_changed", { enabled: false });
      }
      io.to("guests").emit("host_left");
    }
    broadcastRoomUpdate();
  });
});

function isHost(socketId) {
  return clients.get(socketId)?.role === "host";
}

function canControl(socketId) {
  const c = clients.get(socketId);
  if (!c) return false;
  return c.role === "host" || (c.role === "guest" && cohostMode);
}

function getHostCount() {
  return [...clients.values()].filter((c) => c.role === "host").length;
}

function getGuestCount() {
  return [...clients.values()].filter((c) => c.role === "guest").length;
}

function broadcastRoomUpdate() {
  io.emit("room_update", {
    connected: clients.size,
    hosts: getHostCount(),
    guests: getGuestCount(),
  });
}

server.listen(PORT, () => {
  console.log(`Spicetify sync server running on http://localhost:${PORT}`);
  console.log(`Status endpoint: http://localhost:${PORT}/status`);
});
