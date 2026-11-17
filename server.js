import express from "express";
import http from "node:http";
import cors from "cors";
import { Server } from "socket.io";
import { randomBytes } from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const CLIENT_ORIGIN = (process.env.CLIENT_ORIGIN || "*").split(",").map(s => s.trim());
const app = express();
app.use(cors({ origin: CLIENT_ORIGIN.includes("*") ? true : CLIENT_ORIGIN }));
app.use(express.json({ limit: "16kb" }));

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: { origin: CLIENT_ORIGIN.includes("*") ? "*" : CLIENT_ORIGIN, methods: ["GET", "POST"] },
  pingInterval: 10000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e5
});

const queues = { pvp: [], coop: [] };
const rooms = new Map();        // roomId -> { id, code, mode, players:Set, createdAt }
const socketRoom = new Map();   // socketId -> roomId
const socketInfo = new Map();   // socketId -> { name, lastInputAt }
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

function makeCode() {
  let code;
  do {
    code = Array.from(randomBytes(6), b => CODE_CHARS[b % CODE_CHARS.length]).join("");
  } while ([...rooms.values()].some(r => r.code === code));
  return code;
}
function cleanName(value) {
  return String(value || "PLAYER").replace(/[^\p{L}\p{N}_-]/gu, "").slice(0, 8) || "PLAYER";
}
function publicRoom(room) {
  return { roomId: room.id, code: room.code, mode: room.mode, players: room.players.size, maxPlayers: 2 };
}
function leaveQueue(socketId) {
  for (const mode of Object.keys(queues)) queues[mode] = queues[mode].filter(id => id !== socketId);
}
function emitRoom(room, event, payload) {
  for (const id of room.players) io.to(id).emit(event, payload);
}
function removeFromRoom(socket, reason = "left") {
  const roomId = socketRoom.get(socket.id);
  if (!roomId) return;
  const room = rooms.get(roomId);
  socketRoom.delete(socket.id);
  socket.leave(roomId);
  if (!room) return;
  room.players.delete(socket.id);
  emitRoom(room, "room:peer-left", { reason });
  if (room.players.size === 0) rooms.delete(roomId);
  else {
    const remaining = [...room.players][0];
    emitRoom(room, "room:host", { isHost: true, peerId: remaining });
  }
}
function createRoom(mode, firstSocket, secondSocket = null, code = makeCode()) {
  const id = randomBytes(12).toString("hex");
  const room = { id, code, mode, players: new Set([firstSocket.id]), createdAt: Date.now() };
  if (secondSocket) room.players.add(secondSocket.id);
  rooms.set(id, room);
  for (const s of [firstSocket, secondSocket].filter(Boolean)) {
    socketRoom.set(s.id, id);
    s.join(id);
  }
  const players = [...room.players].map(id => ({ id, name: socketInfo.get(id)?.name || "PLAYER" }));
  emitRoom(room, "room:ready", { ...publicRoom(room), players, yourId: firstSocket.id });
  if (secondSocket) {
    secondSocket.emit("room:ready", { ...publicRoom(room), players, yourId: secondSocket.id });
    firstSocket.emit("room:ready", { ...publicRoom(room), players, yourId: firstSocket.id });
  }
  return room;
}
function tryMatch(mode) {
  const q = queues[mode];
  while (q.length) {
    const aId = q.shift();
    const a = io.sockets.sockets.get(aId);
    if (!a || socketRoom.has(aId)) continue;
    while (q.length) {
      const bId = q.shift();
      const b = io.sockets.sockets.get(bId);
      if (!b || socketRoom.has(bId) || bId === aId) continue;
      createRoom(mode, a, b);
      return;
    }
    q.unshift(aId);
    return;
  }
}

app.get("/", (_req, res) => res.json({ service: "VOLTIS online server", status: "ok" }));
app.get("/health", (_req, res) => res.json({ ok: true, rooms: rooms.size, queues: { pvp: queues.pvp.length, coop: queues.coop.length } }));

io.on("connection", socket => {
  socketInfo.set(socket.id, { name: "PLAYER", lastInputAt: 0 });

  socket.on("player:set-name", (raw, ack = () => {}) => {
    const info = socketInfo.get(socket.id);
    if (info) info.name = cleanName(raw);
    ack({ ok: true, name: info?.name || "PLAYER" });
  });

  socket.on("match:join", (payload = {}, ack = () => {}) => {
    const mode = payload.mode;
    if (!["pvp", "coop"].includes(mode)) return ack({ ok: false, error: "지원하지 않는 모드입니다." });
    if (socketRoom.has(socket.id)) return ack({ ok: false, error: "이미 방에 참가 중입니다." });
    leaveQueue(socket.id);
    queues[mode].push(socket.id);
    ack({ ok: true, queued: true, mode, position: queues[mode].length });
    tryMatch(mode);
  });

  socket.on("match:cancel", (ack = () => {}) => {
    leaveQueue(socket.id);
    ack({ ok: true });
  });

  socket.on("room:create", (payload = {}, ack = () => {}) => {
    const mode = payload.mode;
    if (!["pvp", "coop"].includes(mode)) return ack({ ok: false, error: "지원하지 않는 모드입니다." });
    if (socketRoom.has(socket.id)) return ack({ ok: false, error: "이미 방에 참가 중입니다." });
    const room = createRoom(mode, socket);
    ack({ ok: true, ...publicRoom(room) });
  });

  socket.on("room:join", (payload = {}, ack = () => {}) => {
    const code = String(payload.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
    const room = [...rooms.values()].find(r => r.code === code);
    if (!room) return ack({ ok: false, error: "방 코드를 찾을 수 없습니다." });
    if (room.players.size >= 2) return ack({ ok: false, error: "방이 가득 찼습니다." });
    if (socketRoom.has(socket.id)) return ack({ ok: false, error: "이미 방에 참가 중입니다." });
    room.players.add(socket.id);
    socketRoom.set(socket.id, room.id);
    socket.join(room.id);
    const players = [...room.players].map(id => ({ id, name: socketInfo.get(id)?.name || "PLAYER" }));
    for (const id of room.players) io.to(id).emit("room:ready", { ...publicRoom(room), players, yourId: id });
    ack({ ok: true, ...publicRoom(room) });
  });

  socket.on("room:leave", (ack = () => {}) => {
    removeFromRoom(socket, "left");
    ack({ ok: true });
  });

  // Client inputs are relayed only to the other player. The game client must
  // validate/apply these controls to its remote fighter.
  socket.on("game:input", (payload = {}) => {
    const roomId = socketRoom.get(socket.id);
    const room = rooms.get(roomId);
    if (!room || !room.players.has(socket.id)) return;
    const info = socketInfo.get(socket.id);
    const now = Date.now();
    if (info && now - info.lastInputAt < 12) return; // max ~83 packets/sec
    if (info) info.lastInputAt = now;
    const input = {
      seq: Number.isInteger(payload.seq) ? payload.seq : 0,
      frame: Number.isInteger(payload.frame) ? payload.frame : 0,
      x: Math.max(-1, Math.min(1, Number(payload.x) || 0)),
      y: Math.max(-1, Math.min(1, Number(payload.y) || 0)),
      jump: !!payload.jump,
      dash: !!payload.dash,
      parry: !!payload.parry,
      attack: ["basic", "skill_1", "skill_2", "skill_3", "ultimate", "whip"].includes(payload.attack) ? payload.attack : null,
      aimX: Math.max(-1, Math.min(1, Number(payload.aimX) || 0)),
      aimY: Math.max(-1, Math.min(1, Number(payload.aimY) || 0))
    };
    socket.to(roomId).volatile.emit("game:peer-input", { from: socket.id, input });
  });

  socket.on("game:ping", (_payload, ack = () => {}) => ack({ now: Date.now() }));

  socket.on("disconnect", () => {
    leaveQueue(socket.id);
    removeFromRoom(socket, "disconnected");
    socketInfo.delete(socket.id);
  });
});

setInterval(() => {
  const cutoff = Date.now() - 10 * 60 * 1000;
  for (const [id, room] of rooms) if (room.createdAt < cutoff && room.players.size === 0) rooms.delete(id);
}, 60000).unref();

httpServer.listen(PORT, "0.0.0.0", () => console.log(`VOLTIS server listening on ${PORT}`));
