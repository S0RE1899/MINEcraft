/**
 * Proximity Voice Chat Server
 * Deploy this to Render.com (free) via GitHub — same way as the TurboWarp server.
 *
 * SETUP:
 *   1. Create a new GitHub repo and push server.js + package.json into it
 *   2. Go to render.com → New → Web Service → connect that repo
 *   3. Settings: Runtime = Node, Build command = (leave blank), Start command = node server.js
 *   4. Click Deploy. Once live, copy your URL (e.g. https://my-voice-server.onrender.com)
 *   5. In behavior_pack/scripts/main.js set SERVER_HOST = "my-voice-server.onrender.com"
 *   6. Install the behavior pack, enable WebSocket Connections in world settings
 *   7. Each player opens https://my-voice-server.onrender.com in their browser
 *
 * NOTE: Render free tier sleeps after 15 min of inactivity.
 *       The first connection may take ~30 seconds to wake up.
 *
 * WebSocket paths:
 *   wss://host/mc   ← Minecraft connects here via /wsserver
 *   wss://host      ← Browser clients connect here (WebRTC signaling)
 */

"use strict";

const http = require("http");
const { WebSocketServer } = require("ws");
const { randomUUID }      = require("crypto");

const PORT = process.env.PORT || 3000;

// ── Shared state ──────────────────────────────────────────────────────────

/** playerPositions: playerName → { x, y, z } */
const playerPositions = {};

/** WebRTC rooms: roomName → Map<clientId, WebSocket> */
const rooms = {};

/** Browser client id → Minecraft username */
const clientUsername = {};

// ── Inlined browser client HTML ───────────────────────────────────────────

const CLIENT_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MC Proximity Voice Chat</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: "Segoe UI", system-ui, sans-serif;
      background: #1a1a2e; color: #e0e0e0;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center; padding: 16px;
    }
    .card {
      background: #16213e; border: 1px solid #0f3460;
      border-radius: 12px; padding: 28px 32px;
      width: 100%; max-width: 480px;
    }
    h1 { font-size: 1.4rem; color: #4ade80; margin-bottom: 6px; }
    .subtitle { font-size: 0.82rem; color: #94a3b8; margin-bottom: 24px; }
    label { display: block; font-size: 0.82rem; color: #94a3b8; margin-bottom: 4px; }
    input[type="text"], input[type="number"] {
      width: 100%; background: #0f3460; border: 1px solid #1e4d8c;
      border-radius: 6px; color: #e0e0e0; padding: 8px 10px;
      font-size: 0.9rem; margin-bottom: 14px; outline: none;
    }
    input:focus { border-color: #4ade80; }
    button {
      width: 100%; padding: 10px; border: none; border-radius: 6px;
      font-size: 0.95rem; font-weight: 600; cursor: pointer; transition: opacity 0.15s;
    }
    button:hover { opacity: 0.85; }
    #joinBtn  { background: #4ade80; color: #0f2018; margin-top: 6px; }
    #muteBtn  { background: #f59e0b; color: #1a0e00; flex: 1; }
    #leaveBtn { background: #ef4444; color: #fff; flex: 1; }
    .btn-row  { display: flex; gap: 10px; margin-top: 6px; }
    #status   { font-size: 0.82rem; color: #94a3b8; margin: 14px 0 10px; min-height: 18px; }
    #peerList { display: flex; flex-direction: column; gap: 8px; max-height: 300px; overflow-y: auto; }
    .peer-item {
      background: #0f3460; border-radius: 8px; padding: 10px 14px;
      display: flex; align-items: center; justify-content: space-between; gap: 10px;
    }
    .peer-name { font-weight: 600; font-size: 0.9rem; }
    .peer-meta { font-size: 0.78rem; color: #94a3b8; text-align: right; }
    .vol-bar-bg { width: 80px; height: 6px; background: #1e4d8c; border-radius: 3px; overflow: hidden; }
    .vol-bar    { height: 100%; background: #4ade80; border-radius: 3px; transition: width 0.2s; }
    .dot        { width: 8px; height: 8px; border-radius: 50%; background: #4ade80; flex-shrink: 0; }
    .dot.silent { background: #475569; }
    #setup { display: block; } #chat { display: none; }
  </style>
</head>
<body>
<div class="card">
  <div id="setup">
    <h1>&#127897; MC Proximity Voice</h1>
    <p class="subtitle">Enter your Minecraft username and click Join to start talking.</p>
    <label>Your Minecraft username (exact, case-sensitive)</label>
    <input type="text" id="username" placeholder="Steve" autocomplete="off">
    <label>Voice server URL (do not change if using Glitch)</label>
    <input type="text" id="serverUrl" autocomplete="off">
    <label>Room name (same for all players)</label>
    <input type="text" id="roomName" value="minecraft" autocomplete="off">
    <label>Max hearing distance (blocks)</label>
    <input type="number" id="maxDist" value="50" min="1" max="500">
    <button id="joinBtn">Join Voice Chat</button>
  </div>
  <div id="chat">
    <h1>&#127897; MC Proximity Voice</h1>
    <p class="subtitle">Connected as <strong id="myNameDisplay"></strong></p>
    <div id="status">Waiting for players&hellip;</div>
    <div id="peerList"></div>
    <div class="btn-row">
      <button id="muteBtn">Mute Mic</button>
      <button id="leaveBtn">Leave</button>
    </div>
  </div>
</div>
<script>
"use strict";
const STUN_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
  { urls: "stun:stun2.l.google.com:19302" },
];
let ws = null, myId = null, myUsername = "", roomName = "", maxDistance = 50;
let localStream = null, micEnabled = true, audioCtx = null;
const peerConns = {}, peerGains = {}, peerNames = {}, peerVolumes = {};
let latestPositions = {};
const setupEl = document.getElementById("setup");
const chatEl  = document.getElementById("chat");
const statusEl      = document.getElementById("status");
const peerListEl    = document.getElementById("peerList");
const myNameDisplay = document.getElementById("myNameDisplay");
const muteBtn  = document.getElementById("muteBtn");
const leaveBtn = document.getElementById("leaveBtn");
const joinBtn  = document.getElementById("joinBtn");
// Auto-fill server URL from current page
document.getElementById("serverUrl").value =
  (location.protocol === "https:" ? "wss://" : "ws://") + location.host;
function generateId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}
function wsSend(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}
function setStatus(t) { statusEl.textContent = t; }
function escapeHtml(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}
function renderPeers() {
  const ids = Object.keys(peerConns);
  if (ids.length === 0) { peerListEl.innerHTML = ""; setStatus("No other players connected."); return; }
  setStatus(ids.length + " player" + (ids.length === 1 ? "" : "s") + " in voice range:");
  peerListEl.innerHTML = "";
  ids.forEach(function(id) {
    var vol  = peerVolumes[id] != null ? peerVolumes[id] : 1;
    var name = peerNames[id] || id;
    var pct  = Math.round(vol * 100);
    var myPos   = latestPositions[myUsername];
    var peerPos = latestPositions[name];
    var distText = "position unknown";
    if (myPos && peerPos) {
      var dx = myPos.x - peerPos.x, dy = myPos.y - peerPos.y, dz = myPos.z - peerPos.z;
      distText = Math.round(Math.sqrt(dx*dx + dy*dy + dz*dz)) + " blocks away";
    }
    var div = document.createElement("div");
    div.className = "peer-item"; div.id = "peer-" + id;
    div.innerHTML =
      "<span class=\"dot" + (vol < 0.05 ? " silent" : "") + "\"></span>" +
      "<span class=\"peer-name\">" + escapeHtml(name) + "</span>" +
      "<div class=\"peer-meta\"><div>" + escapeHtml(distText) + "</div>" +
      "<div class=\"vol-bar-bg\"><div class=\"vol-bar\" style=\"width:" + pct + "%\"></div></div></div>";
    peerListEl.appendChild(div);
  });
}
function updatePeerVolBar(peerId, vol) {
  peerVolumes[peerId] = vol;
  var row = document.getElementById("peer-" + peerId);
  if (!row) return;
  var bar = row.querySelector(".vol-bar"), dot = row.querySelector(".dot");
  if (bar) bar.style.width = Math.round(vol * 100) + "%";
  if (dot) dot.className = "dot" + (vol < 0.05 ? " silent" : "");
}
async function acquireMic() {
  if (localStream) return localStream;
  try {
    localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
    localStream.getAudioTracks().forEach(function(t) { t.enabled = micEnabled; });
  } catch(e) { setStatus("Microphone access denied."); localStream = null; }
  return localStream;
}
function getOrCreateAudioCtx() {
  if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  return audioCtx;
}
function attachAudio(peerId, stream) {
  var ctx = getOrCreateAudioCtx();
  var source = ctx.createMediaStreamSource(stream);
  var gain = ctx.createGain();
  gain.gain.value = peerVolumes[peerId] != null ? peerVolumes[peerId] : 1;
  source.connect(gain); gain.connect(ctx.destination);
  peerGains[peerId] = gain;
}
function applyProximityVolumes(positions) {
  latestPositions = positions;
  var myPos = positions[myUsername];
  Object.keys(peerConns).forEach(function(peerId) {
    var peerName = peerNames[peerId];
    var peerPos  = peerName ? positions[peerName] : null;
    var vol = 1;
    if (myPos && peerPos) {
      var dx = myPos.x - peerPos.x, dy = myPos.y - peerPos.y, dz = myPos.z - peerPos.z;
      vol = Math.max(0, 1 - Math.sqrt(dx*dx + dy*dy + dz*dz) / maxDistance);
    }
    var gain = peerGains[peerId];
    if (gain) gain.gain.linearRampToValueAtTime(vol, audioCtx.currentTime + 0.15);
    updatePeerVolBar(peerId, vol);
  });
  renderPeers();
}
function createPeerConnection(peerId) {
  if (peerConns[peerId]) return peerConns[peerId];
  var pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });
  pc.onicecandidate = function(e) {
    if (e.candidate) wsSend({ type: "ice", to: peerId, candidate: e.candidate });
  };
  pc.ontrack = function(e) { attachAudio(peerId, e.streams[0]); renderPeers(); };
  pc.onconnectionstatechange = function() {
    var s = pc.connectionState;
    if (s === "failed" || s === "closed" || s === "disconnected") { cleanupPeer(peerId); renderPeers(); }
  };
  if (localStream) localStream.getTracks().forEach(function(t) { pc.addTrack(t, localStream); });
  peerConns[peerId] = pc;
  return pc;
}
function cleanupPeer(peerId) {
  if (peerConns[peerId]) { peerConns[peerId].close(); delete peerConns[peerId]; }
  var gain = peerGains[peerId];
  if (gain) { gain.disconnect(); delete peerGains[peerId]; }
  delete peerNames[peerId]; delete peerVolumes[peerId];
}
function cleanupAllPeers() { Object.keys(peerConns).forEach(cleanupPeer); }
async function handleSignal(msg) {
  try {
    switch (msg.type) {
      case "peers": {
        await acquireMic();
        if (msg.peerInfo) msg.peerInfo.forEach(function(p) { if (p.username) peerNames[p.id] = p.username; });
        for (var i = 0; i < (msg.ids || []).length; i++) {
          var peerId = msg.ids[i];
          var pc = createPeerConnection(peerId);
          var offer = await pc.createOffer();
          await pc.setLocalDescription(offer);
          wsSend({ type: "offer", to: peerId, offer: pc.localDescription });
        }
        renderPeers(); break;
      }
      case "joined": {
        peerNames[msg.id] = msg.username || msg.id;
        await acquireMic(); renderPeers(); break;
      }
      case "left": { cleanupPeer(msg.id); renderPeers(); break; }
      case "offer": {
        await acquireMic();
        var pc2 = createPeerConnection(msg.from);
        await pc2.setRemoteDescription(new RTCSessionDescription(msg.offer));
        var answer = await pc2.createAnswer();
        await pc2.setLocalDescription(answer);
        wsSend({ type: "answer", to: msg.from, answer: pc2.localDescription }); break;
      }
      case "answer": {
        var pc3 = peerConns[msg.from];
        if (pc3 && pc3.signalingState !== "stable")
          await pc3.setRemoteDescription(new RTCSessionDescription(msg.answer));
        break;
      }
      case "ice": {
        var pc4 = peerConns[msg.from];
        if (pc4) await pc4.addIceCandidate(new RTCIceCandidate(msg.candidate));
        break;
      }
      case "positions": { if (msg.players) applyProximityVolumes(msg.players); break; }
    }
  } catch(e) { console.error("Signal error:", e); }
}
joinBtn.addEventListener("click", async function() {
  var username  = document.getElementById("username").value.trim();
  var serverUrl = document.getElementById("serverUrl").value.trim();
  roomName    = document.getElementById("roomName").value.trim() || "minecraft";
  maxDistance = Number(document.getElementById("maxDist").value) || 50;
  if (!username)  { alert("Enter your Minecraft username first."); return; }
  if (!serverUrl) { alert("Enter the voice server URL."); return; }
  myUsername = username; myId = generateId();
  getOrCreateAudioCtx();
  joinBtn.disabled = true; joinBtn.textContent = "Connecting\u2026";
  try {
    ws = new WebSocket(serverUrl);
    ws.onmessage = function(e) {
      try { handleSignal(JSON.parse(e.data)); } catch(err) { console.error(err); }
    };
    ws.onclose = function() { ws = null; cleanupAllPeers(); showSetup(); setStatus("Disconnected."); };
    ws.onerror = function() { setStatus("WebSocket connection error."); };
    await new Promise(function(resolve, reject) {
      var t = setTimeout(function() { reject(new Error("timeout")); }, 6000);
      ws.addEventListener("open",  function() { clearTimeout(t); resolve(); }, { once: true });
      ws.addEventListener("error", function() { clearTimeout(t); reject(new Error("error")); }, { once: true });
    });
    wsSend({ type: "join", room: roomName, id: myId, username: myUsername });
    showChat(); myNameDisplay.textContent = myUsername;
    setStatus("Connected \u2014 waiting for other players\u2026");
  } catch(e) {
    setStatus("Could not connect to voice server.");
    joinBtn.disabled = false; joinBtn.textContent = "Join Voice Chat";
    if (ws) { ws.close(); ws = null; }
  }
});
function leave() {
  if (ws) { wsSend({ type: "leave" }); ws.close(); ws = null; }
  cleanupAllPeers();
  if (localStream) { localStream.getTracks().forEach(function(t) { t.stop(); }); localStream = null; }
  showSetup();
}
leaveBtn.addEventListener("click", leave);
muteBtn.addEventListener("click", function() {
  micEnabled = !micEnabled;
  if (localStream) localStream.getAudioTracks().forEach(function(t) { t.enabled = micEnabled; });
  muteBtn.textContent = micEnabled ? "Mute Mic" : "Unmute Mic";
  muteBtn.style.background = micEnabled ? "#f59e0b" : "#64748b";
});
function showChat()  { setupEl.style.display = "none"; chatEl.style.display = "block"; }
function showSetup() {
  chatEl.style.display = "none"; setupEl.style.display = "block";
  joinBtn.disabled = false; joinBtn.textContent = "Join Voice Chat";
  muteBtn.textContent = "Mute Mic"; muteBtn.style.background = "#f59e0b";
  micEnabled = true; peerListEl.innerHTML = "";
}
<\/script>
</body>
</html>`;

// ── HTTP server ───────────────────────────────────────────────────────────

const httpServer = http.createServer((req, res) => {
  if (req.method === "GET" && (req.url === "/" || req.url === "/index.html")) {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(CLIENT_HTML);
  } else {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Proximity Voice Chat Server running.\n");
  }
});

// ── WebSocket servers ─────────────────────────────────────────────────────

const browserWss = new WebSocketServer({ noServer: true });
const mcWss      = new WebSocketServer({ noServer: true });

// Route upgrade requests by path
httpServer.on("upgrade", (req, socket, head) => {
  if (req.url === "/mc") {
    mcWss.handleUpgrade(req, socket, head, (ws) => {
      mcWss.emit("connection", ws, req);
    });
  } else {
    browserWss.handleUpgrade(req, socket, head, (ws) => {
      browserWss.emit("connection", ws, req);
    });
  }
});

// ── Minecraft WebSocket handler ───────────────────────────────────────────
// Minecraft connects here when the player runs /wsserver ws://host:PORT/mc

mcWss.on("connection", (ws, req) => {
  const remoteAddr = req.socket.remoteAddress;
  console.log(`[MC] Minecraft connected from ${remoteAddr}`);

  /** Subscribe to a Minecraft WebSocket event by name */
  function subscribe(eventName) {
    ws.send(JSON.stringify({
      header: {
        version:        1,
        requestId:      randomUUID(),
        messageType:    "commandRequest",
        messagePurpose: "subscribe",
      },
      body: { eventName },
    }));
  }

  // Request the events we need from Minecraft
  subscribe("PlayerTransform"); // fires every tick with player position/rotation
  subscribe("PlayerJoin");
  subscribe("PlayerLeft");

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const purpose   = msg?.header?.messagePurpose;
    const eventName = msg?.header?.eventName;

    if (purpose !== "event") return;

    if (eventName === "PlayerTransform") {
      // body.player = { id, name, position: {x,y,z}, yRot, ... }
      const p = msg?.body?.player;
      if (p?.name && p?.position) {
        playerPositions[p.name] = {
          x: p.position.x,
          y: p.position.y,
          z: p.position.z,
        };
      }

    } else if (eventName === "PlayerLeft") {
      const name = msg?.body?.player?.name;
      if (name) {
        console.log(`[MC] Player left: ${name}`);
        delete playerPositions[name];
      }

    } else if (eventName === "PlayerJoin") {
      const name = msg?.body?.player?.name;
      if (name) {
        console.log(`[MC] Player joined: ${name}`);
      }
    }
  });

  ws.on("close", () => {
    console.log("[MC] Minecraft disconnected — clearing all player positions");
    Object.keys(playerPositions).forEach((k) => delete playerPositions[k]);
  });

  ws.on("error", (err) => {
    console.error("[MC] WebSocket error:", err.message);
  });
});

// ── Position broadcast ────────────────────────────────────────────────────
// Send current player positions to every connected browser client every 250 ms.

setInterval(() => {
  if (browserWss.clients.size === 0) return;
  const msg = JSON.stringify({ type: "positions", players: playerPositions });
  browserWss.clients.forEach((ws) => {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  });
}, 250);

// ── Browser WebRTC signaling handler ─────────────────────────────────────

browserWss.on("connection", (ws, req) => {
  let myRoom = null;
  let myId   = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── join: client enters a room ──────────────────────────────────────
      case "join": {
        myRoom = msg.room  || "minecraft";
        myId   = msg.id    || randomUUID();

        if (msg.username) clientUsername[myId] = msg.username;

        if (!rooms[myRoom]) rooms[myRoom] = new Map();

        const existing = [...rooms[myRoom].keys()];
        rooms[myRoom].set(myId, ws);

        console.log(`[WS] ${msg.username || myId} joined room "${myRoom}" (${rooms[myRoom].size} total)`);

        // Tell new client who's already here (include their usernames)
        const peerInfo = existing.map((id) => ({
          id,
          username: clientUsername[id] || id,
        }));
        ws.send(JSON.stringify({ type: "peers", ids: existing, peerInfo }));

        // Notify existing peers of the newcomer
        existing.forEach((id) => {
          const peer = rooms[myRoom]?.get(id);
          if (peer?.readyState === peer?.OPEN) {
            peer.send(JSON.stringify({
              type:     "joined",
              id:       myId,
              username: msg.username || myId,
            }));
          }
        });
        break;
      }

      // ── leave: client explicitly leaves ────────────────────────────────
      case "leave":
        cleanup();
        break;

      // ── WebRTC signaling: relay offer / answer / ice to target peer ────
      case "offer":
      case "answer":
      case "ice": {
        if (!myRoom) break;
        const target = rooms[myRoom]?.get(msg.to);
        if (target?.readyState === target?.OPEN) {
          target.send(JSON.stringify({ ...msg, from: myId }));
        }
        break;
      }
    }
  });

  function cleanup() {
    if (!myRoom || !myId) return;

    const room = rooms[myRoom];
    room?.delete(myId);
    delete clientUsername[myId];

    const leftMsg = JSON.stringify({ type: "left", id: myId });
    room?.forEach((peer) => {
      if (peer?.readyState === peer?.OPEN) peer.send(leftMsg);
    });

    if (room?.size === 0) delete rooms[myRoom];

    console.log(`[WS] ${myId} left room "${myRoom}"`);
    myRoom = null;
    myId   = null;
  }

  ws.on("close", cleanup);

  ws.on("error", (err) => {
    console.error("[WS] Browser client error:", err.message);
  });
});

// ── Start ─────────────────────────────────────────────────────────────────

httpServer.listen(PORT, () => {
  console.log("\n=====================================================");
  console.log("        Proximity Voice Chat Server");
  console.log("=====================================================");
  console.log("  Listening on port " + PORT + "\n");
  console.log("  If on Render, your URLs are:");
  console.log("  Browser page:  https://<service>.onrender.com");
  console.log("  MC endpoint:   wss://<service>.onrender.com/mc\n");
  console.log("  If running locally:");
  console.log("  Browser page:  http://localhost:" + PORT);
  console.log("  MC endpoint:   ws://YOUR_LAN_IP:" + PORT + "/mc\n");
});
