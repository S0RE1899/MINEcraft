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
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MC Proximity Voice</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Segoe UI",sans-serif;background:#1a1a2e;color:#e0e0e0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}
.card{background:#16213e;border:1px solid #0f3460;border-radius:12px;padding:28px 32px;width:100%;max-width:440px}
h1{font-size:1.3rem;color:#4ade80;margin-bottom:6px}
.sub{font-size:.82rem;color:#94a3b8;margin-bottom:20px}
label{display:block;font-size:.8rem;color:#94a3b8;margin-bottom:4px}
input{width:100%;background:#0f3460;border:1px solid #1e4d8c;border-radius:6px;color:#e0e0e0;padding:8px 10px;font-size:.9rem;margin-bottom:14px;outline:none}
input:focus{border-color:#4ade80}
button{width:100%;padding:10px;border:none;border-radius:6px;font-size:.95rem;font-weight:600;cursor:pointer}
#connectBtn{background:#4ade80;color:#0f2018;margin-top:4px}
#muteBtn{background:#f59e0b;color:#1a0e00;flex:1}
#leaveBtn{background:#ef4444;color:#fff;flex:1}
.row{display:flex;gap:10px;margin-top:8px}
#st{font-size:.82rem;color:#94a3b8;margin:12px 0 8px;min-height:16px}
#peers{display:flex;flex-direction:column;gap:8px;max-height:280px;overflow-y:auto}
.peer{background:#0f3460;border-radius:8px;padding:10px 14px;display:flex;align-items:center;gap:10px}
.dot{width:8px;height:8px;border-radius:50%;background:#4ade80;flex-shrink:0}
.dot.off{background:#475569}
.pname{font-weight:600;font-size:.9rem;flex:1}
.pdist{font-size:.78rem;color:#94a3b8}
.vbg{width:70px;height:5px;background:#1e4d8c;border-radius:3px;overflow:hidden}
.vbar{height:100%;background:#4ade80;border-radius:3px;transition:width .2s}
#setup{display:block}#session{display:none}
.forget{font-size:.72rem;color:#4a5568;text-align:center;margin-top:10px;cursor:pointer}
</style>
</head>
<body>
<div class="card">
  <div id="setup">
    <h1>&#127897; MC Proximity Voice</h1>
    <p class="sub">Enter your exact Minecraft username. Saved for next time.</p>
    <label>Minecraft username (exact, case-sensitive)</label>
    <input id="uname" placeholder="Steve" autocomplete="off">
    <button id="connectBtn">Connect</button>
    <p class="forget" id="forget" style="display:none">change username</p>
  </div>
  <div id="session">
    <h1>&#127897; MC Proximity Voice</h1>
    <p class="sub">Connected as <strong id="whoami"></strong></p>
    <div id="st">Waiting for other players...</div>
    <div id="peers"></div>
    <div class="row">
      <button id="muteBtn">Mute</button>
      <button id="leaveBtn">Leave</button>
    </div>
  </div>
</div>
<script>
var STUN=[{urls:"stun:stun.l.google.com:19302"},{urls:"stun:stun1.l.google.com:19302"}];
var WS_URL=(location.protocol==="https:"?"wss:":"ws:")+"//"+location.host;
var params=new URLSearchParams(location.search);
var ROOM=params.get("room")||"minecraft";
var MAXD=parseInt(params.get("dist"))||50;
var ws=null,myId=null,myName="",mic=null,muted=false,actx=null;
var conns={},gains={},names={},vols={},pos={};
function gel(id){return document.getElementById(id);}
var saved=localStorage.getItem("vc_name")||"";
if(saved){gel("uname").value=saved;gel("forget").style.display="block";}
if(saved){window.addEventListener("load",function(){go(saved);});}
gel("connectBtn").addEventListener("click",function(){
  var n=gel("uname").value.trim();
  if(!n){alert("Enter your Minecraft username");return;}
  localStorage.setItem("vc_name",n);
  go(n);
});
gel("uname").addEventListener("keydown",function(e){if(e.key==="Enter")gel("connectBtn").click();});
gel("forget").addEventListener("click",function(){localStorage.removeItem("vc_name");location.reload();});
gel("leaveBtn").addEventListener("click",leave);
gel("muteBtn").addEventListener("click",function(){
  muted=!muted;
  if(mic)mic.getAudioTracks().forEach(function(t){t.enabled=!muted;});
  gel("muteBtn").textContent=muted?"Unmute":"Mute";
  gel("muteBtn").style.background=muted?"#64748b":"#f59e0b";
});
function uid(){return Date.now().toString(36)+Math.random().toString(36).slice(2,6);}
function send(o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o));}
function setst(t){gel("st").textContent=t;}
function go(name){
  myName=name;myId=uid();
  actx=new(window.AudioContext||window.webkitAudioContext)();
  gel("connectBtn").disabled=true;gel("connectBtn").textContent="Connecting...";
  ws=new WebSocket(WS_URL);
  ws.onmessage=function(e){try{handle(JSON.parse(e.data));}catch(x){console.error(x);}};
  ws.onclose=function(){ws=null;cleanup();show("setup");setst("Disconnected.");};
  ws.onerror=function(){setst("Error - server may be waking up, wait 30s and refresh.");};
  ws.onopen=function(){
    send({type:"join",room:ROOM,id:myId,username:myName});
    gel("whoami").textContent=myName;
    show("session");
    setst("Connected - room: "+ROOM+" - max dist: "+MAXD+" blocks");
  };
}
function leave(){
  if(ws){send({type:"leave"});ws.close();ws=null;}
  cleanup();
  if(mic){mic.getTracks().forEach(function(t){t.stop();});mic=null;}
  show("setup");
}
function show(id){
  gel("setup").style.display=id==="setup"?"block":"none";
  gel("session").style.display=id==="session"?"block":"none";
  if(id==="setup"){gel("connectBtn").disabled=false;gel("connectBtn").textContent="Connect";}
}
async function getMic(){
  if(mic)return mic;
  try{mic=await navigator.mediaDevices.getUserMedia({audio:true,video:false});}
  catch(e){setst("Mic denied - allow microphone in browser settings");mic=null;}
  return mic;
}
function addAudio(pid,stream){
  var src=actx.createMediaStreamSource(stream);
  var g=actx.createGain();
  g.gain.value=vols[pid]!=null?vols[pid]:1;
  src.connect(g);g.connect(actx.destination);gains[pid]=g;
}
function calcVol(positions){
  pos=positions;
  var me=positions[myName];
  Object.keys(conns).forEach(function(pid){
    var pn=names[pid],pp=pn?positions[pn]:null,v=1;
    if(me&&pp){var dx=me.x-pp.x,dy=me.y-pp.y,dz=me.z-pp.z;v=Math.max(0,1-Math.sqrt(dx*dx+dy*dy+dz*dz)/MAXD);}
    vols[pid]=v;
    if(gains[pid]&&actx)gains[pid].gain.linearRampToValueAtTime(v,actx.currentTime+0.15);
  });
  renderPeers();
}
function renderPeers(){
  var ids=Object.keys(conns);
  if(!ids.length){gel("peers").innerHTML="";setst("No other players in range.");return;}
  setst(ids.length+" player"+(ids.length===1?"":"s")+" nearby:");
  gel("peers").innerHTML="";
  ids.forEach(function(pid){
    var v=vols[pid]!=null?vols[pid]:1,n=names[pid]||pid;
    var me=pos[myName],pp=pos[n],dt="position unknown";
    if(me&&pp){var dx=me.x-pp.x,dy=me.y-pp.y,dz=me.z-pp.z;dt=Math.round(Math.sqrt(dx*dx+dy*dy+dz*dz))+" blocks";}
    var d=document.createElement("div");d.className="peer";d.id="p"+pid;
    d.innerHTML="<span class='dot"+(v<0.05?" off":"")+"'></span><span class='pname'>"+esc(n)+"</span><span class='pdist'>"+esc(dt)+"</span><div class='vbg'><div class='vbar' style='width:"+Math.round(v*100)+"%'></div></div>";
    gel("peers").appendChild(d);
  });
}
function esc(s){return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
function mkPC(pid){
  if(conns[pid])return conns[pid];
  var pc=new RTCPeerConnection({iceServers:STUN});
  pc.onicecandidate=function(e){if(e.candidate)send({type:"ice",to:pid,candidate:e.candidate});};
  pc.ontrack=function(e){addAudio(pid,e.streams[0]);renderPeers();};
  pc.onconnectionstatechange=function(){var s=pc.connectionState;if(s==="failed"||s==="closed"||s==="disconnected"){drop(pid);renderPeers();}};
  if(mic)mic.getTracks().forEach(function(t){pc.addTrack(t,mic);});
  conns[pid]=pc;return pc;
}
function drop(pid){if(conns[pid]){conns[pid].close();delete conns[pid];}if(gains[pid]){gains[pid].disconnect();delete gains[pid];}delete names[pid];delete vols[pid];}
function cleanup(){Object.keys(conns).forEach(drop);}
async function handle(msg){
  try{switch(msg.type){
    case"peers":
      await getMic();
      if(msg.peerInfo)msg.peerInfo.forEach(function(p){if(p.username)names[p.id]=p.username;});
      for(var i=0;i<(msg.ids||[]).length;i++){var pc=mkPC(msg.ids[i]),o=await pc.createOffer();await pc.setLocalDescription(o);send({type:"offer",to:msg.ids[i],offer:pc.localDescription});}
      renderPeers();break;
    case"joined":names[msg.id]=msg.username||msg.id;await getMic();renderPeers();break;
    case"left":drop(msg.id);renderPeers();break;
    case"offer":await getMic();var pc2=mkPC(msg.from);await pc2.setRemoteDescription(new RTCSessionDescription(msg.offer));var ans=await pc2.createAnswer();await pc2.setLocalDescription(ans);send({type:"answer",to:msg.from,answer:pc2.localDescription});break;
    case"answer":var pc3=conns[msg.from];if(pc3&&pc3.signalingState!=="stable")await pc3.setRemoteDescription(new RTCSessionDescription(msg.answer));break;
    case"ice":var pc4=conns[msg.from];if(pc4)await pc4.addIceCandidate(new RTCIceCandidate(msg.candidate));break;
    case"positions":if(msg.players)calcVol(msg.players);break;
  }}catch(e){console.error(e);}
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
