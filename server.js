const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'public');
const server = http.createServer((req, res) => {
  const pathname = decodeURIComponent((req.url || '/').split('?')[0]);
  if (pathname === '/health') { res.writeHead(200, {'content-type':'text/plain; charset=utf-8'}); return res.end('ok'); }
  const file = pathname === '/' ? path.join(ROOT, 'index.html') : path.join(ROOT, pathname.replace(/^\/+/,''));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, data) => { if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(file); const types = {'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.png':'image/png','.jpg':'image/jpeg','.webp':'image/webp'};
    res.writeHead(200, {'content-type':types[ext] || 'application/octet-stream','cache-control':'no-cache'}); res.end(data);
  });
});
const wss = new WebSocket.Server({server, path:'/multiplayer', maxPayload: 65536});
const rooms = new Map(); const sockets = new Map(); let serial = 0;
function send(ws, obj) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function publicRoom(r) { return {id:r.id,name:r.name,host:r.host,n:r.clients.size+1,max:r.max,started:r.started,pw:!!r.password}; }
function broadcastRooms() { const list = [...rooms.values()].map(publicRoom); for (const ws of wss.clients) send(ws,{type:'rooms',rooms:list}); }
function roomFor(ws) { const id=sockets.get(ws); return id ? rooms.get(id) : null; }
function removeSocket(ws) {
  const roomId=sockets.get(ws); sockets.delete(ws); if (!roomId) return;
  const r=rooms.get(roomId); if (!r) return;
  if (r.hostSocket === ws) { for (const c of r.clients.values()) send(c.ws,{type:'room-closed'}); rooms.delete(roomId); }
  else { const p=r.clients.get(ws); if (p) { r.clients.delete(ws); send(r.hostSocket,{type:'peer-left',peerId:p.peerId}); } }
  broadcastRooms();
}
wss.on('connection', ws => {
  ws.on('message', raw => {
    let m; try { m=JSON.parse(raw.toString()); } catch (_) { return; }
    if (!m || typeof m.type !== 'string') return;
    if (m.type === 'list') return send(ws,{type:'rooms',rooms:[...rooms.values()].map(publicRoom)});
    if (m.type === 'create') {
      removeSocket(ws); const id='so-'+Date.now().toString(36)+'-'+(++serial).toString(36);
      const r={id,name:String(m.name||'Salon').slice(0,20),host:String(m.host||'Hôte').slice(0,14),max:Math.max(2,Math.min(4,Number(m.max)||4)),password:String(m.password||'').slice(0,16),started:false,hostSocket:ws,clients:new Map()};
      rooms.set(id,r); sockets.set(ws,id); send(ws,{type:'created',room:publicRoom(r)}); broadcastRooms(); return;
    }
    if (m.type === 'join') {
      const r=rooms.get(String(m.roomId||''));
      if (!r) return send(ws,{type:'join-error',message:'Ce salon n’existe plus.'});
      if (r.started) return send(ws,{type:'join-error',message:'Cette partie a déjà commencé.'});
      if (r.password !== String(m.password||'')) return send(ws,{type:'join-error',message:'Mot de passe incorrect.'});
      if (r.clients.size >= r.max-1) return send(ws,{type:'join-error',message:'Ce salon est complet.'});
      removeSocket(ws); const peerId='peer-'+(++serial).toString(36); const name=String(m.name||'Joueur').slice(0,14);
      r.clients.set(ws,{ws,peerId,name}); sockets.set(ws,r.id);
      send(ws,{type:'joined',room:publicRoom(r),peerId}); send(r.hostSocket,{type:'peer-join',peerId,name}); broadcastRooms(); return;
    }
    const r=roomFor(ws); if (!r) return;
    if (m.type === 'signal') {
      if (ws === r.hostSocket) { const target=[...r.clients.values()].find(p=>p.peerId===m.to); if (target) send(target.ws,{type:'signal',from:'host',data:m.data}); }
      else { const p=r.clients.get(ws); if (p) send(r.hostSocket,{type:'signal',from:p.peerId,data:m.data}); }
    } else if (m.type === 'relay') {
      // Relay gameplay data through Render when direct WebRTC connections are blocked.
      if (ws === r.hostSocket) {
        const target=[...r.clients.values()].find(p=>p.peerId===m.to);
        if (target) send(target.ws,{type:'relay',from:'host',data:m.data});
      } else {
        const p=r.clients.get(ws);
        if (p) send(r.hostSocket,{type:'relay',from:p.peerId,data:m.data});
      }
    } else if (m.type === 'room-update' && ws===r.hostSocket) { r.started=!!m.started; broadcastRooms(); }
    else if (m.type === 'start-room' && ws===r.hostSocket) { r.started=true; for (const c of r.clients.values()) send(c.ws,{type:'room-started'}); broadcastRooms(); }
    else if (m.type === 'reject-peer' && ws===r.hostSocket) { const target=[...r.clients.values()].find(p=>p.peerId===m.peerId); if(target) { send(target.ws,{type:'join-error',message:'Le salon est complet.'}); target.ws.close(); } }
    else if (m.type === 'leave') removeSocket(ws);
  });
  ws.on('close',()=>removeSocket(ws)); ws.on('error',()=>removeSocket(ws));
});
server.listen(PORT,()=>console.log('Sainte-Odile multiplayer server listening on '+PORT));
