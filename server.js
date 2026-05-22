var WebSocket = require('ws');
var server = new WebSocket.Server({ port: process.env.PORT || 8080 });

var rooms = {};
var history = {};
var users = {};
var userNames = {};
var dmHistory = {};
var MAX_HISTORY = 300;

function dmKey(a, b) { return [a, b].sort().join('::'); }

function broadcast(room, data, exclude) {
  if (!rooms[room]) return;
  var json = JSON.stringify(data);
  rooms[room].forEach(function(c) {
    if (c !== exclude && c.readyState === 1) c.send(json);
  });
}

function broadcastAll(room, data) {
  if (!rooms[room]) return;
  var json = JSON.stringify(data);
  rooms[room].forEach(function(c) { if (c.readyState === 1) c.send(json); });
}

function getMembers(room) {
  if (!rooms[room]) return [];
  var names = [];
  rooms[room].forEach(function(c) { if (c.name) names.push(c.name); });
  return names;
}

function saveMsg(room, msg) {
  if (!history[room]) history[room] = [];
  history[room].push(msg);
  if (history[room].length > MAX_HISTORY) history[room].shift();
}

function saveDM(key, msg) {
  if (!dmHistory[key]) dmHistory[key] = [];
  dmHistory[key].push(msg);
  if (dmHistory[key].length > MAX_HISTORY) dmHistory[key].shift();
}

function sendTo(userId, data) {
  var ws = users[userId];
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(data));
}

server.on('connection', function(ws) {
  ws.room = null;
  ws.name = null;
  ws.userId = null;

  ws.on('message', function(raw) {
    var msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    switch (msg.type) {

      case 'register':
        ws.userId = msg.userId;
        ws.name = msg.name;
        users[msg.userId] = ws;
        userNames[msg.userId] = msg.name;
        break;

      case 'find_user':
        var found = null;
        // По ID
        if (msg.query && msg.query !== ws.userId && userNames[msg.query]) {
          found = { userId: msg.query, name: userNames[msg.query], online: !!users[msg.query] };
        }
        // По имени
        if (!found) {
          for (var uid in userNames) {
            if (uid !== ws.userId && userNames[uid] &&
                userNames[uid].toLowerCase() === (msg.query || '').toLowerCase()) {
              found = { userId: uid, name: userNames[uid], online: !!users[uid] };
              break;
            }
          }
        }
        ws.send(JSON.stringify({ type: 'find_result', query: msg.query, user: found }));
        break;

      // ── ЛИЧНЫЕ СООБЩЕНИЯ ──
      case 'dm':
        var k = dmKey(ws.userId, msg.toId);
        var dm = {
          type: 'dm', fromId: ws.userId, toId: msg.toId,
          name: ws.name, text: msg.text, time: msg.time,
          id: msg.id, replyTo: msg.replyTo || null
        };
        saveDM(k, dm);
        ws.send(JSON.stringify(dm));
        sendTo(msg.toId, dm);
        break;

      case 'dm_file':
        var kf = dmKey(ws.userId, msg.toId);
        var dmf = {
          type: 'dm_file', fromId: ws.userId, toId: msg.toId,
          name: ws.name, fileType: msg.fileType, fileName: msg.fileName,
          fileSize: msg.fileSize, data: msg.data, time: msg.time, id: msg.id
        };
        saveDM(kf, dmf);
        ws.send(JSON.stringify(dmf));
        sendTo(msg.toId, dmf);
        break;

      case 'dm_edit':
        var ke = dmKey(ws.userId, msg.toId);
        if (dmHistory[ke]) {
          for (var i = 0; i < dmHistory[ke].length; i++) {
            if (dmHistory[ke][i].id === msg.id && dmHistory[ke][i].fromId === ws.userId) {
              dmHistory[ke][i].text = msg.text;
              dmHistory[ke][i].edited = true;
              break;
            }
          }
        }
        var ep = { type: 'dm_edit', fromId: ws.userId, toId: msg.toId, id: msg.id, text: msg.text };
        ws.send(JSON.stringify(ep));
        sendTo(msg.toId, ep);
        break;

      case 'dm_delete':
        var kd = dmKey(ws.userId, msg.toId);
        if (dmHistory[kd]) {
          for (var j = 0; j < dmHistory[kd].length; j++) {
            if (dmHistory[kd][j].id === msg.id && dmHistory[kd][j].fromId === ws.userId) {
              dmHistory[kd].splice(j, 1);
              break;
            }
          }
        }
        var dp = { type: 'dm_delete', fromId: ws.userId, toId: msg.toId, id: msg.id };
        ws.send(JSON.stringify(dp));
        sendTo(msg.toId, dp);
        break;

      case 'dm_history':
        var hk = dmKey(ws.userId, msg.withId);
        ws.send(JSON.stringify({ type: 'dm_history', withId: msg.withId, msgs: dmHistory[hk] || [] }));
        break;

      case 'dm_clear':
        var ck = dmKey(ws.userId, msg.toId);
        dmHistory[ck] = [];
        var cp = { type: 'dm_clear', fromId: ws.userId, toId: msg.toId };
        ws.send(JSON.stringify(cp));
        sendTo(msg.toId, cp);
        break;

      case 'dm_typing':
        sendTo(msg.toId, {
          type: 'dm_typing', fromId: ws.userId, name: ws.name,
          isTyping: msg.isTyping, isSending: msg.isSending || false
        });
        break;

      // ── ЗВОНКИ P2P ──
      case 'call_offer':
        sendTo(msg.toId, {
          type: 'call_offer', fromId: ws.userId, fromName: ws.name,
          sdp: msg.sdp, callType: msg.callType || 'audio'
        });
        break;
      case 'call_answer':
        sendTo(msg.toId, { type: 'call_answer', fromId: ws.userId, sdp: msg.sdp });
        break;
      case 'call_ice':
        sendTo(msg.toId, { type: 'call_ice', fromId: ws.userId, candidate: msg.candidate });
        break;
      case 'call_reject':
        sendTo(msg.toId, { type: 'call_reject', fromId: ws.userId });
        break;
      case 'call_end':
        sendTo(msg.toId, { type: 'call_end', fromId: ws.userId });
        break;

      // ── ГРУППОВЫЕ ЗВОНКИ ──
      // Участник заходит в групповой звонок — сервер рассылает всем в комнате
      case 'group_call_join':
        if (!ws.room) break;
        broadcast(ws.room, {
          type: 'group_call_join', fromId: ws.userId, fromName: ws.name
        }, ws);
        break;

      // Один участник шлёт offer конкретному другому (mesh-топология)
      case 'group_call_offer':
        sendTo(msg.toId, {
          type: 'group_call_offer', fromId: ws.userId, fromName: ws.name,
          sdp: msg.sdp, callType: msg.callType || 'audio'
        });
        break;
      case 'group_call_answer':
        sendTo(msg.toId, { type: 'group_call_answer', fromId: ws.userId, sdp: msg.sdp });
        break;
      case 'group_call_ice':
        sendTo(msg.toId, { type: 'group_call_ice', fromId: ws.userId, candidate: msg.candidate });
        break;
      case 'group_call_leave':
        if (!ws.room) break;
        broadcast(ws.room, {
          type: 'group_call_leave', fromId: ws.userId, fromName: ws.name
        }, ws);
        break;

      // ── ГРУППЫ ──
      case 'join':
        if (ws.room && rooms[ws.room]) {
          rooms[ws.room].delete(ws);
          broadcast(ws.room, {
            type: 'system', room: ws.room,
            text: (ws.name || '?') + ' вышел из комнаты',
            members: getMembers(ws.room)
          });
        }
        ws.room = msg.room;
        ws.name = msg.name;
        if (!rooms[msg.room]) rooms[msg.room] = new Set();
        rooms[msg.room].add(ws);
        broadcast(msg.room, {
          type: 'system', room: msg.room,
          text: msg.name + ' подключился',
          members: getMembers(msg.room)
        }, ws);
        ws.send(JSON.stringify({
          type: 'init', room: msg.room,
          members: getMembers(msg.room),
          history: history[msg.room] || []
        }));
        break;

      case 'message':
        var gm = {
          type: 'message', room: ws.room, name: ws.name,
          text: msg.text, time: msg.time,
          replyTo: msg.replyTo || null, id: msg.id
        };
        saveMsg(ws.room, gm);
        broadcastAll(ws.room, gm);
        break;

      case 'file':
        var gf = {
          type: 'file', room: ws.room, name: ws.name,
          fileType: msg.fileType, fileName: msg.fileName,
          fileSize: msg.fileSize, data: msg.data,
          time: msg.time, id: msg.id, replyTo: msg.replyTo || null
        };
        saveMsg(ws.room, gf);
        broadcastAll(ws.room, gf);
        break;

      case 'edit':
        if (history[ws.room]) {
          for (var ei = 0; ei < history[ws.room].length; ei++) {
            if (history[ws.room][ei].id === msg.id && history[ws.room][ei].name === ws.name) {
              history[ws.room][ei].text = msg.text;
              history[ws.room][ei].edited = true;
              break;
            }
          }
        }
        broadcastAll(ws.room, { type: 'edit', room: ws.room, id: msg.id, text: msg.text, name: ws.name });
        break;

      case 'delete':
        if (history[ws.room]) {
          for (var di = 0; di < history[ws.room].length; di++) {
            if (history[ws.room][di].id === msg.id && history[ws.room][di].name === ws.name) {
              history[ws.room].splice(di, 1);
              break;
            }
          }
        }
        broadcastAll(ws.room, { type: 'delete', room: ws.room, id: msg.id, name: ws.name });
        break;

      case 'clear_chat':
        history[ws.room] = [];
        broadcastAll(ws.room, { type: 'clear_chat', room: ws.room, by: ws.name });
        break;

      case 'typing':
        broadcast(ws.room, {
          type: 'typing', room: ws.room, name: ws.name,
          isTyping: msg.isTyping, isSending: msg.isSending || false
        }, ws);
        break;
    }
  });

  ws.on('close', function() {
    if (ws.userId) delete users[ws.userId];
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
      broadcast(ws.room, {
        type: 'system', room: ws.room,
        text: (ws.name || '?') + ' вышел из комнаты',
        members: getMembers(ws.room)
      });
    }
  });

  ws.on('error', function() {
    if (ws.userId) delete users[ws.userId];
    if (ws.room && rooms[ws.room]) rooms[ws.room].delete(ws);
  });
});

console.log('Сервер запущен!');


