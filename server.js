var WebSocket = require('ws');
var fs = require('fs');
var path = require('path');

var server = new WebSocket.Server({ port: process.env.PORT || 8080 });

var rooms = {};
var history = {};
var users = {};
var userNames = {};
var userHandles = {};
var handleIndex = {};
var dmHistory = {};
var MAX_HISTORY = 200;

var DATA_FILE = path.join(__dirname, 'handles.json');

function loadHandles() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      var d = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      handleIndex = d.handleIndex || {};
      userHandles = d.userHandles || {};
      console.log('Загружено @username:', Object.keys(handleIndex).length);
    }
  } catch(e) { console.log('Ошибка загрузки:', e.message); }
}

function saveHandles() {
  try { fs.writeFileSync(DATA_FILE, JSON.stringify({ handleIndex: handleIndex, userHandles: userHandles }), 'utf8'); }
  catch(e) { console.log('Ошибка сохранения:', e.message); }
}

setInterval(saveHandles, 30000);
process.on('SIGTERM', function() { saveHandles(); process.exit(0); });
process.on('SIGINT', function() { saveHandles(); process.exit(0); });

function dmKey(a, b) { return [a, b].sort().join('::'); }

function broadcast(room, data, exclude) {
  if (!rooms[room]) return;
  var json = JSON.stringify(data);
  rooms[room].forEach(function(c) { if (c !== exclude && c.readyState === 1) c.send(json); });
}

function broadcastAll(room, data) {
  if (!rooms[room]) return;
  var json = JSON.stringify(data);
  rooms[room].forEach(function(c) { if (c.readyState === 1) c.send(json); });
}

function getMembers(room) {
  if (!rooms[room]) return [];
  var out = [];
  rooms[room].forEach(function(c) { if (c.name) out.push(c.name); });
  return out;
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
  ws.handle = null;

  ws.on('message', function(raw) {
    var msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    switch (msg.type) {

      case 'register':
        ws.userId = msg.userId;
        ws.name = msg.name;
        users[msg.userId] = ws;
        userNames[msg.userId] = msg.name;
        if (userHandles[msg.userId]) {
          ws.handle = userHandles[msg.userId];
          ws.send(JSON.stringify({ type: 'handle_ok', handle: ws.handle }));
        }
        break;

      case 'set_handle':
        var h = (msg.handle || '').toLowerCase().replace(/[^a-zа-яё0-9_]/gi, '').slice(0, 20);
        if (!h || h.length < 3) { ws.send(JSON.stringify({ type: 'handle_err', reason: 'Минимум 3 символа' })); break; }
        if (handleIndex[h] && handleIndex[h] !== ws.userId) { ws.send(JSON.stringify({ type: 'handle_err', reason: '@' + h + ' уже занят' })); break; }
        if (ws.handle && handleIndex[ws.handle] === ws.userId) delete handleIndex[ws.handle];
        ws.handle = h;
        userHandles[ws.userId] = h;
        handleIndex[h] = ws.userId;
        ws.send(JSON.stringify({ type: 'handle_ok', handle: h }));
        saveHandles();
        break;

      case 'find_user':
        var q = (msg.query || '').toLowerCase().replace(/^@/, '');
        var found = null;
        if (handleIndex[q] && handleIndex[q] !== ws.userId) {
          var uid = handleIndex[q];
          found = { userId: uid, name: userNames[uid] || '', handle: userHandles[uid] || '', online: !!users[uid] };
        }
        if (!found && msg.query !== ws.userId && userNames[msg.query]) {
          found = { userId: msg.query, name: userNames[msg.query], handle: userHandles[msg.query] || '', online: !!users[msg.query] };
        }
        if (!found) {
          for (var uid2 in userNames) {
            if (uid2 !== ws.userId && userNames[uid2] && userNames[uid2].toLowerCase() === q) {
              found = { userId: uid2, name: userNames[uid2], handle: userHandles[uid2] || '', online: !!users[uid2] };
              break;
            }
          }
        }
        ws.send(JSON.stringify({ type: 'find_result', query: msg.query, user: found }));
        break;

      case 'dm':
        var kd = dmKey(ws.userId, msg.toId);
        var dm = { type: 'dm', fromId: ws.userId, toId: msg.toId, name: ws.name, handle: ws.handle || '', text: msg.text, time: msg.time, id: msg.id, replyTo: msg.replyTo || null };
        saveDM(kd, dm);
        ws.send(JSON.stringify(dm));
        sendTo(msg.toId, dm);
        sendTo(msg.toId, { type: 'push_notify', fromName: ws.name, fromHandle: ws.handle || '', fromId: ws.userId, preview: (msg.text || '').slice(0, 60) });
        break;

      case 'dm_file':
        var kf = dmKey(ws.userId, msg.toId);
        var dmf = { type: 'dm_file', fromId: ws.userId, toId: msg.toId, name: ws.name, handle: ws.handle || '', fileType: msg.fileType, fileName: msg.fileName, fileSize: msg.fileSize, data: msg.data, time: msg.time, id: msg.id };
        saveDM(kf, dmf);
        ws.send(JSON.stringify(dmf));
        sendTo(msg.toId, dmf);
        sendTo(msg.toId, { type: 'push_notify', fromName: ws.name, fromHandle: ws.handle || '', fromId: ws.userId, preview: '[файл] ' + msg.fileName });
        break;

      case 'dm_edit':
        var ke = dmKey(ws.userId, msg.toId);
        if (dmHistory[ke]) { for (var i = 0; i < dmHistory[ke].length; i++) { if (dmHistory[ke][i].id === msg.id && dmHistory[ke][i].fromId === ws.userId) { dmHistory[ke][i].text = msg.text; dmHistory[ke][i].edited = true; break; } } }
        var ep = { type: 'dm_edit', fromId: ws.userId, toId: msg.toId, id: msg.id, text: msg.text };
        ws.send(JSON.stringify(ep)); sendTo(msg.toId, ep); break;

      case 'dm_delete':
        var kdl = dmKey(ws.userId, msg.toId);
        if (dmHistory[kdl]) { for (var j = 0; j < dmHistory[kdl].length; j++) { if (dmHistory[kdl][j].id === msg.id && dmHistory[kdl][j].fromId === ws.userId) { dmHistory[kdl].splice(j, 1); break; } } }
        var dp = { type: 'dm_delete', fromId: ws.userId, toId: msg.toId, id: msg.id };
        ws.send(JSON.stringify(dp)); sendTo(msg.toId, dp); break;

      case 'dm_history':
        var hk = dmKey(ws.userId, msg.withId);
        ws.send(JSON.stringify({ type: 'dm_history', withId: msg.withId, msgs: dmHistory[hk] || [] })); break;

      case 'dm_clear':
        var ck = dmKey(ws.userId, msg.toId);
        dmHistory[ck] = [];
        var cp = { type: 'dm_clear', fromId: ws.userId, toId: msg.toId };
        ws.send(JSON.stringify(cp)); sendTo(msg.toId, cp); break;

      case 'dm_typing':
        sendTo(msg.toId, { type: 'dm_typing', fromId: ws.userId, name: ws.name, isTyping: msg.isTyping, isSending: msg.isSending || false }); break;

      case 'call_offer':
        sendTo(msg.toId, { type: 'call_offer', fromId: ws.userId, fromName: ws.name, fromHandle: ws.handle || '', sdp: msg.sdp, callType: msg.callType || 'audio' }); break;
      case 'call_answer':
        sendTo(msg.toId, { type: 'call_answer', fromId: ws.userId, sdp: msg.sdp }); break;
      case 'call_ice':
        sendTo(msg.toId, { type: 'call_ice', fromId: ws.userId, candidate: msg.candidate }); break;
      case 'call_reject':
        sendTo(msg.toId, { type: 'call_reject', fromId: ws.userId }); break;
      case 'call_end':
        sendTo(msg.toId, { type: 'call_end', fromId: ws.userId }); break;

      case 'join':
        if (ws.room && rooms[ws.room]) {
          rooms[ws.room].delete(ws);
          broadcast(ws.room, { type: 'system', room: ws.room, text: (ws.name || '?') + ' вышел из комнаты', members: getMembers(ws.room) });
        }
        ws.room = msg.room; ws.name = msg.name;
        if (!rooms[msg.room]) rooms[msg.room] = new Set();
        rooms[msg.room].add(ws);
        broadcast(msg.room, { type: 'system', room: msg.room, text: msg.name + ' подключился', members: getMembers(msg.room) }, ws);
        ws.send(JSON.stringify({ type: 'init', room: msg.room, members: getMembers(msg.room), history: history[msg.room] || [] }));
        break;

      case 'message':
        var gm = { type: 'message', room: ws.room, name: ws.name, text: msg.text, time: msg.time, replyTo: msg.replyTo || null, id: msg.id };
        saveMsg(ws.room, gm); broadcastAll(ws.room, gm); break;

      case 'file':
        var gf = { type: 'file', room: ws.room, name: ws.name, fileType: msg.fileType, fileName: msg.fileName, fileSize: msg.fileSize, data: msg.data, time: msg.time, id: msg.id, replyTo: msg.replyTo || null };
        saveMsg(ws.room, gf); broadcastAll(ws.room, gf); break;

      case 'edit':
        if (history[ws.room]) { for (var ei = 0; ei < history[ws.room].length; ei++) { if (history[ws.room][ei].id === msg.id && history[ws.room][ei].name === ws.name) { history[ws.room][ei].text = msg.text; history[ws.room][ei].edited = true; break; } } }
        broadcastAll(ws.room, { type: 'edit', room: ws.room, id: msg.id, text: msg.text, name: ws.name }); break;

      case 'delete':
        if (history[ws.room]) { for (var di = 0; di < history[ws.room].length; di++) { if (history[ws.room][di].id === msg.id && history[ws.room][di].name === ws.name) { history[ws.room].splice(di, 1); break; } } }
        broadcastAll(ws.room, { type: 'delete', room: ws.room, id: msg.id, name: ws.name }); break;

      case 'clear_chat':
        history[ws.room] = [];
        broadcastAll(ws.room, { type: 'clear_chat', room: ws.room, by: ws.name }); break;

      case 'typing':
        broadcast(ws.room, { type: 'typing', room: ws.room, name: ws.name, isTyping: msg.isTyping, isSending: msg.isSending || false }, ws); break;
    }
  });

  ws.on('close', function() {
    if (ws.userId) delete users[ws.userId];
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
      broadcast(ws.room, { type: 'system', room: ws.room, text: (ws.name || '?') + ' вышел из комнаты', members: getMembers(ws.room) });
    }
  });

  ws.on('error', function() {
    if (ws.userId) delete users[ws.userId];
    if (ws.room && rooms[ws.room]) rooms[ws.room].delete(ws);
  });
});

loadHandles();
console.log('Сервер запущен!');
