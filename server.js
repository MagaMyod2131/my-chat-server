var WebSocket = require('ws');
var server = new WebSocket.Server({ port: process.env.PORT || 8080 });

var rooms = {};      // roomName -> Set<ws>
var history = {};    // roomName -> [{msg}, ...]
var MAX_HISTORY = 200;

function broadcast(room, data, exclude) {
  if (!rooms[room]) return;
  var json = JSON.stringify(data);
  rooms[room].forEach(function(client) {
    if (client !== exclude && client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  });
}

function broadcastAll(room, data) {
  if (!rooms[room]) return;
  var json = JSON.stringify(data);
  rooms[room].forEach(function(client) {
    if (client.readyState === WebSocket.OPEN) client.send(json);
  });
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

server.on('connection', function(ws) {
  ws.room = null;
  ws.name = null;

  ws.on('message', function(raw) {
    var msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    if (msg.type === 'join') {
      // Покидаем старую комнату
      if (ws.room && rooms[ws.room]) {
        rooms[ws.room].delete(ws);
        broadcast(ws.room, {
          type: 'system', room: ws.room,
          text: ws.name + ' вышел из комнаты',
          members: getMembers(ws.room)
        });
      }
      ws.room = msg.room;
      ws.name = msg.name;
      if (!rooms[msg.room]) rooms[msg.room] = new Set();
      rooms[msg.room].add(ws);

      // Уведомить остальных
      broadcast(msg.room, {
        type: 'system', room: msg.room,
        text: msg.name + ' подключился',
        members: getMembers(msg.room)
      }, ws);

      // Отправить историю + список участников новому пользователю
      ws.send(JSON.stringify({
        type: 'init',
        room: msg.room,
        members: getMembers(msg.room),
        history: history[msg.room] || []
      }));
    }

    else if (msg.type === 'message') {
      var m = {
        type: 'message', room: ws.room, name: ws.name,
        text: msg.text, time: msg.time,
        replyTo: msg.replyTo || null, id: msg.id
      };
      saveMsg(ws.room, m);
      broadcastAll(ws.room, m);
    }

    else if (msg.type === 'file') {
      var f = {
        type: 'file', room: ws.room, name: ws.name,
        fileType: msg.fileType, fileName: msg.fileName,
        fileSize: msg.fileSize, data: msg.data,
        time: msg.time, id: msg.id,
        replyTo: msg.replyTo || null
      };
      saveMsg(ws.room, f);
      broadcastAll(ws.room, f);
    }

    else if (msg.type === 'edit') {
      // Редактировать сообщение в истории
      if (history[ws.room]) {
        for (var i = 0; i < history[ws.room].length; i++) {
          if (history[ws.room][i].id === msg.id && history[ws.room][i].name === ws.name) {
            history[ws.room][i].text = msg.text;
            history[ws.room][i].edited = true;
            break;
          }
        }
      }
      broadcastAll(ws.room, {
        type: 'edit', room: ws.room,
        id: msg.id, text: msg.text, name: ws.name
      });
    }

    else if (msg.type === 'delete') {
      // Удалить сообщение из истории
      if (history[ws.room]) {
        for (var j = 0; j < history[ws.room].length; j++) {
          if (history[ws.room][j].id === msg.id && history[ws.room][j].name === ws.name) {
            history[ws.room].splice(j, 1);
            break;
          }
        }
      }
      broadcastAll(ws.room, {
        type: 'delete', room: ws.room, id: msg.id, name: ws.name
      });
    }

    else if (msg.type === 'clear_chat') {
      // Очистить всю историю комнаты
      history[ws.room] = [];
      broadcastAll(ws.room, {
        type: 'clear_chat', room: ws.room, by: ws.name
      });
    }

    else if (msg.type === 'typing') {
      broadcast(ws.room, {
        type: 'typing', room: ws.room, name: ws.name,
        isTyping: msg.isTyping, isSending: msg.isSending || false
      }, ws);
    }
  });

  ws.on('close', function() {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
      broadcast(ws.room, {
        type: 'system', room: ws.room,
        text: ws.name + ' вышел из комнаты',
        members: getMembers(ws.room)
      });
    }
  });

  ws.on('error', function() {
    if (ws.room && rooms[ws.room]) rooms[ws.room].delete(ws);
  });
});

console.log('Сервер запущен!');
