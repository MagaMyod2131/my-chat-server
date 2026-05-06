var WebSocket = require('ws');
var server = new WebSocket.Server({ port: process.env.PORT || 8080 });

var rooms = {}; // roomName -> Set<ws>

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
    if (client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  });
}

function getMembers(room) {
  if (!rooms[room]) return [];
  var names = [];
  rooms[room].forEach(function(c) { if (c.name) names.push(c.name); });
  return names;
}

server.on('connection', function(ws) {
  ws.room = null;
  ws.name = null;

  ws.on('message', function(data) {
    var msg;
    try { msg = JSON.parse(data); } catch(e) { return; }

    if (msg.type === 'join') {
      // Покидаем старую комнату
      if (ws.room && rooms[ws.room]) {
        rooms[ws.room].delete(ws);
        broadcast(ws.room, {
          type: 'system',
          room: ws.room,
          text: ws.name + ' вышел из комнаты',
          members: getMembers(ws.room)
        });
      }
      ws.room = msg.room;
      ws.name = msg.name;
      if (!rooms[msg.room]) rooms[msg.room] = new Set();
      rooms[msg.room].add(ws);

      // Уведомить остальных о входе
      broadcast(msg.room, {
        type: 'system',
        room: msg.room,
        text: msg.name + ' подключился',
        members: getMembers(msg.room)
      }, ws);

      // Отправить текущему пользователю список участников
      ws.send(JSON.stringify({
        type: 'members',
        room: msg.room,
        members: getMembers(msg.room)
      }));
    }

    else if (msg.type === 'message') {
      // Рассылаем ВСЕМ включая отправителя — так нет дублей
      broadcastAll(ws.room, {
        type: 'message',
        room: ws.room,
        name: ws.name,
        text: msg.text,
        time: msg.time,
        replyTo: msg.replyTo || null,
        id: msg.id
      });
    }

    else if (msg.type === 'file') {
      // Рассылаем ВСЕМ включая отправителя
      broadcastAll(ws.room, {
        type: 'file',
        room: ws.room,
        name: ws.name,
        time: msg.time,
        fileType: msg.fileType,
        fileName: msg.fileName,
        fileSize: msg.fileSize,
        data: msg.data,
        id: msg.id,
        replyTo: msg.replyTo || null
      });
    }

    else if (msg.type === 'typing') {
      broadcast(ws.room, {
        type: 'typing',
        room: ws.room,
        name: ws.name,
        isTyping: msg.isTyping,
        isSending: msg.isSending || false
      }, ws);
    }
  });

  ws.on('close', function() {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
      broadcast(ws.room, {
        type: 'system',
        room: ws.room,
        text: ws.name + ' вышел из комнаты',
        members: getMembers(ws.room)
      });
    }
  });

  ws.on('error', function() {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
    }
  });
});

console.log('Сервер запущен!');
