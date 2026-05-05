const WebSocket = require('ws');
const server = new WebSocket.Server({ port: process.env.PORT || 8080 });

// rooms: { roomName: Set<ws> }
const rooms = {};

function broadcast(room, data, exclude = null) {
  if (!rooms[room]) return;
  const json = JSON.stringify(data);
  rooms[room].forEach(client => {
    if (client !== exclude && client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  });
}

function getRoomMembers(room) {
  if (!rooms[room]) return [];
  return [...rooms[room]].map(c => c.name).filter(Boolean);
}

server.on('connection', (ws) => {
  ws.room = null;
  ws.name = null;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    switch (msg.type) {

      case 'join': {
        // Покидаем предыдущую комнату
        if (ws.room && rooms[ws.room]) {
          rooms[ws.room].delete(ws);
          broadcast(ws.room, {
            type: 'system',
            room: ws.room,
            text: `${ws.name} вышел из комнаты`,
            name: ws.name,
            members: getRoomMembers(ws.room)
          });
        }
        ws.room = msg.room;
        ws.name = msg.name;
        if (!rooms[msg.room]) rooms[msg.room] = new Set();
        rooms[msg.room].add(ws);
        // Уведомить всех о входе
        broadcast(msg.room, {
          type: 'system',
          room: msg.room,
          text: `${msg.name} подключился`,
          name: msg.name,
          members: getRoomMembers(msg.room)
        }, ws);
        // Отправить текущему пользователю список участников
        ws.send(JSON.stringify({
          type: 'members',
          room: msg.room,
          members: getRoomMembers(msg.room)
        }));
        break;
      }

      case 'message': {
        broadcast(ws.room, {
          type: 'message',
          room: ws.room,
          name: ws.name,
          text: msg.text,
          time: msg.time,
          replyTo: msg.replyTo || null,
          id: msg.id
        });
        break;
      }

      case 'file': {
        broadcast(ws.room, {
          type: 'file',
          room: ws.room,
          name: ws.name,
          time: msg.time,
          fileType: msg.fileType, // image/audio/video/file
          fileName: msg.fileName,
          fileSize: msg.fileSize,
          data: msg.data, // base64
          id: msg.id
        });
        break;
      }

      case 'typing': {
        broadcast(ws.room, {
          type: 'typing',
          room: ws.room,
          name: ws.name,
          isTyping: msg.isTyping,
          isSending: msg.isSending || false
        }, ws);
        break;
      }
    }
  });

  ws.on('close', () => {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
      broadcast(ws.room, {
        type: 'system',
        room: ws.room,
        text: `${ws.name} вышел из комнаты`,
        name: ws.name,
        members: getRoomMembers(ws.room)
      });
    }
  });

  ws.on('error', () => {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
    }
  });
});

console.log('Сервер запущен!');
