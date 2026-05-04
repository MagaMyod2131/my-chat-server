const WebSocket = require('ws');
const server = new WebSocket.Server({ port: process.env.PORT || 8080 });

const rooms = {};

server.on('connection', (ws) => {
  console.log('Новое подключение');

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);

      if (msg.type === 'join') {
        ws.room = msg.room;
        ws.name = msg.name;
        if (!rooms[msg.room]) rooms[msg.room] = new Set();
        rooms[msg.room].add(ws);
        console.log(`${msg.name} вошёл в комнату ${msg.room}`);
      }

      if (msg.type === 'message') {
        rooms[ws.room]?.forEach(client => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(msg));
          }
        });
      }
    } catch (e) {
      console.error('Ошибка:', e);
    }
  });

  ws.on('close', () => {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].delete(ws);
    }
  });
});

console.log('Сервер запущен!');
