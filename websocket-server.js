const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8005 });

console.log('🚀 WebSocket server started on port 8005');

wss.on('connection', (ws) => {
  console.log('📱 Client connected');
  
  ws.on('message', (message) => {
    console.log('📨 Received:', message.toString());
    
    // Broadcast to all connected clients (including your main WebSocket client)
    wss.clients.forEach((client) => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });
  
  ws.on('close', () => {
    console.log('📱 Client disconnected');
  });
});

console.log('✅ WebSocket server ready to receive mock data');