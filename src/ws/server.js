const WebSocket = require('ws');

let wss = null;
const tripClients = new Map(); // tripId -> Set<WebSocket>

const createWSServer = (httpServer) => {
  wss = new WebSocket.Server({ noServer: true });

  wss.on('connection', (ws, req) => {
    console.log('[ws-server] New frontend WebSocket connection');
    
    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'SUBSCRIBE_TRIP' && msg.tripId) {
          ws.tripId = msg.tripId;
          if (!tripClients.has(msg.tripId)) {
            tripClients.set(msg.tripId, new Set());
          }
          tripClients.get(msg.tripId).add(ws);
          console.log(`[ws-server] Client subscribed to trip ${msg.tripId}`);
        } else if (msg.type === 'UNSUBSCRIBE_TRIP' && msg.tripId) {
          const clients = tripClients.get(msg.tripId);
          if (clients) {
            clients.delete(ws);
            if (clients.size === 0) tripClients.delete(msg.tripId);
          }
          console.log(`[ws-server] Client unsubscribed from trip ${msg.tripId}`);
        }
      } catch (err) {
        console.error('[ws-server] Invalid message:', err.message);
      }
    });

    ws.on('close', () => {
      if (ws.tripId) {
        const clients = tripClients.get(ws.tripId);
        if (clients) {
          clients.delete(ws);
          if (clients.size === 0) tripClients.delete(ws.tripId);
        }
        console.log(`[ws-server] Client disconnected from trip ${ws.tripId}`);
      }
    });

    ws.on('error', (err) => {
      console.error('[ws-server] WebSocket error:', err.message);
    });
  });

  // Attach to HTTP server for upgrade handling
  httpServer.on('upgrade', (req, socket, head) => {
    if (req.url === '/api/trips/ws') {
      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req);
      });
    }
  });

  console.log('[ws-server] WebSocket server ready on /api/trips/ws');
  return wss;
};

const getTripClients = (tripId) => {
  return tripClients.get(tripId);
};

const broadcastToTrip = (tripId, event) => {
  const clients = tripClients.get(tripId);
  if (!clients || clients.size === 0) return;
  
  const message = JSON.stringify(event);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
};

module.exports = { createWSServer, getTripClients, broadcastToTrip };