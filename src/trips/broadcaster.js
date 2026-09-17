// Trip event broadcaster - called by TripTracker to push events to frontend WebSocket clients

let wsServer = null;

const setWSServer = (server) => {
  wsServer = server;
};

const broadcastTripEvent = (event) => {
  if (!wsServer) return;
  
  const tripId = event.trip_id;
  const clients = wsServer.getTripClients(tripId);
  
  if (clients && clients.size > 0) {
    const message = JSON.stringify(event);
    for (const client of clients) {
      if (client.readyState === 1) { // WebSocket.OPEN
        client.send(message);
      }
    }
  }
};

module.exports = { setWSServer, broadcastTripEvent };