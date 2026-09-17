const wsServer = require('./src/ws/server');
const trips = require('./src/trips');

// Initialize broadcaster with WS server reference
wsServer.createWSServer = ((originalCreate) => {
  return (httpServer) => {
    const wss = originalCreate(httpServer);
    trips.setWSServer(wsServer);
    return wss;
  };
})(wsServer.createWSServer);

module.exports = wsServer;