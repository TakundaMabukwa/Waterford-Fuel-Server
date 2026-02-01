const TestWebSocketServer = require('./test-websocket-server');
const EnergyRiteWebSocketClient = require('./websocket-client');

console.log('🧪 Starting local test environment...');

// Start test server on port 8006
const server = new TestWebSocketServer(8006);
server.start();

// Wait a moment for server to start
setTimeout(() => {
  // Create client that connects to local test server
  const client = new EnergyRiteWebSocketClient('ws://localhost:8006');
  client.connect();
  
  // Start test scenario after client connects
  setTimeout(() => {
    console.log('🚀 Starting test scenario...');
    server.runTestScenario();
  }, 2000);
  
}, 1000);

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down test environment...');
  server.stop();
  process.exit(0);
});