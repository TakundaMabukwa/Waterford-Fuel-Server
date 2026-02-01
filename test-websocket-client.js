require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

console.log('🧪 Starting WebSocket Client Test');
console.log('📡 Connecting to test server: ws://localhost:8090\n');

// Create client that connects to our test server
const client = new EnergyRiteWebSocketClient('ws://localhost:8090');

// Start the connection
client.connect();

// Keep the process running
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down client...');
  client.close();
  process.exit(0);
});