require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

console.log('🧪 Connecting Real App to Test Server');
console.log('📡 This connects your actual app to the test data server\n');

// Create client that connects to test server (REAL CONNECTION)
const client = new EnergyRiteWebSocketClient('ws://localhost:8009');

// Connect to the test server
client.connect();

console.log('🔍 Watch for:');
console.log('- WebSocket connection to test server');
console.log('- Real message processing');
console.log('- Database operations');
console.log('- Engine and fuel fill sessions');
console.log('- All debug output\n');

// Keep running
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down...');
  client.close();
  process.exit(0);
});