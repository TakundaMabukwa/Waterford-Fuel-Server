const EnergyRiteWebSocketClient = require('./websocket-client');

console.log('🧪 Testing Fuel Fill Detection with Debug');
console.log('📡 Connecting to test server: ws://localhost:8007\n');

// Create client that connects to our test server
const client = new EnergyRiteWebSocketClient('ws://localhost:8007');

// Start the connection
client.connect();

// Log connection status
setTimeout(() => {
  console.log('⏰ Client should be connected and receiving data...');
  console.log('🔍 Watch for FUEL FILL DEBUG messages in the output');
}, 2000);

// Keep the process running
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down client...');
  client.close();
  process.exit(0);
});