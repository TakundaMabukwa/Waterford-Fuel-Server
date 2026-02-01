require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

// Create client that connects to the real WebSocket server
const client = new EnergyRiteWebSocketClient('ws://64.227.138.235:8005');

console.log('🧪 Starting test WebSocket client...');
console.log('📡 This will connect to the real WebSocket server and process test messages');

// Connect to the WebSocket server
client.connect();

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down test client...');
  client.close();
  process.exit(0);
});

// Keep the process running
console.log('✅ Test client running. Press Ctrl+C to stop.');