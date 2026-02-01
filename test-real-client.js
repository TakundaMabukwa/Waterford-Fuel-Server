require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

console.log('🧪 Starting Real Client Test');
console.log('📡 Connecting to dummy server: ws://localhost:8008\n');

// Create client that connects to dummy server
const client = new EnergyRiteWebSocketClient('ws://localhost:8008');

// Start the connection
client.connect();

console.log('🔍 Watch for:');
console.log('- Engine session creation');
console.log('- Fuel fill session start/complete');
console.log('- Database operations');
console.log('- Session updates\n');

// Keep the process running
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down client...');
  client.close();
  process.exit(0);
});