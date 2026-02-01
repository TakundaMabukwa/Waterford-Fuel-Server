// Quick test to verify server setup
require('dotenv').config();

console.log('Testing server setup...');
console.log('Environment variables:');
console.log('- SUPABASE_URL:', process.env.SUPABASE_URL ? 'Set' : 'Not set');
console.log('- SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? 'Set' : 'Not set');
console.log('- WEBSOCKET_URL:', process.env.WEBSOCKET_URL);
console.log('- PORT:', process.env.PORT);

try {
  const { supabase } = require('./supabase-client');
  console.log('✅ Supabase client loaded successfully');
} catch (error) {
  console.log('❌ Supabase client error:', error.message);
}

try {
  const EnergyRiteWebSocketClient = require('./websocket-client');
  console.log('✅ WebSocket client loaded successfully');
} catch (error) {
  console.log('❌ WebSocket client error:', error.message);
}

try {
  const controller = require('./controllers/energy-rite/energyRiteDataController');
  console.log('✅ EnergyRite controller loaded successfully');
} catch (error) {
  console.log('❌ Controller error:', error.message);
}

console.log('\n🚀 Basic setup verification complete!');
console.log('\nNext steps:');
console.log('1. Set up your Supabase project and update .env file');
console.log('2. Run the migration SQL script in Supabase');
console.log('3. Start the server with: npm start');