#!/usr/bin/env node
require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testExternalFuelData() {
  console.log('🧪 Testing external fuel data capture...\n');
  
  const client = new EnergyRiteWebSocketClient('dummy'); // Test mode
  
  // Simulate an engine on event without fuel data in WebSocket message
  const mockEngineOnMessage = {
    Plate: 'MILNERTON',
    Quality: '61.13.2.31',
    DriverName: 'ENGINE ON'
    // No fuel data in WebSocket message - should get from external API
  };
  
  console.log('📋 Mock ENGINE ON message (no fuel data):');
  console.log(JSON.stringify(mockEngineOnMessage, null, 2));
  console.log('\n🔄 Processing message (should get fuel data from external API)...\n');
  
  try {
    await client.processVehicleUpdate(mockEngineOnMessage);
    console.log('✅ Message processed successfully with external fuel data');
  } catch (error) {
    console.error('❌ Error processing message:', error.message);
  }
}

testExternalFuelData();