#!/usr/bin/env node
require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testDataCapture() {
  console.log('🧪 Testing WebSocket data capture...\n');
  
  const client = new EnergyRiteWebSocketClient('dummy'); // Test mode
  
  // Simulate a realistic WebSocket message
  const mockMessage = {
    Plate: 'MILNERTON',
    Quality: '192.168.1.100',
    DriverName: 'ENGINE ON',
    fuel_probe_1_level: 150.5,
    fuel_probe_1_level_percentage: 75.2,
    fuel_probe_1_volume_in_tank: 200.0,
    fuel_probe_1_temperature: 22.5
  };
  
  console.log('📋 Mock WebSocket message:');
  console.log(JSON.stringify(mockMessage, null, 2));
  console.log('\n🔄 Processing message...\n');
  
  try {
    await client.processVehicleUpdate(mockMessage);
    console.log('✅ Message processed successfully');
  } catch (error) {
    console.error('❌ Error processing message:', error.message);
  }
}

testDataCapture();