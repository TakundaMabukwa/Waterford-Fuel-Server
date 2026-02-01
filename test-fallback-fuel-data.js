#!/usr/bin/env node
require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testFallbackFuelData() {
  console.log('🧪 Testing fallback fuel data capture...\n');
  
  const client = new EnergyRiteWebSocketClient('dummy'); // Test mode
  
  // Simulate a WebSocket message WITHOUT fuel data (common scenario)
  const mockMessageNoFuel = {
    Plate: 'BALLYCLARE',
    Quality: '192.168.1.101',
    DriverName: 'ENGINE ON'
    // No fuel_probe_1_level, etc.
  };
  
  console.log('📋 Mock WebSocket message (no fuel data):');
  console.log(JSON.stringify(mockMessageNoFuel, null, 2));
  console.log('\n🔄 Processing message (should fallback to database)...\n');
  
  try {
    await client.processVehicleUpdate(mockMessageNoFuel);
    console.log('✅ Message processed successfully with fallback');
  } catch (error) {
    console.error('❌ Error processing message:', error.message);
  }
}

testFallbackFuelData();