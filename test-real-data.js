#!/usr/bin/env node
require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testRealData() {
  const client = new EnergyRiteWebSocketClient('dummy');
  
  // Simulate real WebSocket message with ENGINE ON
  const realMessage = {
    Plate: 'PHOKENG',
    Quality: '61.175.2.101',
    DriverName: 'ENGINE ON',
    fuel_probe_1_level: 263.1,
    fuel_probe_1_volume_in_tank: 805.3,
    fuel_probe_1_temperature: 31,
    fuel_probe_1_level_percentage: 87
  };
  
  console.log('🧪 Testing with real WebSocket data:');
  console.log(`Site: ${realMessage.Plate}`);
  console.log(`Fuel: ${realMessage.fuel_probe_1_level}L (${realMessage.fuel_probe_1_level_percentage}%)`);
  console.log(`Temp: ${realMessage.fuel_probe_1_temperature}°C\n`);
  
  await client.processVehicleUpdate(realMessage);
  console.log('✅ Test completed');
}

testRealData();