require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testEngineSessions() {
  console.log('🚗 Testing Engine Sessions\n');
  
  const wsClient = new EnergyRiteWebSocketClient();

  // Engine ON message
  const engineOnMessage = {
    "Plate": "SUNVALLEY",
    "Speed": 25,
    "DriverName": "PTO ON", // This triggers engine session
    "fuel_probe_1_level": 166.2,
    "fuel_probe_1_level_percentage": 81
  };

  console.log('📡 Sending Engine ON message:');
  console.log(`   DriverName: "${engineOnMessage.DriverName}"`);
  await wsClient.processVehicleUpdate(engineOnMessage);
  
  await new Promise(resolve => setTimeout(resolve, 3000));

  // Engine OFF message  
  const engineOffMessage = {
    "Plate": "SUNVALLEY", 
    "Speed": 0,
    "DriverName": "PTO OFF", // This completes engine session
    "fuel_probe_1_level": 158.5,
    "fuel_probe_1_level_percentage": 77
  };

  console.log('\n📡 Sending Engine OFF message:');
  console.log(`   DriverName: "${engineOffMessage.DriverName}"`);
  await wsClient.processVehicleUpdate(engineOffMessage);

  console.log('\n✅ Engine session test completed!');
}

testEngineSessions().then(() => process.exit(0));