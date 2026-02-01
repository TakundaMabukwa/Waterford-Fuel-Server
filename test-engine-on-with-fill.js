require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testEngineOnWithFill() {
  console.log('🚗⛽ Testing Engine ON + Fuel Fill During Operation\n');
  
  const wsClient = new EnergyRiteWebSocketClient();

  // 1. Engine ON
  console.log('📡 1. Engine ON');
  await wsClient.processVehicleUpdate({
    "Plate": "SUNVALLEY",
    "DriverName": "PTO ON",
    "fuel_probe_1_level": 150.0,
    "fuel_probe_1_level_percentage": 75
  });
  
  await new Promise(resolve => setTimeout(resolve, 2000));

  // 2. Fuel Fill Start (while engine is ON)
  console.log('\n📡 2. Fuel Fill Start (engine still ON)');
  await wsClient.processVehicleUpdate({
    "Plate": "SUNVALLEY",
    "DriverName": "Possible Fuel Fill",
    "fuel_probe_1_level": 148.5,
    "fuel_probe_1_level_percentage": 74
  });
  
  await new Promise(resolve => setTimeout(resolve, 3000));

  // 3. Fuel Fill End
  console.log('\n📡 3. Fuel Fill End');
  await wsClient.processVehicleUpdate({
    "Plate": "SUNVALLEY", 
    "DriverName": "",
    "fuel_probe_1_level": 188.5,
    "fuel_probe_1_level_percentage": 94
  });
  
  await new Promise(resolve => setTimeout(resolve, 2000));

  // 4. Engine OFF
  console.log('\n📡 4. Engine OFF');
  await wsClient.processVehicleUpdate({
    "Plate": "SUNVALLEY",
    "DriverName": "PTO OFF", 
    "fuel_probe_1_level": 185.0,
    "fuel_probe_1_level_percentage": 92
  });

  console.log('\n✅ Test completed!');
  console.log('💡 Expected results:');
  console.log('   • 1 Engine session (ONGOING → COMPLETED)');
  console.log('   • 1 Fuel fill session (FUEL_FILL)');
  console.log('   • Engine session updated with fill info');
}

testEngineOnWithFill().then(() => process.exit(0));