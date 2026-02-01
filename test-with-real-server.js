require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

console.log('🧪 Testing with Real Server');
console.log('📡 This will process data through the actual application\n');

// Create the client (this is your real application)
const client = new EnergyRiteWebSocketClient('dummy'); // Use dummy to avoid WebSocket connection

// Test data sequence
const testData = [
  {
    "Plate": "TEST001",
    "DriverName": "ENGINE ON", 
    "fuel_probe_1_level": "45.0",
    "fuel_probe_1_level_percentage": "67",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "RUNNING",
    "fuel_probe_1_level": "43.2", 
    "fuel_probe_1_level_percentage": "64",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "POSSIBLE FUEL FILL",
    "fuel_probe_1_level": "42.8",
    "fuel_probe_1_level_percentage": "63", 
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "",
    "fuel_probe_1_level": "68.5",
    "fuel_probe_1_level_percentage": "95",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001", 
    "DriverName": "RUNNING",
    "fuel_probe_1_level": "67.8",
    "fuel_probe_1_level_percentage": "94",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "ENGINE OFF",
    "fuel_probe_1_level": "66.2",
    "fuel_probe_1_level_percentage": "92",
    "Quality": "192.168.1.100"
  }
];

async function runTest() {
  console.log('🚀 Starting test sequence...\n');
  
  for (let i = 0; i < testData.length; i++) {
    const data = testData[i];
    
    console.log(`\n📤 Step ${i + 1}: Processing "${data.DriverName || 'Empty'}" (${data.fuel_probe_1_level}L)`);
    console.log('=' .repeat(60));
    
    // Process through the real application
    await client.processVehicleUpdate(data);
    
    // Wait between steps
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  console.log('\n✅ Test sequence completed!');
  console.log('\n📊 Expected Results:');
  console.log('- Engine session: ONGOING → COMPLETED');
  console.log('- Fuel fill session: FUEL_FILL_ONGOING → FUEL_FILL_COMPLETED');
  console.log('- Fill amount: 68.5L - 42.8L = 25.7L');
  console.log('- Engine session updated with fill data');
}

runTest().catch(console.error);