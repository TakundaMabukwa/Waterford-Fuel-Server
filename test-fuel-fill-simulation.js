const EnergyRiteWebSocketClient = require('./websocket-client');
const { supabase } = require('./supabase-client');

async function simulateWebSocketData() {
  console.log('🧪 Testing Fuel Fill Session with Dummy WebSocket Data\n');
  
  const client = new EnergyRiteWebSocketClient('ws://test');
  const plate = 'TEST123';
  
  console.log('=== SCENARIO: Engine Session + Fuel Fill ===\n');
  
  // Step 1: Engine starts
  console.log('1️⃣ Engine starts...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'ENGINE ON',
    fuel_probe_1_level: '45.5',
    fuel_probe_1_level_percentage: '68'
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Step 2: Normal operation
  console.log('\n2️⃣ Normal operation...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'RUNNING',
    fuel_probe_1_level: '44.8',
    fuel_probe_1_level_percentage: '67'
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Step 3: Fuel fill starts
  console.log('\n3️⃣ Fuel fill starts...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'POSSIBLE FUEL FILL',
    fuel_probe_1_level: '44.2',
    fuel_probe_1_level_percentage: '66'
  });
  
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  // Step 4: Fuel fill ends (status changes to null)
  console.log('\n4️⃣ Fuel fill ends (status becomes null)...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: null,
    fuel_probe_1_level: '68.7',
    fuel_probe_1_level_percentage: '95'
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Step 5: Continue operation
  console.log('\n5️⃣ Continue operation...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'RUNNING',
    fuel_probe_1_level: '68.1',
    fuel_probe_1_level_percentage: '94'
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Step 6: Engine stops
  console.log('\n6️⃣ Engine stops...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'ENGINE OFF',
    fuel_probe_1_level: '67.5',
    fuel_probe_1_level_percentage: '93'
  });
  
  console.log('\n✅ Simulation completed!');
  console.log('\n📊 Expected Results:');
  console.log('- Engine session created with opening fuel: 45.5L');
  console.log('- Fuel fill session: 44.2L → 68.7L = +24.5L filled');
  console.log('- Engine session updated with fill amount');
  console.log('- Engine session closed with final fuel: 67.5L');
}

// Run simulation
simulateWebSocketData().catch(console.error);