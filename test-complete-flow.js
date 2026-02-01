// Mock dependencies
const mockSupabase = {
  from: (table) => ({
    select: () => ({
      eq: () => ({
        eq: () => ({
          order: () => ({
            limit: () => Promise.resolve({
              data: table === 'energy_rite_operating_sessions' ? [{
                id: 789,
                branch: 'FLOW001',
                session_start_time: new Date(Date.now() - 1800000).toISOString(), // 30 min ago
                opening_fuel: 50.0,
                fill_events: 0,
                fill_amount_during_session: 0,
                total_fill: 0,
                notes: 'Engine started. Opening: 50.0L (75%)'
              }] : []
            })
          })
        })
      })
    }),
    insert: (data) => {
      console.log(`📝 ${table}: ${data.activity_type || data.session_status || 'INSERT'}`);
      return Promise.resolve();
    },
    update: (data) => ({
      eq: () => {
        if (data.fill_events) {
          console.log(`📝 ${table}: FILL_UPDATE - Events: ${data.fill_events}, Amount: ${data.fill_amount_during_session}L`);
        } else {
          console.log(`📝 ${table}: SESSION_UPDATE - Status: ${data.session_status}`);
        }
        return Promise.resolve();
      }
    })
  })
};

const mockAxios = {
  get: () => Promise.resolve({
    data: {
      data: [{
        branch: 'FLOW001',
        quality: '192.168.1.102',
        fuel_probe_1_level: '45.8',
        fuel_level: '45.8',
        fuel_probe_1_level_percentage: '69',
        fuel_percentage: '69',
        company: 'KFC'
      }]
    }
  })
};

require.cache[require.resolve('./supabase-client')] = { exports: { supabase: mockSupabase } };
require.cache[require.resolve('axios')] = { exports: mockAxios };

const EnergyRiteWebSocketClient = require('./websocket-client');

async function testCompleteFlow() {
  console.log('🔄 COMPLETE FLOW TEST - Fuel Fills + Normal Sessions\n');
  
  const client = new EnergyRiteWebSocketClient('ws://test');
  const plate = 'FLOW001';
  
  console.log('=== SCENARIO: Complete Operational Day ===\n');
  
  // 1. Engine starts
  console.log('1️⃣ Engine starts (morning)');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'ENGINE ON',
    fuel_probe_1_level: '50.0',
    fuel_probe_1_level_percentage: '75'
  });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // 2. Normal operation
  console.log('\n2️⃣ Normal operation (fuel consumption)');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'RUNNING',
    fuel_probe_1_level: '47.2',
    fuel_probe_1_level_percentage: '71'
  });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // 3. First fuel fill
  console.log('\n3️⃣ First fuel fill starts');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'POSSIBLE FUEL FILL',
    fuel_probe_1_level: '46.8',
    fuel_probe_1_level_percentage: '70'
  });
  await new Promise(resolve => setTimeout(resolve, 500));
  
  console.log('\n4️⃣ First fuel fill ends');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: null,
    fuel_probe_1_level: '72.5',
    fuel_probe_1_level_percentage: '95'
  });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // 5. Continue operation
  console.log('\n5️⃣ Continue operation after fill');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'RUNNING',
    fuel_probe_1_level: '71.8',
    fuel_probe_1_level_percentage: '94'
  });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // 6. Second fuel fill
  console.log('\n6️⃣ Second fuel fill starts');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'FUEL FILL',
    fuel_probe_1_level: '68.2',
    fuel_probe_1_level_percentage: '89'
  });
  await new Promise(resolve => setTimeout(resolve, 500));
  
  console.log('\n7️⃣ Second fuel fill ends');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'IDLE',
    fuel_probe_1_level: '75.0',
    fuel_probe_1_level_percentage: '98'
  });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // 8. Engine stops
  console.log('\n8️⃣ Engine stops (end of day)');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'ENGINE OFF',
    fuel_probe_1_level: '73.5',
    fuel_probe_1_level_percentage: '96'
  });
  
  console.log('\n✅ COMPLETE FLOW TEST FINISHED');
  console.log('\n📊 Expected Results:');
  console.log('- 1 Engine session: 50.0L → 73.5L (net +23.5L due to fills)');
  console.log('- 2 Fuel fill sessions:');
  console.log('  • Fill 1: 46.8L → 72.5L = +25.7L');
  console.log('  • Fill 2: 68.2L → 75.0L = +6.8L');
  console.log('- Engine session updated with both fills (+32.5L total)');
}

testCompleteFlow().catch(console.error);