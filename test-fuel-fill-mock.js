// Mock supabase for testing
const mockSupabase = {
  from: (table) => ({
    select: () => ({
      eq: () => ({
        eq: () => ({
          order: () => ({
            limit: () => Promise.resolve({
              data: table === 'energy_rite_operating_sessions' ? [{
                id: 123,
                branch: 'TEST123',
                session_start_time: new Date(Date.now() - 3600000).toISOString(),
                opening_fuel: 45.5,
                fill_events: 0,
                fill_amount_during_session: 0,
                total_fill: 0,
                notes: 'Engine started. Opening: 45.5L (68%)'
              }] : []
            })
          })
        })
      })
    }),
    insert: (data) => {
      console.log(`📝 INSERT ${table}:`, data);
      return Promise.resolve();
    },
    update: (data) => ({
      eq: () => {
        console.log(`📝 UPDATE ${table}:`, data);
        return Promise.resolve();
      }
    })
  })
};

// Mock axios
const mockAxios = {
  get: () => Promise.resolve({
    data: {
      data: [{
        branch: 'TEST123',
        quality: '192.168.1.100',
        fuel_probe_1_level: '45.5',
        fuel_level: '45.5',
        fuel_probe_1_level_percentage: '68',
        fuel_percentage: '68',
        company: 'KFC'
      }]
    }
  })
};

// Replace modules
require.cache[require.resolve('./supabase-client')] = {
  exports: { supabase: mockSupabase }
};
require.cache[require.resolve('axios')] = {
  exports: mockAxios
};

const EnergyRiteWebSocketClient = require('./websocket-client');

async function simulateWebSocketData() {
  console.log('🧪 Testing Fuel Fill Session with Mock Data\n');
  
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
  
  await new Promise(resolve => setTimeout(resolve, 500));
  
  // Step 2: Fuel fill starts
  console.log('\n2️⃣ Fuel fill starts...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: 'POSSIBLE FUEL FILL',
    fuel_probe_1_level: '44.2',
    fuel_probe_1_level_percentage: '66'
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Step 3: Fuel fill ends
  console.log('\n3️⃣ Fuel fill ends (status becomes null)...');
  await client.processVehicleUpdate({
    Plate: plate,
    DriverName: null,
    fuel_probe_1_level: '68.7',
    fuel_probe_1_level_percentage: '95'
  });
  
  await new Promise(resolve => setTimeout(resolve, 500));
  
  // Step 4: Engine stops
  console.log('\n4️⃣ Engine stops...');
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

simulateWebSocketData().catch(console.error);