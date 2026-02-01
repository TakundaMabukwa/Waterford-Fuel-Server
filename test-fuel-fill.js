const EnergyRiteWebSocketClient = require('./websocket-client');

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
                branch: 'ABC123',
                session_start_time: new Date(Date.now() - 3600000).toISOString(), // 1 hour ago
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
      console.log(`📝 INSERT into ${table}:`, data);
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

// Replace supabase in the client
const originalSupabase = require('./supabase-client').supabase;
require('./supabase-client').supabase = mockSupabase;

async function testFuelFillDuringSession() {
  console.log('🧪 Testing Fuel Fill During Ongoing Session\n');
  
  const client = new EnergyRiteWebSocketClient('ws://test');
  
  // Test data
  const plate = 'ABC123';
  
  // Step 1: Simulate fuel fill start
  console.log('1️⃣ Simulating fuel fill start...');
  const fillStartData = {
    Plate: plate,
    DriverName: 'POSSIBLE FUEL FILL',
    fuel_probe_1_level: '45.5',
    fuel_probe_1_level_percentage: '68'
  };
  
  await client.processVehicleUpdate(fillStartData);
  
  // Wait a moment
  await new Promise(resolve => setTimeout(resolve, 100));
  
  // Step 2: Simulate fuel fill end (status change)
  console.log('\n2️⃣ Simulating fuel fill end (status change to null)...');
  const fillEndData = {
    Plate: plate,
    DriverName: null, // Status changed to null
    fuel_probe_1_level: '65.2', // Fuel increased by 19.7L
    fuel_probe_1_level_percentage: '98'
  };
  
  await client.processVehicleUpdate(fillEndData);
  
  console.log('\n✅ Test completed!');
  console.log('\n📊 Expected Results:');
  console.log('- Fill detected at 45.5L');
  console.log('- Fill ended at 65.2L');
  console.log('- Fill amount: 19.7L');
  console.log('- Session updated with fill event');
}

// Run test
testFuelFillDuringSession().catch(console.error);