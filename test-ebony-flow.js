// Mock dependencies
const mockSupabase = {
  from: (table) => ({
    select: () => ({
      eq: () => ({
        eq: () => ({
          order: () => ({
            limit: () => Promise.resolve({
              data: table === 'energy_rite_operating_sessions' ? [{
                id: 999,
                branch: 'EBONY',
                session_start_time: new Date(Date.now() - 1800000).toISOString(),
                opening_fuel: 45.0,
                fill_events: 0,
                fill_amount_during_session: 0,
                total_fill: 0,
                notes: 'Engine started. Opening: 45.0L (67%)'
              }] : []
            })
          })
        })
      })
    }),
    insert: (data) => {
      console.log(`📝 ${table}: ${data.activity_type || 'INSERT'}`);
      return Promise.resolve();
    },
    update: (data) => ({
      eq: () => {
        if (data.fill_events) {
          console.log(`📝 ${table}: FILL_UPDATE - Events: ${data.fill_events}, Amount: ${data.fill_amount_during_session}L`);
        } else {
          console.log(`📝 ${table}: SESSION_COMPLETE`);
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
        branch: 'EBONY',
        quality: '60.115.1.10',
        fuel_probe_1_level: '42.8',
        fuel_level: '42.8',
        fuel_probe_1_level_percentage: '64',
        fuel_percentage: '64',
        company: 'KFC'
      }]
    }
  })
};

require.cache[require.resolve('./supabase-client')] = { exports: { supabase: mockSupabase } };
require.cache[require.resolve('axios')] = { exports: mockAxios };

const EnergyRiteWebSocketClient = require('./websocket-client');

async function testEbonyFlow() {
  console.log('🧪 Testing EBONY Vehicle - Fuel Fill Flow\n');
  
  const client = new EnergyRiteWebSocketClient('ws://test');
  
  console.log('=== Real WebSocket Data Structure ===\n');
  
  // 1. Engine starts
  console.log('1️⃣ Engine starts');
  await client.processVehicleUpdate({
    Plate: "EBONY",
    Speed: 0,
    Latitude: -26.004057,
    Longitude: 28.17784,
    LocTime: "4055",
    Quality: "60.115.1.10",
    Mileage: null,
    Pocsagstr: "",
    Head: "",
    Geozone: "1031 29 September Dr, Ebony Park, Midrand, 1690, South Africa",
    DriverName: "ENGINE ON",
    NameEvent: "",
    Temperature: "",
    fuel_probe_1_level: "45.0",
    fuel_probe_1_level_percentage: "67"
  });
  
  await new Promise(resolve => setTimeout(resolve, 500));
  
  // 2. Fuel fill starts
  console.log('\n2️⃣ Fuel fill detected');
  await client.processVehicleUpdate({
    Plate: "EBONY",
    Speed: 0,
    Latitude: -26.004057,
    Longitude: 28.17784,
    LocTime: "4055",
    Quality: "60.115.1.10",
    Mileage: null,
    Pocsagstr: "",
    Head: "",
    Geozone: "1031 29 September Dr, Ebony Park, Midrand, 1690, South Africa",
    DriverName: "POSSIBLE FUEL FILL",
    NameEvent: "",
    Temperature: "",
    fuel_probe_1_level: "43.2",
    fuel_probe_1_level_percentage: "64"
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // 3. Fuel fill ends (DriverName becomes empty)
  console.log('\n3️⃣ Fuel fill ends (status cleared)');
  await client.processVehicleUpdate({
    Plate: "EBONY",
    Speed: 0,
    Latitude: -26.004057,
    Longitude: 28.17784,
    LocTime: "4055",
    Quality: "60.115.1.10",
    Mileage: null,
    Pocsagstr: "",
    Head: "",
    Geozone: "1031 29 September Dr, Ebony Park, Midrand, 1690, South Africa",
    DriverName: "",
    NameEvent: "",
    Temperature: "",
    fuel_probe_1_level: "67.8",
    fuel_probe_1_level_percentage: "95"
  });
  
  await new Promise(resolve => setTimeout(resolve, 500));
  
  // 4. Engine stops
  console.log('\n4️⃣ Engine stops');
  await client.processVehicleUpdate({
    Plate: "EBONY",
    Speed: 0,
    Latitude: -26.004057,
    Longitude: 28.17784,
    LocTime: "4055",
    Quality: "60.115.1.10",
    Mileage: null,
    Pocsagstr: "",
    Head: "",
    Geozone: "1031 29 September Dr, Ebony Park, Midrand, 1690, South Africa",
    DriverName: "ENGINE OFF",
    NameEvent: "",
    Temperature: "",
    fuel_probe_1_level: "66.5",
    fuel_probe_1_level_percentage: "93"
  });
  
  console.log('\n✅ EBONY Test Completed');
  console.log('\n📊 Results:');
  console.log('- Vehicle: EBONY (Quality: 60.115.1.10)');
  console.log('- Fuel fill: 43.2L → 67.8L = +24.6L');
  console.log('- Engine session updated with fill data');
}

testEbonyFlow().catch(console.error);