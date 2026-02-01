require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function testRealWebSocketData() {
  console.log('🧪 TESTING REAL WEBSOCKET DATA\n');
  
  const client = new EnergyRiteWebSocketClient('ws://test');
  
  // Real WebSocket message from NORTHMEAD
  const realMessage = {
    "Plate": "NORTHMEAD",
    "Speed": 0,
    "Latitude": -26.16966,
    "Longitude": 28.32891,
    "LocTime": "385",
    "Quality": "60.115.1.41",
    "Mileage": null,
    "Pocsagstr": "",
    "Head": "",
    "Geozone": "29 2nd Street, Benoni 1501, South Africa",
    "DriverName": "PTO ON / ENGINE ON",
    "NameEvent": "",
    "Temperature": "25,405,1006,2020,0AC5,2021,17F6,2022,1C,2023,57",
    "fuel_probe_1_level": 275.7,
    "fuel_probe_1_volume_in_tank": 613.4,
    "fuel_probe_1_temperature": 28,
    "fuel_probe_1_level_percentage": 87,
    "message_type": 405
  };
  
  console.log('📨 Processing real WebSocket message:');
  console.log(`   Site: ${realMessage.Plate}`);
  console.log(`   DriverName: "${realMessage.DriverName}"`);
  console.log(`   Fuel Level: ${realMessage.fuel_probe_1_level}L (${realMessage.fuel_probe_1_level_percentage}%)`);
  
  // Test pattern detection
  const engineStatus = client.parseEngineStatus(realMessage.DriverName);
  console.log(`   Detected Status: ${engineStatus}`);
  
  if (engineStatus) {
    console.log('\n🟢 Processing ENGINE ON event...');
    try {
      await client.processVehicleUpdate(realMessage);
      console.log('✅ Message processed successfully');
      
      // Check if session was created
      setTimeout(async () => {
        const { supabase } = require('./supabase-client');
        
        const { data: sessions } = await supabase
          .from('energy_rite_operating_sessions')
          .select('*')
          .eq('branch', 'NORTHMEAD')
          .eq('session_status', 'ONGOING')
          .order('session_start_time', { ascending: false })
          .limit(1);
        
        if (sessions?.length > 0) {
          const session = sessions[0];
          console.log('\n📊 SESSION CREATED:');
          console.log(`   ID: ${session.id}`);
          console.log(`   Start: ${session.session_start_time}`);
          console.log(`   Opening Fuel: ${session.opening_fuel}L`);
          console.log(`   Status: ${session.session_status}`);
        } else {
          console.log('\n❌ No session created');
        }
        
        // Check activity logs
        const { data: logs } = await supabase
          .from('energy_rite_activity_log')
          .select('*')
          .eq('branch', 'NORTHMEAD')
          .gte('created_at', new Date(Date.now() - 60000).toISOString())
          .order('created_at', { ascending: false });
        
        console.log(`\n📋 Activity Logs: ${logs?.length || 0} events`);
        logs?.forEach(log => {
          console.log(`   ${log.activity_type}: ${log.created_at}`);
        });
        
      }, 2000);
      
    } catch (error) {
      console.error('❌ Processing failed:', error.message);
    }
  } else {
    console.log('❌ No engine status detected');
  }
}

testRealWebSocketData();