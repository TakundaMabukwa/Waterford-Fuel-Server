require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

async function simulateGermistonEvents() {
  console.log('🧪 SIMULATING GERMISTON ON/OFF EVENTS\n');
  
  // Create WebSocket client instance to test detection logic
  const client = new EnergyRiteWebSocketClient('ws://64.227.138.235:8005');
  
  // Test the detection logic directly
  console.log('1. TESTING PATTERN DETECTION:');
  const testMessages = [
    'PTO ON',
    'ENGINE ON', 
    'GENERATOR ON',
    'PTO OFF',
    'ENGINE OFF',
    'GENERATOR OFF'
  ];
  
  testMessages.forEach(msg => {
    const result = client.parseEngineStatus(msg);
    console.log(`   "${msg}" -> ${result || 'null'}`);
  });
  
  // Simulate actual vehicle data for GERMISTON
  console.log('\n2. SIMULATING GERMISTON ENGINE ON:');
  const mockOnData = {
    Plate: 'GERMISTON',
    DriverName: 'PTO ON',
    fuel_probe_1_level: 150.5,
    fuel_probe_1_level_percentage: 75.2
  };
  
  try {
    await client.processVehicleUpdate(mockOnData);
    console.log('   ✅ Engine ON event processed');
  } catch (error) {
    console.error('   ❌ Engine ON failed:', error.message);
  }
  
  // Wait 3 seconds then simulate OFF
  setTimeout(async () => {
    console.log('\n3. SIMULATING GERMISTON ENGINE OFF:');
    const mockOffData = {
      Plate: 'GERMISTON',
      DriverName: 'PTO OFF',
      fuel_probe_1_level: 148.2,
      fuel_probe_1_level_percentage: 74.1
    };
    
    try {
      await client.processVehicleUpdate(mockOffData);
      console.log('   ✅ Engine OFF event processed');
      
      // Check if session was created
      setTimeout(async () => {
        const { supabase } = require('./supabase-client');
        
        console.log('\n4. CHECKING RESULTS:');
        const { data: sessions } = await supabase
          .from('energy_rite_operating_sessions')
          .select('*')
          .eq('branch', 'GERMISTON')
          .order('session_start_time', { ascending: false })
          .limit(1);
        
        if (sessions?.length > 0) {
          const session = sessions[0];
          console.log(`   ✅ Session created: ${session.session_status}`);
          console.log(`   Duration: ${session.operating_hours?.toFixed(2) || 0}h`);
          console.log(`   Usage: ${session.total_usage?.toFixed(1) || 0}L`);
        } else {
          console.log('   ❌ No session created');
        }
        
        const { data: logs } = await supabase
          .from('energy_rite_activity_log')
          .select('*')
          .eq('branch', 'GERMISTON')
          .gte('created_at', new Date(Date.now() - 60000).toISOString())
          .order('created_at', { ascending: false });
        
        console.log(`   Activity logs: ${logs?.length || 0} events`);
        logs?.forEach(log => {
          console.log(`     ${log.activity_type}: ${log.created_at}`);
        });
        
      }, 2000);
      
    } catch (error) {
      console.error('   ❌ Engine OFF failed:', error.message);
    }
  }, 3000);
}

simulateGermistonEvents();