require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');
const { supabase } = require('./supabase-client');

async function simulateRealtimeWebSocket() {
  console.log('🔄 SIMULATING REAL-TIME WEBSOCKET DETECTION FLOW\n');
  
  const client = new EnergyRiteWebSocketClient('ws://simulation');
  
  // Simulate GERMISTON generator lifecycle
  const site = 'GERMISTON';
  let fuelLevel = 200.0;
  let sessionId = null;
  
  console.log(`🏭 Monitoring ${site} generator...`);
  console.log('═'.repeat(60));
  
  // Step 1: Generator starts (ENGINE ON)
  await simulateMessage({
    Plate: site,
    DriverName: 'PTO ON / ENGINE ON',
    fuel_probe_1_level: fuelLevel,
    fuel_probe_1_level_percentage: 80
  }, 'Generator starts up');
  
  await wait(3000);
  
  // Step 2: Generator running (fuel consumption)
  fuelLevel -= 5.2;
  await simulateMessage({
    Plate: site,
    DriverName: 'Running normally',
    fuel_probe_1_level: fuelLevel,
    fuel_probe_1_level_percentage: 78
  }, 'Generator consuming fuel');
  
  await wait(2000);
  
  // Step 3: More fuel consumption
  fuelLevel -= 8.1;
  await simulateMessage({
    Plate: site,
    DriverName: 'Operating',
    fuel_probe_1_level: fuelLevel,
    fuel_probe_1_level_percentage: 75
  }, 'Continued operation');
  
  await wait(2000);
  
  // Step 4: Generator stops (ENGINE OFF)
  await simulateMessage({
    Plate: site,
    DriverName: 'PTO OFF / ENGINE OFF',
    fuel_probe_1_level: fuelLevel,
    fuel_probe_1_level_percentage: 75
  }, 'Generator shuts down');
  
  await wait(3000);
  
  // Final summary
  console.log('\n📊 FINAL SESSION SUMMARY:');
  await checkFinalResults(site);
  
  async function simulateMessage(message, description) {
    console.log(`\n⏰ ${new Date().toLocaleTimeString()} - ${description}`);
    console.log(`📨 WebSocket Message: ${message.Plate} - "${message.DriverName}"`);
    console.log(`⛽ Fuel: ${message.fuel_probe_1_level}L (${message.fuel_probe_1_level_percentage}%)`);
    
    // Process through our detection system
    const engineStatus = client.parseEngineStatus(message.DriverName);
    console.log(`🔍 Detection Result: ${engineStatus || 'No status change'}`);
    
    if (engineStatus) {
      console.log(`🔄 Processing ${engineStatus} event...`);
      await client.processVehicleUpdate(message);
      
      // Check what happened in database
      await checkDatabaseChanges(site, engineStatus);
    }
    
    console.log('─'.repeat(40));
  }
  
  async function checkDatabaseChanges(site, engineStatus) {
    if (engineStatus === 'ON') {
      // Check for new session
      const { data: sessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('branch', site)
        .eq('session_status', 'ONGOING')
        .order('session_start_time', { ascending: false })
        .limit(1);
      
      if (sessions?.length > 0) {
        sessionId = sessions[0].id;
        console.log(`✅ Session Created: ID ${sessionId}`);
        console.log(`   Start Time: ${sessions[0].session_start_time}`);
        console.log(`   Opening Fuel: ${sessions[0].opening_fuel}L`);
      }
    } else if (engineStatus === 'OFF') {
      // Check for completed session
      const { data: sessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('id', sessionId);
      
      if (sessions?.length > 0) {
        const session = sessions[0];
        console.log(`✅ Session Completed: ID ${sessionId}`);
        console.log(`   Duration: ${session.operating_hours?.toFixed(2) || 0}h`);
        console.log(`   Fuel Used: ${session.total_usage?.toFixed(1) || 0}L`);
        console.log(`   Cost: R${session.cost_for_usage?.toFixed(2) || 0}`);
      }
    }
    
    // Check activity logs
    const { data: logs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .eq('branch', site)
      .gte('created_at', new Date(Date.now() - 10000).toISOString())
      .order('created_at', { ascending: false })
      .limit(1);
    
    if (logs?.length > 0) {
      console.log(`📋 Activity Logged: ${logs[0].activity_type} at ${logs[0].created_at}`);
    }
  }
  
  async function checkFinalResults(site) {
    const { data: sessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('branch', site)
      .gte('session_date', new Date().toISOString().split('T')[0])
      .order('session_start_time', { ascending: false })
      .limit(1);
    
    if (sessions?.length > 0) {
      const session = sessions[0];
      console.log(`🏭 Site: ${session.branch}`);
      console.log(`⏰ Operating Hours: ${session.operating_hours?.toFixed(2) || 0}h`);
      console.log(`⛽ Fuel Usage: ${session.total_usage?.toFixed(1) || 0}L`);
      console.log(`💰 Cost: R${session.cost_for_usage?.toFixed(2) || 0}`);
      console.log(`📊 Status: ${session.session_status}`);
    }
    
    const { data: logs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .eq('branch', site)
      .gte('created_at', new Date(Date.now() - 60000).toISOString())
      .order('created_at', { ascending: false });
    
    console.log(`📋 Total Activity Events: ${logs?.length || 0}`);
  }
  
  function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

simulateRealtimeWebSocket();