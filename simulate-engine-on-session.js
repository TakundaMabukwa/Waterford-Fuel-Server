require('dotenv').config();
const { supabase } = require('./supabase-client');

/**
 * Simulate engine ON session to test if it gets captured
 */

async function simulateEngineOnSession() {
  console.log('🚗 Simulating Engine ON Session\n');

  const plate = 'SUNVALLEY';
  const currentTime = new Date();

  // First store fuel data (like WebSocket would)
  console.log('📊 Storing fuel data...');
  await supabase.from('energy_rite_fuel_data').insert({
    plate: plate,
    fuel_probe_1_level: 166.2,
    fuel_probe_1_volume_in_tank: 343.6,
    fuel_probe_1_temperature: 27,
    fuel_probe_1_level_percentage: 81,
    speed: 25, // Speed > 0 = Engine ON
    latitude: -34.122268,
    longitude: 18.390642,
    drivername: '',
    address: '0B Buller Louw Blvd, Sunnydale, Cape Town',
    status: 'ON'
  });

  // Simulate session creation logic
  console.log('🔍 Checking for existing sessions...');
  
  const { data: existing } = await supabase
    .from('energy_rite_operating_sessions')
    .select('id')
    .eq('branch', plate)
    .eq('session_status', 'ONGOING')
    .limit(1);

  if (existing.length === 0) {
    console.log('✅ No existing session found, creating new session...');
    
    // Get vehicle info
    const { data: vehicleInfo } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('cost_code, company')
      .eq('plate', plate)
      .single();

    // Create new session
    const { data: newSession, error } = await supabase
      .from('energy_rite_operating_sessions')
      .insert({
        branch: plate,
        company: vehicleInfo?.company || 'KFC',
        cost_code: vehicleInfo?.cost_code,
        session_date: currentTime.toISOString().split('T')[0],
        session_start_time: currentTime.toISOString(),
        opening_fuel: 166.2,
        opening_percentage: 81,
        opening_volume: 343.6,
        opening_temperature: 27,
        session_status: 'ONGOING',
        notes: `Engine started. Opening: 166.2L (81%)`
      })
      .select();

    if (error) {
      console.log(`❌ Error creating session: ${error.message}`);
    } else {
      console.log(`🟢 Engine ON session created!`);
      console.log(`   Session ID: ${newSession[0].id}`);
      console.log(`   Vehicle: ${plate}`);
      console.log(`   Opening Fuel: 166.2L (81%)`);
      console.log(`   Status: ONGOING`);
    }

    // Log activity
    await supabase.from('energy_rite_activity_log').insert({
      branch: plate,
      activity_type: 'ENGINE_ON',
      activity_time: currentTime.toISOString(),
      notes: `Engine ON detected`
    });

  } else {
    console.log(`ℹ️  Session already exists (ID: ${existing[0].id})`);
  }

  // Check current sessions
  console.log('\n📋 Current active sessions:');
  const { data: sessions } = await supabase
    .from('energy_rite_operating_sessions')
    .select('*')
    .eq('session_status', 'ONGOING')
    .order('session_start_time', { ascending: false });

  if (sessions && sessions.length > 0) {
    sessions.forEach(session => {
      console.log(`\n🟢 ${session.branch}:`);
      console.log(`   ID: ${session.id}`);
      console.log(`   Started: ${new Date(session.session_start_time).toLocaleTimeString()}`);
      console.log(`   Opening: ${session.opening_fuel}L (${session.opening_percentage}%)`);
      console.log(`   Company: ${session.company}`);
      console.log(`   Cost Code: ${session.cost_code || 'N/A'}`);
    });
  } else {
    console.log('   No active sessions found');
  }

  console.log('\n✅ Engine ON simulation complete!');
}

simulateEngineOnSession().then(() => process.exit(0));