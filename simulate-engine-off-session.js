require('dotenv').config();
const { supabase } = require('./supabase-client');

/**
 * Simulate engine OFF to complete the session
 */

async function simulateEngineOffSession() {
  console.log('🛑 Simulating Engine OFF Session\n');

  const plate = 'SUNVALLEY';
  const currentTime = new Date();

  // Store fuel data with lower fuel level (consumption)
  console.log('📊 Storing fuel data (after consumption)...');
  await supabase.from('energy_rite_fuel_data').insert({
    plate: plate,
    fuel_probe_1_level: 158.7, // Consumed 7.5L
    fuel_probe_1_volume_in_tank: 327.4,
    fuel_probe_1_temperature: 29,
    fuel_probe_1_level_percentage: 77,
    speed: 0, // Speed = 0 = Engine OFF
    latitude: -34.122268,
    longitude: 18.390642,
    drivername: '',
    address: '0B Buller Louw Blvd, Sunnydale, Cape Town',
    status: 'OFF'
  });

  // Find ongoing session
  console.log('🔍 Looking for ongoing session...');
  
  const { data: sessions } = await supabase
    .from('energy_rite_operating_sessions')
    .select('*')
    .eq('branch', plate)
    .eq('session_status', 'ONGOING')
    .order('session_start_time', { ascending: false })
    .limit(1);

  if (sessions.length > 0) {
    const session = sessions[0];
    console.log(`✅ Found ongoing session (ID: ${session.id})`);
    
    // Calculate session metrics
    const startTime = new Date(session.session_start_time);
    const operatingHours = Math.max(0, (currentTime - startTime) / 3600000);
    const startingFuel = session.opening_fuel || 0;
    const currentFuel = 158.7;
    const fuelConsumed = Math.max(0, startingFuel - currentFuel);
    const fuelCost = fuelConsumed * 20;
    const literUsagePerHour = operatingHours > 0 ? fuelConsumed / operatingHours : 0;

    console.log(`📊 Session metrics:`);
    console.log(`   Duration: ${operatingHours.toFixed(2)} hours`);
    console.log(`   Starting fuel: ${startingFuel}L`);
    console.log(`   Ending fuel: ${currentFuel}L`);
    console.log(`   Fuel consumed: ${fuelConsumed.toFixed(1)}L`);
    console.log(`   Usage per hour: ${literUsagePerHour.toFixed(1)}L/h`);
    console.log(`   Cost: R${fuelCost.toFixed(2)}`);

    // Complete the session
    const { error } = await supabase
      .from('energy_rite_operating_sessions')
      .update({
        session_end_time: currentTime.toISOString(),
        operating_hours: operatingHours,
        closing_fuel: currentFuel,
        closing_percentage: 77,
        closing_volume: 327.4,
        closing_temperature: 29,
        total_usage: fuelConsumed,
        liter_usage_per_hour: literUsagePerHour,
        cost_for_usage: fuelCost,
        session_status: 'COMPLETED',
        notes: `Engine stopped. Duration: ${operatingHours.toFixed(2)}h, Opening: ${startingFuel}L, Closing: ${currentFuel}L, Used: ${fuelConsumed.toFixed(1)}L`
      })
      .eq('id', session.id);

    if (error) {
      console.log(`❌ Error completing session: ${error.message}`);
    } else {
      console.log(`🔴 Engine OFF session completed!`);
    }

    // Log activity
    await supabase.from('energy_rite_activity_log').insert({
      branch: plate,
      activity_type: 'ENGINE_OFF',
      activity_time: currentTime.toISOString(),
      notes: `Engine OFF detected`
    });

  } else {
    console.log('❌ No ongoing session found for this vehicle');
  }

  // Show completed session
  console.log('\n📋 Completed session:');
  const { data: completedSession } = await supabase
    .from('energy_rite_operating_sessions')
    .select('*')
    .eq('branch', plate)
    .eq('session_status', 'COMPLETED')
    .order('session_end_time', { ascending: false })
    .limit(1);

  if (completedSession && completedSession.length > 0) {
    const session = completedSession[0];
    console.log(`\n🏁 ${session.branch}:`);
    console.log(`   ID: ${session.id}`);
    console.log(`   Started: ${new Date(session.session_start_time).toLocaleTimeString()}`);
    console.log(`   Ended: ${new Date(session.session_end_time).toLocaleTimeString()}`);
    console.log(`   Duration: ${session.operating_hours.toFixed(2)}h`);
    console.log(`   Opening: ${session.opening_fuel}L (${session.opening_percentage}%)`);
    console.log(`   Closing: ${session.closing_fuel}L (${session.closing_percentage}%)`);
    console.log(`   Consumed: ${session.total_usage}L`);
    console.log(`   Usage/hour: ${session.liter_usage_per_hour.toFixed(1)}L/h`);
    console.log(`   Cost: R${session.cost_for_usage.toFixed(2)}`);
    console.log(`   Status: ${session.session_status}`);
  }

  console.log('\n✅ Engine OFF simulation complete!');
}

simulateEngineOffSession().then(() => process.exit(0));