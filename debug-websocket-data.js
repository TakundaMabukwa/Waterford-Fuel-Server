#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugWebSocketData() {
  console.log('🔍 Debugging WebSocket data capture...\n');
  
  try {
    // Check recent sessions with zero values
    console.log('📊 Recent sessions with zero fuel values:');
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('opening_fuel', 0)
      .order('session_start_time', { ascending: false })
      .limit(5);
    
    if (error) throw error;
    
    sessions.forEach(session => {
      console.log(`🏢 ${session.branch} - ${session.session_start_time}`);
      console.log(`   Opening: ${session.opening_fuel}L (${session.opening_percentage}%)`);
      console.log(`   Closing: ${session.closing_fuel}L (${session.closing_percentage}%)`);
      console.log(`   Temperature: ${session.opening_temperature}°C`);
      console.log(`   Volume: ${session.opening_volume}L`);
      console.log(`   Status: ${session.session_status}`);
      console.log(`   Notes: ${session.notes}\n`);
    });
    
    // Check recent fuel data entries
    console.log('⛽ Recent fuel data entries:');
    const { data: fuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(10);
    
    if (fuelError) throw fuelError;
    
    fuelData.forEach(fuel => {
      console.log(`🏢 ${fuel.plate} - ${fuel.created_at}`);
      console.log(`   Level: ${fuel.fuel_probe_1_level}L (${fuel.fuel_probe_1_level_percentage}%)\n`);
    });
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

debugWebSocketData();