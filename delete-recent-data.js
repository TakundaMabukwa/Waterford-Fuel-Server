#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function deleteRecentData() {
  console.log('🗑️ Deleting data added in the last 10 minutes...\n');
  
  try {
    // Calculate 10 minutes ago
    const tenMinutesAgo = new Date();
    tenMinutesAgo.setMinutes(tenMinutesAgo.getMinutes() - 10);
    const cutoffTime = tenMinutesAgo.toISOString();
    
    console.log(`🕒 Deleting records created after: ${cutoffTime}`);
    
    // Delete from operating sessions
    const { data: deletedSessions, error: sessionsError } = await supabase
      .from('energy_rite_operating_sessions')
      .delete()
      .gte('created_at', cutoffTime)
      .select('id');
    
    if (sessionsError) {
      console.error('❌ Error deleting sessions:', sessionsError.message);
    } else {
      console.log(`✅ Deleted ${deletedSessions?.length || 0} operating sessions`);
    }
    
    // Delete from fuel data
    const { data: deletedFuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .delete()
      .gte('created_at', cutoffTime)
      .select('id');
    
    if (fuelError) {
      console.error('❌ Error deleting fuel data:', fuelError.message);
    } else {
      console.log(`✅ Deleted ${deletedFuelData?.length || 0} fuel data records`);
    }
    
    console.log('\n🎉 Recent data cleanup completed!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

deleteRecentData();