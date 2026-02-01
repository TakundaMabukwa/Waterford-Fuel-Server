require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkDataTables() {
  try {
    console.log('🔍 Checking data in tables used for reports...\n');
    
    // Check energy_rite_operating_sessions table
    console.log('📊 Checking energy_rite_operating_sessions table:');
    const { data: sessions, error: sessionsError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .limit(5);
    
    if (sessionsError) {
      console.error('❌ Sessions error:', sessionsError);
    } else {
      console.log(`   Found ${sessions.length} sessions`);
      if (sessions.length > 0) {
        console.log('   Sample session:', JSON.stringify(sessions[0], null, 2));
      } else {
        console.log('   ⚠️  No sessions found - this is why reports show 0 data');
      }
    }
    
    // Check energy_rite_fuel_data table (real-time data)
    console.log('\n📊 Checking energy_rite_fuel_data table:');
    const { data: fuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .limit(5);
    
    if (fuelError) {
      console.error('❌ Fuel data error:', fuelError);
    } else {
      console.log(`   Found ${fuelData.length} fuel records`);
      if (fuelData.length > 0) {
        console.log('   Sample fuel data:', JSON.stringify(fuelData[0], null, 2));
      }
    }
    
    // Check all tables to see what data exists
    console.log('\n📋 Checking all EnergyRite tables:');
    const tables = [
      'energy_rite_fuel_data',
      'energy_rite_operating_sessions', 
      'energy_rite_activity_log',
      'energy_rite_fuel_anomalies',
      'energy_rite_daily_snapshots'
    ];
    
    for (const table of tables) {
      try {
        const { count, error } = await supabase
          .from(table)
          .select('*', { count: 'exact', head: true });
        
        if (error) {
          console.log(`   ${table}: ❌ Error - ${error.message}`);
        } else {
          console.log(`   ${table}: ${count || 0} records`);
        }
      } catch (e) {
        console.log(`   ${table}: ❌ Table doesn't exist or access denied`);
      }
    }
    
  } catch (error) {
    console.error('❌ Check error:', error);
  }
}

checkDataTables();