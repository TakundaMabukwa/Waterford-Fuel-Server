require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugFuelDetection() {
  try {
    console.log('🔍 Debugging fuel fill detection...\n');
    
    // Check recent fuel data
    const { data: recentFuelData } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(10);
    
    console.log('📊 Recent fuel data entries:');
    recentFuelData?.forEach((entry, i) => {
      console.log(`${i + 1}. ${entry.plate} - ${entry.fuel_probe_1_level}L (${entry.fuel_probe_1_level_percentage}%) - ${entry.created_at}`);
    });
    
    // Check recent activity logs for fuel fills
    const { data: activityLogs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .eq('activity_type', 'FUEL_FILL_SESSION_START')
      .order('activity_time', { ascending: false })
      .limit(5);
    
    console.log('\n🔋 Recent fuel fill sessions:');
    activityLogs?.forEach((log, i) => {
      console.log(`${i + 1}. ${log.branch} - ${log.notes} - ${log.activity_time}`);
    });
    
    // Check detected fuel fills
    const { data: detectedFills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .order('fill_date', { ascending: false })
      .limit(5);
    
    console.log('\n⛽ Recent detected fuel fills:');
    detectedFills?.forEach((fill, i) => {
      console.log(`${i + 1}. ${fill.plate} - +${fill.fill_amount}L (${fill.fuel_before}L → ${fill.fuel_after}L) - ${fill.fill_date}`);
    });
    
  } catch (error) {
    console.error('❌ Debug error:', error);
  }
}

debugFuelDetection();