require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkFuelFillLogs() {
  try {
    console.log('📋 Checking fuel fill activity logs...\n');
    
    // Get fuel fill activities
    const { data: activities, error } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .eq('activity_type', 'FUEL_FILL')
      .order('created_at', { ascending: false })
      .limit(10);
    
    if (error) {
      console.error('❌ Error:', error);
      return;
    }
    
    console.log(`Found ${activities.length} fuel fill activities:\n`);
    
    activities.forEach((activity, index) => {
      console.log(`⛽ Fuel Fill ${index + 1}:`);
      console.log(`   Branch: ${activity.branch}`);
      console.log(`   Description: ${activity.description}`);
      console.log(`   Created: ${activity.created_at}`);
      
      if (activity.activity_data) {
        const data = activity.activity_data;
        console.log(`   Previous Fuel: ${data.previous_fuel}L`);
        console.log(`   Current Fuel: ${data.current_fuel}L`);
        console.log(`   Fill Amount: ${data.fill_amount}L`);
      }
      console.log('');
    });
    
    // Check if any sessions were updated with fill data
    console.log('📊 Checking sessions with fill data...\n');
    
    const { data: sessions, error: sessionsError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .not('total_fill', 'is', null)
      .gt('total_fill', 0)
      .order('created_at', { ascending: false });
    
    if (sessionsError) {
      console.error('❌ Sessions error:', sessionsError);
      return;
    }
    
    console.log(`Found ${sessions.length} sessions with fill data:\n`);
    
    sessions.forEach((session, index) => {
      console.log(`🔋 Session ${index + 1}:`);
      console.log(`   Branch: ${session.branch}`);
      console.log(`   Total Fill: ${session.total_fill}L`);
      console.log(`   Notes: ${session.notes}`);
      console.log('');
    });
    
  } catch (error) {
    console.error('❌ Check error:', error);
  }
}

checkFuelFillLogs();