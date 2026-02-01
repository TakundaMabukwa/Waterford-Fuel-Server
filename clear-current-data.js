const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);

async function clearCurrentData() {
  try {
    console.log('🗑️ Clearing current data...\n');
    
    // Delete from activity log first (foreign key dependency)
    console.log('Deleting activity log entries...');
    const { error: activityError } = await supabase
      .from('energy_rite_activity_log')
      .delete()
      .neq('id', 0); // Delete all records
    
    if (activityError) {
      console.error('Error deleting activity log:', activityError);
      return;
    }
    
    // Delete from operating sessions
    console.log('Deleting operating sessions...');
    const { error: sessionsError } = await supabase
      .from('energy_rite_operating_sessions')
      .delete()
      .neq('id', 0); // Delete all records
    
    if (sessionsError) {
      console.error('Error deleting sessions:', sessionsError);
      return;
    }
    
    console.log('✅ All current data cleared successfully');
    console.log('Ready for fresh historical data import');
    
  } catch (error) {
    console.error('❌ Error clearing data:', error);
  }
}

clearCurrentData();