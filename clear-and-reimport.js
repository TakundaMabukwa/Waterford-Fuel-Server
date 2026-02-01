require('dotenv').config();
const { supabase } = require('./supabase-client');

async function clearAndReimport() {
  try {
    console.log('🗑️ Clearing existing sessions...');
    
    const { error: deleteError } = await supabase
      .from('energy_rite_operating_sessions')
      .delete()
      .neq('id', 0);

    if (deleteError) throw deleteError;
    
    console.log('✅ Cleared existing data');
    console.log('🔄 Starting reimport...');
    
    // Run the accurate import with times
    require('./import-monthly-accurate-times.js');
    
  } catch (error) {
    console.error('❌ Clear failed:', error.message);
  }
}

clearAndReimport();