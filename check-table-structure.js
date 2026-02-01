require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkTableStructure() {
  console.log('🔍 CHECKING ACTUAL TABLE STRUCTURES\n');
  
  try {
    // Check what tables exist
    console.log('1. CHECKING AVAILABLE TABLES:');
    const { data: tables, error } = await supabase
      .rpc('get_table_columns', { table_name: 'energy_rite_activity_log' })
      .catch(() => null);
    
    // Try a simple select to see what columns exist
    console.log('2. TESTING SIMPLE SELECT:');
    const { data, error: selectError } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .limit(1);
    
    if (selectError) {
      console.log('   Table structure issue:', selectError.message);
      
      // Try with minimal columns
      console.log('3. TESTING MINIMAL INSERT:');
      const { data: insertData, error: insertError } = await supabase
        .from('energy_rite_activity_log')
        .insert({
          activity_type: 'TEST'
        })
        .select();
      
      if (insertError) {
        console.log('   Minimal insert failed:', insertError.message);
      } else {
        console.log('   ✅ Minimal insert worked');
      }
    } else {
      console.log('   ✅ Table accessible, found', data?.length || 0, 'records');
    }
    
  } catch (error) {
    console.error('❌ Check failed:', error);
  }
}

checkTableStructure();