require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugActivityLogging() {
  console.log('🔍 DEBUGGING ACTIVITY LOGGING\n');
  
  try {
    // Test direct activity log insertion
    console.log('1. TESTING DIRECT ACTIVITY LOG INSERT:');
    const { data, error } = await supabase
      .from('energy_rite_activity_log')
      .insert({
        activity_type: 'ENGINE_ON',
        description: 'Test engine ON for GERMISTON',
        branch: 'GERMISTON'
      })
      .select();
    
    if (error) {
      console.error('   ❌ Insert failed:', error);
    } else {
      console.log('   ✅ Activity log inserted successfully:', data[0].id);
    }
    
    // Check if we can read it back
    console.log('\n2. CHECKING IF LOG WAS SAVED:');
    const { data: logs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .eq('branch', 'GERMISTON')
      .gte('created_at', new Date(Date.now() - 60000).toISOString())
      .order('created_at', { ascending: false });
    
    console.log(`   Found ${logs?.length || 0} recent logs for GERMISTON`);
    logs?.forEach(log => {
      console.log(`     ${log.activity_type}: ${log.created_at}`);
    });
    
  } catch (error) {
    console.error('❌ Debug failed:', error);
  }
}

debugActivityLogging();