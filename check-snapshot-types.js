// Load environment variables first
require('dotenv').config();

const { supabase } = require('./supabase-client');

async function checkValidSnapshotTypes() {
  console.log('🔍 CHECKING VALID SNAPSHOT TYPES...');
  
  try {
    // Check existing snapshot types
    const { data: existingSnapshots, error } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('snapshot_type')
      .limit(50);

    if (error) {
      console.log('❌ Error fetching snapshots:', error.message);
      return;
    }

    // Get unique snapshot types
    const uniqueTypes = [...new Set(existingSnapshots.map(s => s.snapshot_type))];
    console.log('📋 Existing snapshot types in database:');
    uniqueTypes.forEach(type => console.log(`   - ${type}`));

    // Try to insert with standard types
    console.log('\n🧪 Testing standard snapshot types...');
    
    const testTypes = ['MORNING', 'MIDDAY', 'EVENING', 'MANUAL', 'SCHEDULED'];
    
    for (const testType of testTypes) {
      try {
        const { error: insertError } = await supabase
          .from('energy_rite_daily_snapshots')
          .insert({
            snapshot_date: new Date().toISOString().split('T')[0],
            snapshot_time: new Date().toISOString(),
            snapshot_type: testType,
            branch: 'TEST_BRANCH',
            company: 'TEST_COMPANY',
            snapshot_data: {
              cost_code: 'TEST-001',
              fuel_level: 50.0,
              fuel_volume: 100.0,
              engine_status: 'OFF',
              test_type: testType
            }
          });

        if (insertError) {
          console.log(`❌ ${testType}: ${insertError.message}`);
        } else {
          console.log(`✅ ${testType}: VALID`);
          
          // Clean up the test record
          await supabase
            .from('energy_rite_daily_snapshots')
            .delete()
            .eq('branch', 'TEST_BRANCH')
            .eq('snapshot_type', testType);
        }
      } catch (error) {
        console.log(`❌ ${testType}: ${error.message}`);
      }
    }

  } catch (error) {
    console.error('❌ Check failed:', error.message);
  }
}

checkValidSnapshotTypes();