// Test the corrected schema fix
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testCorrectedFix() {
  try {
    console.log('🔧 Testing Corrected Schema Fix');
    console.log('=' .repeat(45));
    
    console.log('\n📋 Testing after applying the corrected migration...');
    console.log('   (Apply minimal-schema-fix.sql for safest results)');
    
    // Test the basic structure first
    console.log('\n🧪 Step 1: Testing basic insert without complex features...');
    
    const basicTest = {
      branch: 'CORRECTED_TEST',
      company: 'TEST_COMPANY',
      snapshot_date: new Date().toISOString().slice(0, 10),
      snapshot_time: new Date().toISOString(),
      snapshot_type: 'CORRECTED_TEST'
    };
    
    try {
      const { data: basicData, error: basicError } = await supabase
        .from('energy_rite_daily_snapshots')
        .insert([basicTest])
        .select();
      
      if (basicError) {
        console.log('   ❌ Basic insert failed:', basicError.message);
      } else {
        console.log('   ✅ Basic insert successful!');
        console.log('   📊 Columns available:', Object.keys(basicData[0]));
        
        // Now test with snapshot_data
        if (basicData[0].snapshot_data !== undefined) {
          console.log('   ✅ snapshot_data column exists!');
          
          // Update with cost code data
          const { data: updateData, error: updateError } = await supabase
            .from('energy_rite_daily_snapshots')
            .update({
              snapshot_data: {
                cost_code: 'TEST-CORRECTED-001',
                fuel_level: 82.3,
                fuel_volume: 198.7,
                engine_status: 'OFF',
                test_status: 'corrected_schema_working'
              }
            })
            .eq('id', basicData[0].id)
            .select();
          
          if (updateError) {
            console.log('   ❌ snapshot_data update failed:', updateError.message);
          } else {
            console.log('   ✅ snapshot_data update successful!');
            console.log('   💰 Cost code stored:', updateData[0].snapshot_data.cost_code);
            console.log('   ⛽ Fuel data stored:', updateData[0].snapshot_data.fuel_level + '%');
          }
        } else {
          console.log('   ⚠️  snapshot_data column still missing - apply the migration');
        }
        
        // Clean up
        await supabase
          .from('energy_rite_daily_snapshots')
          .delete()
          .eq('branch', 'CORRECTED_TEST');
          
        console.log('   🧹 Test data cleaned up');
      }
    } catch (e) {
      console.log('   ❌ Basic test exception:', e.message);
    }
    
    console.log('\n🧪 Step 2: Testing enhanced snapshot function...');
    
    try {
      const { captureScheduledSnapshot } = require('./helpers/snapshot-scheduler');
      
      console.log('   Triggering enhanced snapshot...');
      const result = await captureScheduledSnapshot('CORRECTED_SCHEMA_TEST');
      
      console.log('   📊 Result:', JSON.stringify(result, null, 2));
      
      if (result.vehicleCount > 0) {
        console.log('   🎉 SUCCESS: Cost code integration working!');
      } else {
        console.log('   📋 No vehicles processed (expected with current fuel data)');
        console.log('   ✅ Enhanced function is ready for when fuel data arrives');
      }
      
    } catch (snapError) {
      console.log('   ❌ Snapshot error:', snapError.message);
      
      if (snapError.message.includes('snapshot_data')) {
        console.log('   💡 Apply minimal-schema-fix.sql to resolve this');
      }
    }
    
    console.log('\n📄 Available Migration Files:');
    console.log('=' .repeat(35));
    console.log('   1. ✅ minimal-schema-fix.sql (RECOMMENDED - safest)');
    console.log('      - Adds snapshot_data column');
    console.log('      - Basic indexes only');
    console.log('      - No complex GIN operations');
    console.log('');
    console.log('   2. 🔧 final-schema-fix.sql (advanced)');
    console.log('      - Includes B-tree index for cost codes');
    console.log('      - GIN index for JSONB queries');
    console.log('      - More performance optimized');
    
    console.log('\n🎯 Next Steps:');
    console.log('   1. Apply minimal-schema-fix.sql in Supabase SQL Editor');
    console.log('   2. Run this test again to verify');
    console.log('   3. Monitor automated snapshots with cost codes');
    console.log('   4. Enjoy enhanced fuel tracking with cost integration!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

// Run the corrected test
testCorrectedFix()
  .then(() => {
    console.log('\n' + '='.repeat(45));
    console.log('🎯 Corrected schema fix test completed');
    console.log('📄 Use minimal-schema-fix.sql for best results');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Error:', error);
    process.exit(1);
  });