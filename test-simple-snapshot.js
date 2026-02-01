// Simple test to trigger snapshots using existing client
require('dotenv').config();
const { supabase } = require('./supabase-client');

// Import the enhanced snapshot function
const { captureScheduledSnapshot } = require('./helpers/snapshot-scheduler');

async function testSnapshot() {
  try {
    console.log('🧪 Testing Enhanced Snapshot Trigger');
    console.log('=' .repeat(50));
    
    console.log('\n📊 Checking current system status...');
    
    // Check if we have any fuel data
    const { data: fuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('plate, fuel_probe_1_level_percentage, status')
      .not('plate', 'is', null)
      .limit(3);
    
    if (fuelError) {
      console.log('   ⚠️  Fuel data check failed:', fuelError.message);
    } else {
      console.log(`   ✅ Found ${fuelData.length} vehicles with fuel data`);
      fuelData.forEach(vehicle => {
        console.log(`      - ${vehicle.plate}: ${vehicle.fuel_probe_1_level_percentage}% ${vehicle.status}`);
      });
    }
    
    // Check operating sessions
    const { data: sessions, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('branch, cost_code, company')
      .not('cost_code', 'is', null)
      .limit(3);
    
    if (sessionError) {
      console.log('   ⚠️  Session data check failed:', sessionError.message);
    } else {
      console.log(`   ✅ Found ${sessions.length} sessions with cost codes`);
      sessions.forEach(session => {
        console.log(`      - ${session.branch}: ${session.cost_code} (${session.company})`);
      });
    }
    
    console.log('\n⚡ Triggering manual snapshot...');
    
    try {
      const startTime = Date.now();
      const result = await captureScheduledSnapshot('MANUAL_TEST');
      const duration = Date.now() - startTime;
      
      console.log(`   ✅ Snapshot completed in ${duration}ms`);
      if (result) {
        console.log('   Result:', typeof result === 'object' ? JSON.stringify(result, null, 2) : result);
      }
      
      // Wait a moment then check results
      console.log('\n🔍 Checking snapshot results...');
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      const today = new Date().toISOString().slice(0, 10);
      const { data: snapshots, error: snapError } = await supabase
        .from('energy_rite_daily_snapshots')
        .select('branch, company, snapshot_data')
        .eq('snapshot_date', today)
        .order('branch')
        .limit(10);
      
      if (snapError) {
        console.log('   ⚠️  Snapshot verification failed:', snapError.message);
      } else {
        console.log(`   📸 Found ${snapshots.length} snapshots for today`);
        
        const withCostCodes = snapshots.filter(s => s.snapshot_data?.cost_code);
        console.log(`   💰 ${withCostCodes.length} snapshots have cost codes`);
        
        if (withCostCodes.length > 0) {
          console.log('\n🎯 Sample snapshots with cost codes:');
          withCostCodes.slice(0, 5).forEach((snapshot, i) => {
            const data = snapshot.snapshot_data;
            console.log(`      ${i+1}. ${snapshot.branch}:`);
            console.log(`         Cost Code: ${data.cost_code}`);
            console.log(`         Company: ${snapshot.company}`);
            console.log(`         Fuel: ${data.fuel_level}% (${data.fuel_volume}L)`);
            console.log(`         Engine: ${data.engine_status}`);
          });
          
          console.log('\n✅ Cost code integration is working correctly!');
        } else {
          console.log('\n⚠️  No cost codes found in snapshots');
          if (snapshots.length > 0) {
            console.log('   Sample snapshot data:', JSON.stringify(snapshots[0].snapshot_data, null, 2));
          }
        }
      }
      
    } catch (snapError) {
      console.error('   ❌ Snapshot trigger failed:', snapError.message);
      console.error('   Stack:', snapError.stack);
    }
    
    console.log('\n🎉 Test completed!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error('Stack:', error.stack);
  }
}

// Run the test
testSnapshot()
  .then(() => {
    console.log('\n' + '='.repeat(50));
    console.log('🎯 Snapshot trigger test finished');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Error:', error);
    process.exit(1);
  });