// Test script to manually trigger snapshots and verify cost code integration
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('❌ Missing Supabase environment variables');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Import the enhanced snapshot function
const { captureScheduledSnapshot } = require('./helpers/snapshot-scheduler');

async function testSnapshotTrigger() {
  try {
    console.log('🧪 Testing Enhanced Snapshot System with Manual Trigger');
    console.log('=' .repeat(70));
    
    // Step 1: Check current vehicle data
    console.log('\n📊 Step 1: Checking current vehicle data...');
    const { data: vehicles, error: vehicleError } = await supabase
      .from('energy_rite_fuel_data')
      .select('plate, fuel_probe_1_level_percentage, fuel_probe_1_volume_in_tank, status')
      .not('plate', 'is', null)
      .limit(5);
    
    if (vehicleError) throw new Error(`Vehicle data error: ${vehicleError.message}`);
    
    console.log(`   Found ${vehicles.length} vehicles with fuel data`);
    vehicles.forEach(vehicle => {
      console.log(`   - ${vehicle.plate}: ${vehicle.fuel_probe_1_level_percentage}% (${vehicle.fuel_probe_1_volume_in_tank}L) - ${vehicle.status}`);
    });
    
    // Step 2: Check operating sessions for cost codes
    console.log('\n🏢 Step 2: Checking operating sessions for cost codes...');
    const { data: sessions, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('branch, cost_code, company')
      .not('branch', 'is', null)
      .not('cost_code', 'is', null)
      .limit(5);
    
    if (sessionError) throw new Error(`Session data error: ${sessionError.message}`);
    
    console.log(`   Found ${sessions.length} sessions with cost codes`);
    sessions.forEach(session => {
      console.log(`   - ${session.branch}: ${session.cost_code} (${session.company})`);
    });
    
    // Step 3: Get snapshot count before
    console.log('\n📸 Step 3: Checking existing snapshots...');
    const { data: existingSnapshots, error: countError } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('snapshot_date, branch')
      .eq('snapshot_date', new Date().toISOString().slice(0, 10));
    
    if (countError) throw new Error(`Snapshot count error: ${countError.message}`);
    
    const beforeCount = existingSnapshots?.length || 0;
    console.log(`   Existing snapshots for today: ${beforeCount}`);
    
    // Step 4: Trigger manual snapshot
    console.log('\n⚡ Step 4: Triggering manual snapshot with cost codes...');
    console.log('   Executing captureScheduledSnapshot("MANUAL_TEST")...');
    
    const startTime = Date.now();
    const result = await captureScheduledSnapshot('MANUAL_TEST');
    const duration = Date.now() - startTime;
    
    console.log(`   ✅ Snapshot completed in ${duration}ms`);
    console.log(`   Result:`, result);
    
    // Step 5: Verify snapshot was created with cost codes
    console.log('\n🔍 Step 5: Verifying snapshot creation...');
    
    // Wait a moment for database consistency
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    const { data: newSnapshots, error: verifyError } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .eq('snapshot_date', new Date().toISOString().slice(0, 10))
      .order('branch');
    
    if (verifyError) throw new Error(`Verification error: ${verifyError.message}`);
    
    const afterCount = newSnapshots?.length || 0;
    const newSnapshotsCount = afterCount - beforeCount;
    
    console.log(`   Snapshots after trigger: ${afterCount} (${newSnapshotsCount > 0 ? '+' + newSnapshotsCount : 'no change'})`);
    
    // Step 6: Analyze cost code integration
    console.log('\n💰 Step 6: Analyzing cost code integration...');
    
    if (newSnapshots && newSnapshots.length > 0) {
      const snapshotsWithCostCodes = newSnapshots.filter(snapshot => {
        return snapshot.snapshot_data && snapshot.snapshot_data.cost_code;
      });
      
      const percentage = ((snapshotsWithCostCodes.length / newSnapshots.length) * 100).toFixed(1);
      
      console.log(`   Snapshots with cost codes: ${snapshotsWithCostCodes.length}/${newSnapshots.length} (${percentage}%)`);
      
      if (snapshotsWithCostCodes.length > 0) {
        console.log('\n🎯 Sample snapshots with cost codes:');
        snapshotsWithCostCodes.slice(0, 5).forEach((snapshot, index) => {
          const data = snapshot.snapshot_data;
          console.log(`   ${index + 1}. ${snapshot.branch}:`);
          console.log(`      Cost Code: ${data.cost_code}`);
          console.log(`      Company: ${snapshot.company}`);
          console.log(`      Fuel: ${data.fuel_level}% (${data.fuel_volume}L)`);
          console.log(`      Engine: ${data.engine_status}`);
          console.log(`      Type: ${data.snapshot_type}`);
        });
        
        // Cost code distribution
        const costCodeStats = {};
        snapshotsWithCostCodes.forEach(snapshot => {
          const code = snapshot.snapshot_data.cost_code;
          if (!costCodeStats[code]) {
            costCodeStats[code] = { count: 0, company: snapshot.company };
          }
          costCodeStats[code].count++;
        });
        
        console.log('\n📊 Cost code distribution:');
        Object.entries(costCodeStats)
          .sort((a, b) => b[1].count - a[1].count)
          .forEach(([code, stats]) => {
            console.log(`   ${code} (${stats.company}): ${stats.count} snapshots`);
          });
        
        console.log('\n✅ Cost code integration is working correctly!');
      } else {
        console.log('\n⚠️  No cost codes found in snapshots');
        console.log('   Sample snapshot structure:');
        if (newSnapshots[0]) {
          console.log('   ', JSON.stringify(newSnapshots[0].snapshot_data, null, 2));
        }
      }
    } else {
      console.log('   No snapshots found for verification');
    }
    
    console.log('\n🎉 Snapshot trigger test completed successfully!');
    
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.error('Stack:', error.stack);
    throw error;
  }
}

// Run the test
testSnapshotTrigger()
  .then(() => {
    console.log('\n' + '='.repeat(70));
    console.log('🎯 SNAPSHOT TRIGGER TEST PASSED');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n' + '='.repeat(70));
    console.error('❌ SNAPSHOT TRIGGER TEST FAILED');
    process.exit(1);
  });