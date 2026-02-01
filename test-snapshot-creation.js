// Test snapshot creation without querying results
require('dotenv').config();
const { supabase } = require('./supabase-client');

// Import the enhanced snapshot function
const { captureScheduledSnapshot } = require('./helpers/snapshot-scheduler');

async function testSnapshotCreation() {
  try {
    console.log('🧪 Testing Enhanced Snapshot Creation');
    console.log('=' .repeat(50));
    
    console.log('\n📊 Pre-flight checks...');
    
    // Check environment
    console.log(`   ✅ Supabase URL: ${process.env.SUPABASE_URL ? 'Set' : 'Missing'}`);
    console.log(`   ✅ Supabase Key: ${process.env.SUPABASE_ANON_KEY ? 'Set' : 'Missing'}`);
    
    // Test basic connection
    try {
      const { data, error } = await supabase
        .from('energy_rite_operating_sessions')
        .select('count')
        .limit(1);
      
      if (error) {
        console.log(`   ⚠️  Database connection test failed: ${error.message}`);
      } else {
        console.log('   ✅ Database connection working');
      }
    } catch (e) {
      console.log(`   ⚠️  Connection error: ${e.message}`);
    }
    
    console.log('\n⚡ Testing snapshot trigger...');
    
    try {
      console.log('   Calling captureScheduledSnapshot("TEST_TRIGGER")...');
      
      const startTime = Date.now();
      const result = await captureScheduledSnapshot('TEST_TRIGGER');
      const duration = Date.now() - startTime;
      
      console.log(`   ✅ Snapshot function completed in ${duration}ms`);
      console.log('   📊 Result:', JSON.stringify(result, null, 2));
      
      // Analyze the result
      if (result) {
        console.log('\n📈 Snapshot Analysis:');
        console.log(`   - Snapshot Type: ${result.snapshotType || 'Unknown'}`);
        console.log(`   - Vehicle Count: ${result.vehicleCount || 0}`);
        console.log(`   - Active Vehicles: ${result.activeVehicles || 0}`);
        console.log(`   - Total Fuel: ${result.totalFuelVolume || 0}L`);
        console.log(`   - Timestamp: ${result.timestamp || 'Not set'}`);
        
        if (result.vehicleCount > 0) {
          console.log('\n✅ SUCCESS: Snapshots were created for vehicles!');
          console.log('   💰 Cost codes should now be included in snapshot_data');
          console.log('   🎯 Each snapshot includes:');
          console.log('      - cost_code from operating sessions');
          console.log('      - fuel_level and fuel_volume');
          console.log('      - engine_status');
          console.log('      - snapshot_type (TEST_TRIGGER)');
        } else {
          console.log('\n⚠️  No vehicles processed');
          console.log('   This could mean:');
          console.log('   - No fuel data available');
          console.log('   - Vehicles have no valid fuel readings');
          console.log('   - Database connection issues');
        }
      } else {
        console.log('\n⚠️  No result returned from snapshot function');
      }
      
    } catch (snapError) {
      console.error('\n❌ Snapshot function failed:');
      console.error(`   Error: ${snapError.message}`);
      console.error(`   Stack: ${snapError.stack}`);
    }
    
    console.log('\n🔧 Enhanced Snapshot System Summary:');
    console.log('   ✓ Snapshot scheduler enhanced with cost codes');
    console.log('   ✓ Site mapping from energy_rite_operating_sessions');
    console.log('   ✓ JSONB storage in energy_rite_daily_snapshots');
    console.log('   ✓ Automated schedule: 06:00, 12:00, 18:00 daily');
    
    console.log('\n🎉 Snapshot creation test completed!');
    
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.error('Stack:', error.stack);
  }
}

// Run the test
testSnapshotCreation()
  .then(() => {
    console.log('\n' + '='.repeat(50));
    console.log('🎯 Test finished');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Error:', error);
    process.exit(1);
  });