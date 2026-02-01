const { supabase } = require('./supabase-client');
const { captureScheduledSnapshot } = require('./helpers/snapshot-scheduler');

async function testSnapshotWithCostCode() {
  try {
    console.log('🧪 Testing Enhanced Snapshot System with Cost Code');
    console.log('=' .repeat(60));
    
    // Test capturing a snapshot
    console.log('📸 Capturing test snapshot...');
    const result = await captureScheduledSnapshot('TEST');
    
    console.log('✅ Snapshot captured successfully!');
    console.log(`   📊 Vehicles: ${result.vehicleCount}`);
    console.log(`   🟢 Active: ${result.activeVehicles}`);
    console.log(`   ⛽ Total Fuel: ${result.totalFuelVolume.toFixed(1)}L`);
    
    // Check the captured snapshot data
    console.log('\n🔍 Checking captured snapshot data...');
    const { data: snapshots, error } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .eq('snapshot_date', new Date().toISOString().slice(0, 10))
      .limit(5);
    
    if (error) throw error;
    
    console.log(`\n📋 Found ${snapshots.length} snapshots for today:`);
    snapshots.forEach((snapshot, index) => {
      const data = snapshot.snapshot_data;
      console.log(`\n   ${index + 1}. Site: ${snapshot.branch}`);
      console.log(`      Company: ${snapshot.company}`);
      console.log(`      Cost Code: ${data.cost_code || 'Not assigned'}`);
      console.log(`      Fuel: ${data.fuel_percentage}% (${data.fuel_volume}L)`);
      console.log(`      Engine: ${data.engine_status}`);
      console.log(`      Snapshot Type: ${data.snapshot_type}`);
      console.log(`      Time: ${data.snapshot_time}`);
    });
    
    // Check cost code distribution
    const costCodes = {};
    snapshots.forEach(snapshot => {
      const costCode = snapshot.snapshot_data.cost_code || 'Unassigned';
      if (!costCodes[costCode]) {
        costCodes[costCode] = 0;
      }
      costCodes[costCode]++;
    });
    
    console.log('\n💰 Cost Code Distribution:');
    Object.entries(costCodes).forEach(([code, count]) => {
      console.log(`   ${code}: ${count} sites`);
    });
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    throw error;
  }
}

// Run the test
testSnapshotWithCostCode()
  .then(() => {
    console.log('\n✅ Test completed successfully!');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Test failed:', error);
    process.exit(1);
  });