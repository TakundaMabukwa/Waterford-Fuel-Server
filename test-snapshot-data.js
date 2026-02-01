require('dotenv').config();
const activitySnapshots = require('./helpers/activity-snapshots');

async function testSnapshotData() {
  console.log('🧪 Testing Snapshot Data Collection\n');
  console.log('='.repeat(70));
  
  try {
    console.log('📸 Taking snapshot...\n');
    const snapshot = await activitySnapshots.takeSnapshot();
    
    if (!snapshot) {
      console.error('❌ Failed to take snapshot');
      return;
    }
    
    console.log('✅ Snapshot captured successfully!\n');
    console.log('='.repeat(70));
    console.log('📊 SNAPSHOT SUMMARY');
    console.log('='.repeat(70));
    console.log(`Date: ${snapshot.snapshot_date}`);
    console.log(`Time: ${snapshot.snapshot_time}`);
    console.log(`Time Slot: ${snapshot.time_slot} (${snapshot.time_slot_name})`);
    console.log(`Total Vehicles: ${snapshot.total_vehicles}`);
    console.log(`Active Vehicles: ${snapshot.active_vehicles}`);
    console.log(`Total Fuel Level: ${snapshot.total_fuel_level.toFixed(2)}L`);
    console.log(`Avg Fuel %: ${snapshot.average_fuel_percentage.toFixed(2)}%`);
    
    console.log('\n' + '='.repeat(70));
    console.log('🚗 SAMPLE VEHICLES (First 5)');
    console.log('='.repeat(70));
    
    snapshot.vehicles_data.slice(0, 5).forEach((vehicle, i) => {
      console.log(`\n${i + 1}. ${vehicle.branch}`);
      console.log(`   Cost Code: ${vehicle.cost_code || 'N/A'}`);
      console.log(`   Active: ${vehicle.is_active ? '✅ YES' : '❌ NO'}`);
      console.log(`   Fuel Level: ${vehicle.fuel_level.toFixed(2)}L (${vehicle.fuel_percentage.toFixed(1)}%)`);
      console.log(`   Engine Status: ${vehicle.engine_status || 'N/A'}`);
    });
    
    console.log('\n' + '='.repeat(70));
    console.log('📈 COST CODE BREAKDOWN');
    console.log('='.repeat(70));
    
    const costCodeStats = {};
    snapshot.vehicles_data.forEach(v => {
      const code = v.cost_code || 'NO_CODE';
      if (!costCodeStats[code]) {
        costCodeStats[code] = { total: 0, active: 0, fuel: 0 };
      }
      costCodeStats[code].total++;
      if (v.is_active) costCodeStats[code].active++;
      costCodeStats[code].fuel += v.fuel_level;
    });
    
    Object.entries(costCodeStats).forEach(([code, stats]) => {
      console.log(`\n${code}:`);
      console.log(`  Vehicles: ${stats.total}`);
      console.log(`  Active: ${stats.active}`);
      console.log(`  Total Fuel: ${stats.fuel.toFixed(2)}L`);
    });
    
    console.log('\n' + '='.repeat(70));
    console.log('✅ TEST COMPLETED SUCCESSFULLY');
    console.log('='.repeat(70));
    
  } catch (error) {
    console.error('\n❌ TEST FAILED:', error.message);
    console.error(error.stack);
  }
}

testSnapshotData();
