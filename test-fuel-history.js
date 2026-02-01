const pendingFuelDb = require('./pending-fuel-db');

async function testFuelHistory() {
  await pendingFuelDb.initDatabase();
  
  console.log('=== Testing Fuel History Storage ===\n');
  
  // Test 1: Check if data is being stored
  const testPlate = 'JANSENPARK';
  const now = Date.now();
  
  // Get fuel history for last hour
  const history = pendingFuelDb.getFuelHistoryBefore(testPlate, now, 50);
  
  console.log(`📊 Found ${history.length} fuel readings for ${testPlate}:`);
  history.forEach((entry, i) => {
    const age = Math.round((now - entry.timestamp) / 1000 / 60);
    console.log(`  ${i+1}. ${entry.fuel_volume}L (${entry.fuel_percentage}%) - ${age} min ago - LocTime: ${entry.loc_time}`);
  });
  
  // Test 2: Test findClosestFuelDataBefore logic
  if (history.length > 0) {
    const targetTime = history[0].timestamp - (2 * 60 * 1000); // 2 min before latest
    const closest = pendingFuelDb.getFuelHistoryBefore(testPlate, targetTime, 1);
    
    console.log(`\n🎯 Closest fuel BEFORE ${new Date(targetTime).toISOString()}:`);
    if (closest.length > 0) {
      console.log(`   ${closest[0].fuel_volume}L at ${closest[0].loc_time}`);
    } else {
      console.log('   None found');
    }
  }
  
  // Test 3: Check for zero values
  const allHistory = pendingFuelDb.getFuelHistoryBefore(testPlate, now, 100);
  const zeroValues = allHistory.filter(h => h.fuel_volume <= 0);
  console.log(`\n⚠️  Zero values in history: ${zeroValues.length}/${allHistory.length}`);
  
  pendingFuelDb.closeDatabase();
}

testFuelHistory().catch(console.error);
