/**
 * Test script for stabilization-based passive fill detection
 * Tests the new fuel_history table and last_increased_at tracking
 */

const pendingFuelDb = require('./pending-fuel-db');

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTest() {
  console.log('🧪 Testing Stabilization-Based Fill Detection');
  console.log('=' .repeat(60));
  
  // Initialize database
  await pendingFuelDb.initDatabase();
  console.log('✅ Database initialized with new schema\n');
  
  // Test 1: Fuel History Storage
  console.log('📊 Test 1: Fuel History Storage');
  console.log('-'.repeat(40));
  
  const plate = 'TEST-001';
  const now = Date.now();
  
  // Store some fuel history records
  await pendingFuelDb.storeFuelHistory(plate, 100, 20, 'site1', now - 60000);  // 1 min ago
  await pendingFuelDb.storeFuelHistory(plate, 150, 30, 'site1', now - 45000);  // 45 sec ago  
  await pendingFuelDb.storeFuelHistory(plate, 200, 40, 'site1', now - 30000);  // 30 sec ago
  await pendingFuelDb.storeFuelHistory(plate, 250, 50, 'site1', now - 15000);  // 15 sec ago
  await pendingFuelDb.storeFuelHistory(plate, 300, 60, 'site1', now);          // now
  
  console.log('  Stored 5 fuel history records');
  
  // Get fuel history before specific time
  const historyBefore = await pendingFuelDb.getFuelHistoryBefore(plate, now - 20000);
  console.log(`  History before 20s ago: ${historyBefore?.fuel_volume || 'none'}L (expected: 200L)`);
  
  // Get fuel history after specific time
  const historyAfter = await pendingFuelDb.getFuelHistoryAfter(plate, now - 40000);
  console.log(`  History after 40s ago: ${historyAfter?.fuel_volume || 'none'}L (expected: 200L)`);
  
  // Get lowest in range
  const lowest = await pendingFuelDb.getLowestFuelInRange(plate, now - 60000, now);
  console.log(`  Lowest in 1 min range: ${lowest?.fuel_volume || 'none'}L (expected: 100L)`);
  
  console.log('');
  
  // Test 2: Fill Watcher with Stabilization
  console.log('📊 Test 2: Fill Watcher Stabilization Tracking');
  console.log('-'.repeat(40));
  
  const fillPlate = 'FILL-001';
  const startTime = new Date(Date.now() - 180000).toISOString(); // started 3 min ago
  const startLocTime = 'site1';
  
  // Create a fill watcher using the proper API
  await pendingFuelDb.setFuelFillWatcher(fillPlate, {
    startTime: startTime,
    startLocTime: startLocTime,
    openingFuel: 100,
    openingPercentage: 20,
    highestFuel: 200,
    highestPercentage: 40,
    highestLocTime: 'site1'
  });
  console.log('  Created fill watcher for FILL-001 (100L → 200L)');
  
  // Get initial watcher state
  let watcher = await pendingFuelDb.getFuelFillWatcher(fillPlate);
  console.log(`  Initial state: highest_fuel=${watcher.highest_fuel}L, last_increased_at=${new Date(watcher.last_increased_at).toLocaleTimeString()}`);
  
  // Simulate fuel increasing
  await pendingFuelDb.updateFuelFillWatcherHighest(fillPlate, 250, 50, 'site1');
  watcher = await pendingFuelDb.getFuelFillWatcher(fillPlate);
  console.log(`  After increase: highest_fuel=${watcher.highest_fuel}L (last_increased_at updated)`);
  
  // Check for stabilized watchers (none should be found - just increased)
  const notStabilized = await pendingFuelDb.getStabilizedFuelFillWatchers(120000); // 2 min threshold
  console.log(`  Stabilized watchers (2min threshold): ${notStabilized.length} (expected: 0)`);
  
  console.log('');
  
  // Test 3: Simulate Fill Stabilization
  console.log('📊 Test 3: Simulate Fill Completion via Stabilization');
  console.log('-'.repeat(40));
  
  const stablePlate = 'STABLE-001';
  const oldTime = Date.now() - 180000; // 3 min ago
  
  // Create a watcher with old timestamps
  await pendingFuelDb.setFuelFillWatcher(stablePlate, {
    startTime: new Date(oldTime).toISOString(),
    startLocTime: 'Test Site',
    openingFuel: 100,
    openingPercentage: 20,
    highestFuel: 300,
    highestPercentage: 60,
    highestLocTime: 'Test Site'
  });
  
  // Manually set last_increased_at to 3 minutes ago to simulate stabilization
  // (In production this happens naturally when fuel stops increasing)
  const db = pendingFuelDb.getDb();
  if (db) {
    const stmt = db.prepare(`
      UPDATE fuel_fill_watchers 
      SET last_increased_at = ? 
      WHERE plate = ?
    `);
    stmt.run([oldTime, stablePlate]);
    stmt.free();
  }
  
  console.log('  Created watcher with fuel stable for 3 minutes');
  
  // Now check for stabilized watchers (should find one)
  const stabilized = await pendingFuelDb.getStabilizedFuelFillWatchers(120000); // 2 min threshold
  console.log(`  Stabilized watchers (2min threshold): ${stabilized.length} (expected: 1)`);
  
  if (stabilized.length > 0) {
    const w = stabilized[0];
    console.log(`  Found: ${w.plate}`);
    console.log(`    Opening fuel: ${w.opening_fuel}L → Highest fuel: ${w.highest_fuel}L`);
    console.log(`    Fill amount: ${w.highest_fuel - w.opening_fuel}L`);
    console.log(`    Last increased: ${new Date(w.last_increased_at).toLocaleTimeString()}`);
    console.log(`    Stable for: ${Math.round((Date.now() - w.last_increased_at) / 1000)}s`);
  }
  
  console.log('');
  
  // Cleanup
  console.log('🧹 Cleanup');
  console.log('-'.repeat(40));
  
  // Remove test watchers
  await pendingFuelDb.deleteFuelFillWatcher(fillPlate);
  await pendingFuelDb.deleteFuelFillWatcher(stablePlate);
  
  // Cleanup old fuel history
  await pendingFuelDb.cleanupOldFuelHistory(60 * 60 * 1000); // 1 hour
  
  console.log('  Removed test watchers and cleaned old history');
  
  console.log('');
  console.log('=' .repeat(60));
  console.log('✅ All tests completed!');
  console.log('');
  console.log('📋 Summary of New Features:');
  console.log('  1. fuel_history table stores all fuel readings');
  console.log('  2. last_increased_at tracks when fuel last went up');
  console.log('  3. getStabilizedFuelFillWatchers() finds fills that stopped increasing');
  console.log('  4. Fills complete when fuel is stable for 2+ minutes');
  
  // Close database
  await pendingFuelDb.closeDatabase();
}

runTest().catch(err => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
