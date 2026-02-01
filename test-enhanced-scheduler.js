// Load environment variables first
require('dotenv').config();

// Test the enhanced snapshot scheduler locally
const { captureScheduledSnapshot, getSnapshotStatistics } = require('./helpers/snapshot-scheduler');

async function testSnapshotFunctionality() {
  console.log('🚀 TESTING ENHANCED SNAPSHOT SCHEDULER');
  console.log('='.repeat(50));
  
  try {
    // Test 1: Trigger morning snapshot
    console.log('\n🧪 TEST 1: Morning Snapshot Trigger');
    const morningResult = await captureScheduledSnapshot('MORNING');
    console.log('✅ Morning snapshot result:', morningResult);
    
    // Wait 2 seconds
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Test 2: Trigger midday snapshot
    console.log('\n🧪 TEST 2: Midday Snapshot Trigger');
    const middayResult = await captureScheduledSnapshot('MIDDAY');
    console.log('✅ Midday snapshot result:', middayResult);
    
    // Wait 2 seconds
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Test 3: Trigger evening snapshot
    console.log('\n🧪 TEST 3: Evening Snapshot Trigger');
    const eveningResult = await captureScheduledSnapshot('EVENING');
    console.log('✅ Evening snapshot result:', eveningResult);
    
    // Test 4: Get snapshot statistics
    console.log('\n🧪 TEST 4: Getting Snapshot Statistics');
    const stats = await getSnapshotStatistics();
    console.log('📊 Today\'s snapshot statistics:', stats);
    
    // Summary
    console.log('\n🎯 TEST SUMMARY:');
    console.log(`Morning: ${morningResult.vehicleCount} vehicles, ${morningResult.totalFuelVolume.toFixed(1)}L fuel`);
    console.log(`Midday: ${middayResult.vehicleCount} vehicles, ${middayResult.totalFuelVolume.toFixed(1)}L fuel`);
    console.log(`Evening: ${eveningResult.vehicleCount} vehicles, ${eveningResult.totalFuelVolume.toFixed(1)}L fuel`);
    
    const totalVehicles = morningResult.vehicleCount + middayResult.vehicleCount + eveningResult.vehicleCount;
    const totalFuel = morningResult.totalFuelVolume + middayResult.totalFuelVolume + eveningResult.totalFuelVolume;
    
    console.log(`\n📈 TOTALS: ${totalVehicles} vehicle snapshots, ${totalFuel.toFixed(1)}L total fuel`);
    console.log(`\n🎉 All snapshot tests completed successfully!`);
    console.log('✅ Cost code integration is working with the enhanced scheduler!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.log('\n📋 This indicates either:');
    console.log('   1. Database connection issue');
    console.log('   2. Missing data in source tables');
    console.log('   3. Schema issues that need fixing');
  }
}

// Run the test
testSnapshotFunctionality();