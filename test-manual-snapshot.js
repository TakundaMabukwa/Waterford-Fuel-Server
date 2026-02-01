// Test script to manually trigger snapshot capture with cost codes
const fetch = require('node-fetch');

async function testManualSnapshot() {
  try {
    console.log('🧪 Testing Manual Snapshot Capture with Cost Codes');
    console.log('=' .repeat(60));
    
    // Manually call the enhanced snapshot function
    console.log('📸 Triggering manual snapshot capture...');
    
    // Import the enhanced snapshot scheduler
    const path = require('path');
    const snapshotPath = path.join(__dirname, 'helpers', 'snapshot-scheduler.js');
    
    // Check if the file exists and is accessible
    const fs = require('fs');
    if (!fs.existsSync(snapshotPath)) {
      throw new Error(`Snapshot scheduler not found at: ${snapshotPath}`);
    }
    
    console.log('🔧 Loading enhanced snapshot scheduler...');
    const { captureScheduledSnapshot } = require(snapshotPath);
    
    if (typeof captureScheduledSnapshot !== 'function') {
      throw new Error('captureScheduledSnapshot is not a function');
    }
    
    console.log('⚡ Executing enhanced snapshot capture...');
    const result = await captureScheduledSnapshot('manual_test');
    
    console.log('✅ Manual snapshot capture completed!');
    console.log('Result:', result);
    
    console.log('\n🎯 The enhanced snapshot should now include:');
    console.log('   ✓ Site mapping from energy_rite_operating_sessions');
    console.log('   ✓ Cost codes for each vehicle');
    console.log('   ✓ Fuel levels and engine status');
    console.log('   ✓ Proper JSONB structure in snapshot_data');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error('Stack:', error.stack);
    throw error;
  }
}

// Run the test
testManualSnapshot()
  .then(() => {
    console.log('\n🎉 Manual snapshot test completed!');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Test failed:', error);
    process.exit(1);
  });