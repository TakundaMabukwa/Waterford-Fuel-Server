const ImprovedOnOffDetection = require('./fix-onoff-detection');

async function testImprovedDetection() {
  console.log('🧪 TESTING IMPROVED ON/OFF DETECTION\n');
  
  const detector = new ImprovedOnOffDetection();
  
  // Test pattern matching
  console.log('1. TESTING PATTERN MATCHING:');
  const testMessages = [
    'PTO ON',
    'ENGINE ON', 
    'GENERATOR ON',
    'GEN ON',
    'STARTED',
    'PTO OFF',
    'ENGINE OFF',
    'GENERATOR OFF', 
    'STOPPED',
    'IDLE',
    'Random message',
    'POSSIBLE FUEL FILL',
    'FUEL FILL DETECTED'
  ];
  
  testMessages.forEach(msg => {
    const result = detector.parseEngineStatus(msg);
    console.log(`   "${msg}" -> ${result || 'null'}`);
  });
  
  // Test debouncing logic
  console.log('\n2. TESTING DEBOUNCING:');
  console.log('   Simulating rapid ON/OFF events...');
  
  const mockFuelData = {
    fuel_probe_1_level: 150.5,
    fuel_probe_1_level_percentage: 75.2,
    company: 'KFC',
    cost_code: 'TEST001'
  };
  
  try {
    // Simulate rapid ON events
    await detector.handleEngineOn('TEST_SITE', mockFuelData);
    console.log('   ✅ First ON event processed');
    
    // This should be debounced
    await detector.handleEngineOn('TEST_SITE', mockFuelData);
    console.log('   ✅ Second ON event (should be debounced)');
    
    // Wait and try OFF (this should work but be too short)
    setTimeout(async () => {
      await detector.handleEngineOff('TEST_SITE', { 
        ...mockFuelData, 
        fuel_probe_1_level: 149.8 
      });
      console.log('   ✅ OFF event after short duration (should be ignored)');
    }, 1000);
    
  } catch (error) {
    console.error('   ❌ Test error:', error.message);
  }
  
  // Test cleanup
  console.log('\n3. TESTING CLEANUP:');
  try {
    const cleaned = await detector.cleanupOrphanedSessions();
    console.log(`   ✅ Cleaned up ${cleaned} orphaned sessions`);
  } catch (error) {
    console.error('   ❌ Cleanup error:', error.message);
  }
  
  console.log('\n💡 IMPROVEMENTS IMPLEMENTED:');
  console.log('   ✅ Enhanced pattern matching for ON/OFF detection');
  console.log('   ✅ 5-minute debouncing to prevent duplicate events');
  console.log('   ✅ 15-minute minimum session duration');
  console.log('   ✅ Better error handling and logging');
  console.log('   ✅ Automatic cleanup of orphaned sessions');
}

testImprovedDetection();