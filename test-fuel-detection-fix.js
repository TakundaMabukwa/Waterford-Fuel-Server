require('dotenv').config();
const { detectFuelFill } = require('./helpers/fuel-fill-detector');

async function testFuelDetection() {
  try {
    console.log('🧪 Testing fixed fuel detection system...\n');
    
    // Test 1: Status-based detection
    console.log('Test 1: Status-based detection');
    const result1 = await detectFuelFill('TEST001', 500, 'Possible Fuel Fill');
    console.log('Result:', result1.isFill ? '✅ DETECTED' : '❌ NOT DETECTED');
    console.log('Reason:', result1.reason || 'Status indicator');
    
    // Test 2: Normal status
    console.log('\nTest 2: Normal status');
    const result2 = await detectFuelFill('TEST002', 500, 'Normal Operation');
    console.log('Result:', result2.isFill ? '✅ DETECTED' : '❌ NOT DETECTED');
    console.log('Reason:', result2.reason || 'No status indicator');
    
    // Test 3: Fuel Fill status
    console.log('\nTest 3: Fuel Fill status');
    const result3 = await detectFuelFill('TEST003', 600, 'Fuel Fill Detected');
    console.log('Result:', result3.isFill ? '✅ DETECTED' : '❌ NOT DETECTED');
    console.log('Reason:', result3.reason || 'Status indicator');
    
    console.log('\n🎯 Detection system test completed');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testFuelDetection();