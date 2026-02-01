const { detectFuelFill } = require('./helpers/fuel-fill-detector');

async function testFixes() {
    console.log('🧪 Testing fuel fill detection fixes...');
    
    // Test with valid data
    try {
        const result = await detectFuelFill('TEST_PLATE', 100, 'Normal operation');
        console.log('✅ Test 1 passed:', result.reason);
    } catch (error) {
        console.error('❌ Test 1 failed:', error.message);
    }
    
    // Test with null data
    try {
        const result = await detectFuelFill('TEST_PLATE', null, null);
        console.log('✅ Test 2 passed:', result.reason);
    } catch (error) {
        console.error('❌ Test 2 failed:', error.message);
    }
    
    // Test with invalid data
    try {
        const result = await detectFuelFill(null, 'invalid', undefined);
        console.log('✅ Test 3 passed:', result.reason);
    } catch (error) {
        console.error('❌ Test 3 failed:', error.message);
    }
    
    console.log('🏁 Tests completed');
}

testFixes();