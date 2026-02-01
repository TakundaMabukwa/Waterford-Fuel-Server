require('dotenv').config();
const axios = require('axios');

async function testHealth() {
  console.log('🏥 Testing Server Health...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/health');
    console.log('✅ Server is running:', response.data);
    
    // Test activity endpoint
    console.log('\n🧪 Testing activity endpoint...');
    const activityResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity?date=2025-10-30');
    console.log('✅ Activity endpoint working!');
    console.log('📊 Response keys:', Object.keys(activityResponse.data.data));
    
  } catch (error) {
    if (error.code === 'ECONNREFUSED') {
      console.error('❌ Server not running on port 4000');
    } else {
      console.error('❌ Error:', error.message);
      console.error('❌ Response:', error.response?.data);
    }
  }
}

testHealth();