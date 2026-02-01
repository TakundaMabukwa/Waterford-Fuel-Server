require('dotenv').config();
const axios = require('axios');

async function testActivitySimple() {
  console.log('🧪 Testing Activity Report (Simple)...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity?date=2025-10-30');
    
    console.log('✅ Activity report working!');
    console.log('📊 Keys:', Object.keys(response.data.data));
    
    if (response.data.data.time_periods) {
      console.log('🕐 Time Periods Available:', Object.keys(response.data.data.time_periods));
    }
    
  } catch (error) {
    console.error('❌ Error details:', error.response?.status, error.response?.statusText);
    console.error('❌ Error data:', error.response?.data);
  }
}

testActivitySimple();