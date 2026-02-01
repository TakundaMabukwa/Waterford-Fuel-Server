require('dotenv').config();
const axios = require('axios');

async function testActivityReports() {
  console.log('🧪 Testing Activity Reports Endpoint...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: {
        date: '2025-10-30'
      }
    });
    
    console.log('✅ Activity reports endpoint working!');
    console.log('📊 Summary:', response.data.data.summary);
    console.log('🏢 Sites:', response.data.data.sites.length);
    
  } catch (error) {
    console.error('❌ Activity reports failed:', error.response?.data || error.message);
  }
}

testActivityReports();