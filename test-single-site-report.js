require('dotenv').config();
const axios = require('axios');

async function testSingleSiteReport() {
  console.log('🧪 Testing Single Site Report Generation...\n');
  
  try {
    // Test single site daily report
    console.log('📊 Testing single site daily report for BALLYCLARE');
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/generate/daily', {
      params: {
        site_id: 'BALLYCLARE',
        date: '2025-10-30'
      }
    });
    
    console.log('✅ Single site daily report generated successfully!');
    console.log('📈 Summary:', response.data.data.summary);
    console.log('🏢 Sites:', response.data.data.sites.length);
    
  } catch (error) {
    console.error('❌ Test failed:', error.response?.data || error.message);
  }
}

testSingleSiteReport();