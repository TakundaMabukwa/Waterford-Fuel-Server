require('dotenv').config();
const axios = require('axios');

async function testActivityEndpointDetailed() {
  console.log('🧪 Testing Activity Reports Endpoint with detailed error info...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: {
        date: '2025-10-30'
      },
      timeout: 30000 // 30 second timeout
    });
    
    console.log('✅ Activity reports endpoint working!');
    console.log('📊 Status Code:', response.status);
    console.log('📊 Response success:', response.data.success);
    
    if (response.data.success) {
      const data = response.data.data;
      console.log('📅 Date:', data.date);
      console.log('🏢 Total Sites:', data.summary?.total_sites || 'N/A');
      console.log('📈 Total Sessions:', data.summary?.total_sessions || 'N/A');
      console.log('⏰ Total Hours:', data.summary?.total_operating_hours || 'N/A');
      console.log('⛽ Total Fuel Usage:', data.summary?.total_fuel_usage || 'N/A');
      
      if (data.sites && data.sites.length > 0) {
        console.log('\n🏪 Sample Site Data:');
        data.sites.slice(0, 3).forEach((site, index) => {
          console.log(`  ${index + 1}. ${site.branch} - Sessions: ${site.session_count}, Fuel: ${site.total_fuel_usage}L`);
        });
      }
    } else {
      console.log('❌ API returned success: false');
      console.log('Error:', response.data.error || response.data.message);
    }
    
  } catch (error) {
    console.error('❌ Activity reports failed with detailed error:');
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Data:', error.response.data);
    } else if (error.request) {
      console.error('No response received:', error.message);
    } else {
      console.error('Error:', error.message);
    }
  }
}

testActivityEndpointDetailed();