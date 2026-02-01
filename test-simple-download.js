const axios = require('axios');

async function testSimpleDownload() {
  try {
    console.log('🔧 Testing basic endpoint...');
    
    // First test the basic activity endpoint to make sure it works
    const basicResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity?date=2025-11-07');
    console.log('✅ Basic activity endpoint works');
    console.log(`   Sites: ${basicResponse.data.data.sites.length}`);
    console.log(`   Sessions: ${basicResponse.data.data.summary.total_sessions}`);
    
    // Now test the download endpoint with JSON format
    console.log('\n📄 Testing JSON download...');
    const jsonResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity/download?date=2025-11-07&format=json');
    console.log('✅ JSON download works');
    console.log(`   Data structure: ${Object.keys(jsonResponse.data)}`);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response && error.response.data) {
      console.error('Response data:', error.response.data);
    }
  }
}

testSimpleDownload();