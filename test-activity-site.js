require('dotenv').config();
const axios = require('axios');

async function testActivitySiteFilter() {
  console.log('🧪 Testing Activity Report with Site Filter...\n');
  
  try {
    // Test with site_id
    console.log('📍 Testing with site_id=BALLYCLARE');
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: {
        date: '2025-10-30',
        site_id: 'BALLYCLARE'
      }
    });
    
    console.log('✅ Activity report with site filter working!');
    console.log('📊 Summary:', response.data.data.summary);
    console.log('🏢 Sites:', response.data.data.sites.length);
    console.log('🎯 Site ID:', response.data.data.site_id);
    
    // Show site names
    response.data.data.sites.forEach(site => {
      console.log(`   - ${site.branch}: ${site.session_count} sessions`);
    });
    
  } catch (error) {
    console.error('❌ Activity site filter test failed:', error.response?.data || error.message);
  }
}

testActivitySiteFilter();