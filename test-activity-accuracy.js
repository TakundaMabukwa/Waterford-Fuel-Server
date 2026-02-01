require('dotenv').config();
const axios = require('axios');

async function testActivityAccuracy() {
  console.log('🔍 Testing Activity Report Accuracy...\n');
  
  try {
    // Test 1: Get all sites first
    console.log('📊 Step 1: Getting all sites activity');
    const allResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { date: '2025-10-30' }
    });
    
    console.log(`All sites: ${allResponse.data.data.sites.length} sites found`);
    allResponse.data.data.sites.forEach(site => {
      console.log(`   - ${site.branch}: ${site.session_count} sessions`);
    });
    
    // Test 2: Pick a site with sessions and test single site filter
    const siteWithSessions = allResponse.data.data.sites.find(s => s.session_count > 0);
    if (siteWithSessions) {
      console.log(`\n🎯 Step 2: Testing single site filter for ${siteWithSessions.branch}`);
      
      const singleResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
        params: { 
          date: '2025-10-30',
          site_id: siteWithSessions.branch
        }
      });
      
      console.log(`Single site result: ${singleResponse.data.data.sites.length} sites`);
      singleResponse.data.data.sites.forEach(site => {
        console.log(`   - ${site.branch}: ${site.session_count} sessions`);
      });
      
      // Verify accuracy
      const isAccurate = singleResponse.data.data.sites.length === 1 && 
                        singleResponse.data.data.sites[0].branch === siteWithSessions.branch;
      
      console.log(`\n✅ Accuracy Check: ${isAccurate ? 'PASSED' : 'FAILED'}`);
      if (isAccurate) {
        console.log('   ✓ Returns exactly 1 site');
        console.log(`   ✓ Site matches requested: ${siteWithSessions.branch}`);
      }
    } else {
      console.log('\n⚠️ No sites with sessions found for accuracy test');
    }
    
  } catch (error) {
    console.error('❌ Accuracy test failed:', error.response?.data || error.message);
  }
}

testActivityAccuracy();