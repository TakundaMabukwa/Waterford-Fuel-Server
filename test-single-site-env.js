require('dotenv').config();
const costCenterAccess = require('./helpers/cost-center-access');

async function testSingleSiteFiltering() {
  console.log('🧪 Testing Single Site Filtering...\n');
  
  try {
    // Test 1: Get accessible sites for a cost center
    console.log('📍 Test 1: Get accessible sites for KFC-0001-0001-0002-0004');
    const sites = await costCenterAccess.getAccessibleSites('KFC-0001-0001-0002-0004');
    console.log(`Found ${sites.length} sites:`, sites.map(s => `${s.plate} (${s.cost_code})`));
    
    // Test 2: Filter to single site
    if (sites.length > 0) {
      const singleSiteId = sites[0].plate;
      console.log(`\n📍 Test 2: Filter to single site ${singleSiteId}`);
      const singleSite = await costCenterAccess.getAccessibleSites('KFC-0001-0001-0002-0004', singleSiteId);
      console.log(`Filtered to ${singleSite.length} site:`, singleSite.map(s => `${s.plate} (${s.cost_code})`));
    }
    
    console.log('\n✅ Single site filtering test completed');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

testSingleSiteFiltering();