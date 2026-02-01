const costCenterAccess = require('./helpers/cost-center-access');

async function testSingleSiteFiltering() {
  console.log('🧪 Testing Single Site Filtering...\n');
  
  try {
    // Test 1: Get accessible sites for a cost center
    console.log('📍 Test 1: Get accessible sites for KFC-0001-0001-0002-0004');
    const sites = await costCenterAccess.getAccessibleSites('KFC-0001-0001-0002-0004');
    console.log(`Found ${sites.length} sites:`, sites.map(s => `${s.site_name} (${s.site_id})`));
    
    // Test 2: Filter to single site
    if (sites.length > 0) {
      const singleSiteId = sites[0].site_id;
      console.log(`\n📍 Test 2: Filter to single site ${singleSiteId}`);
      const singleSite = await costCenterAccess.getAccessibleSites('KFC-0001-0001-0002-0004', singleSiteId);
      console.log(`Filtered to ${singleSite.length} site:`, singleSite.map(s => `${s.site_name} (${s.site_id})`));
    }
    
    // Test 3: Test with parent cost center and single site
    console.log(`\n📍 Test 3: Parent cost center with single site filter`);
    const parentSites = await costCenterAccess.getAccessibleSites('KFC-0001-0001-0002');
    console.log(`Parent has access to ${parentSites.length} sites`);
    
    if (parentSites.length > 0) {
      const testSiteId = parentSites[5]?.site_id; // Pick 6th site
      if (testSiteId) {
        console.log(`Filtering parent to single site ${testSiteId}`);
        const filteredParent = await costCenterAccess.getAccessibleSites('KFC-0001-0001-0002', testSiteId);
        console.log(`Filtered parent to ${filteredParent.length} site:`, filteredParent.map(s => `${s.site_name} (${s.site_id})`));
      }
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

testSingleSiteFiltering();