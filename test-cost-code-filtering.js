require('dotenv').config();
const controller = require('./controllers/energy-rite/energyRiteMonitoringController');

async function testCostCodeFiltering() {
  try {
    console.log('🔍 Testing Cost Code Filtering...\n');
    
    // Test top usage with specific cost code
    console.log('📊 Top Usage - Specific Cost Code (KFC-0001-0001-0002-0004):');
    await controller.getTopUsageSites(
      { query: { days: 30, limit: 10, cost_code: 'KFC-0001-0001-0002-0004' } },
      {
        status: (code) => ({
          json: (data) => {
            console.log(`   Found ${data.data.top_usage_sites.length} sites`);
            console.log(`   Filter: ${data.data.cost_code_filter}`);
            data.data.top_usage_sites.forEach((site, i) => {
              console.log(`   ${i+1}. ${site.branch} (${site.cost_code}) - ${site.total_usage}L`);
            });
            console.log('');
          }
        })
      }
    );
    
    // Test top usage with all cost codes
    console.log('📊 Top Usage - All Cost Codes:');
    await controller.getTopUsageSites(
      { query: { days: 30, limit: 10 } },
      {
        status: (code) => ({
          json: (data) => {
            console.log(`   Found ${data.data.top_usage_sites.length} sites`);
            console.log(`   Filter: ${data.data.cost_code_filter}`);
            data.data.top_usage_sites.forEach((site, i) => {
              console.log(`   ${i+1}. ${site.branch} (${site.cost_code || 'N/A'}) - ${site.total_usage}L`);
            });
            console.log('');
          }
        })
      }
    );
    
    // Test long running with cost code filter
    console.log('⏰ Long Running - Specific Cost Code:');
    await controller.getLongRunningSites(
      { query: { hours: 1, cost_code: 'KFC-0001-0001-0002-0004' } },
      {
        status: (code) => ({
          json: (data) => {
            console.log(`   Found ${data.data.count} long running sites`);
            console.log(`   Filter: ${data.data.cost_code_filter}`);
            console.log('');
          }
        })
      }
    );
    
    // Test dashboard with cost code filter
    console.log('📊 Dashboard - Specific Cost Code:');
    await controller.getMonitoringDashboard(
      { query: { cost_code: 'KFC-0001-0001-0002-0004' } },
      {
        status: (code) => ({
          json: (data) => {
            console.log(`   Filter: ${data.data.cost_code_filter}`);
            console.log(`   Active Sites: ${data.data.active_sites.total}`);
            console.log(`   Last 24h Sessions: ${data.data.last_24_hours.completed_sessions}`);
            console.log('');
          }
        })
      }
    );
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testCostCodeFiltering();