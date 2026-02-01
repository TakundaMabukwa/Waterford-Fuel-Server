const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testExecutiveFiltered() {
  console.log('👔 Testing Executive Dashboard with Cost Center Filter\n');

  try {
    // Test 1: Filter by single cost code
    console.log('1️⃣ Testing single cost code filter (KFC-0001-0001-0003)...\n');
    
    const response1 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard?costCode=KFC-0001-0001-0003`);
    
    if (response1.data.success) {
      const data = response1.data.data;
      
      console.log('✅ Filtered Dashboard (KFC-0001-0001-0003):');
      console.log('='.repeat(50));
      console.log(`🚗 Fleet: ${data.fleet_overview.active_vehicles}/${data.fleet_overview.total_vehicles} active`);
      console.log(`⚙️ Operating Hours: ${data.operational_metrics.total_operating_hours}h`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`🏢 Cost Centers: ${data.cost_center_performance.length}`);
      console.log(`📍 Filter Applied: ${data.period.cost_code_filter}`);
      
      console.log('\n💼 Cost Centers in Results:');
      data.cost_center_performance.forEach(cc => {
        console.log(`   - ${cc.cost_code}: ${cc.site_count} sites, R${cc.total_cost.toFixed(2)}`);
      });
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 2: Filter by multiple cost codes
    console.log('2️⃣ Testing multiple cost codes filter...\n');
    
    const response2 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard?costCodes=KFC-0001-0001-0003,KFC-0001-0001-0002-0004`);
    
    if (response2.data.success) {
      const data = response2.data.data;
      
      console.log('✅ Filtered Dashboard (Multiple Cost Codes):');
      console.log('='.repeat(50));
      console.log(`🚗 Fleet: ${data.fleet_overview.active_vehicles}/${data.fleet_overview.total_vehicles} active`);
      console.log(`⚙️ Operating Hours: ${data.operational_metrics.total_operating_hours}h`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`📍 Filters Applied: ${data.period.cost_codes_filter}`);
      
      console.log('\n💼 Cost Centers in Results:');
      data.cost_center_performance.forEach(cc => {
        console.log(`   - ${cc.cost_code}: ${cc.site_count} sites, R${cc.total_cost.toFixed(2)}`);
      });
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 3: No filter (all cost centers)
    console.log('3️⃣ Testing no filter (all cost centers)...\n');
    
    const response3 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard`);
    
    if (response3.data.success) {
      const data = response3.data.data;
      
      console.log('✅ Unfiltered Dashboard (All Cost Centers):');
      console.log('='.repeat(50));
      console.log(`🚗 Fleet: ${data.fleet_overview.active_vehicles}/${data.fleet_overview.total_vehicles} active`);
      console.log(`⚙️ Operating Hours: ${data.operational_metrics.total_operating_hours}h`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`🏢 Total Cost Centers: ${data.cost_center_performance.length}`);
      
      console.log('\n💼 All Cost Centers:');
      data.cost_center_performance.slice(0, 5).forEach((cc, index) => {
        console.log(`   ${index + 1}. ${cc.cost_code}: ${cc.site_count} sites, R${cc.total_cost.toFixed(2)}`);
      });
    }

  } catch (error) {
    console.log('❌ Error:', error.response?.data?.error || error.message);
  }
}

testExecutiveFiltered().catch(console.error);