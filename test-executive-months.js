const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testExecutiveMonths() {
  console.log('📅 Testing Executive Dashboard Month Selection\n');

  try {
    // Test 1: Current month
    console.log('1️⃣ Testing current month (2025-10)...\n');
    
    const response1 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard?month=2025-10`);
    
    if (response1.data.success) {
      const data = response1.data.data;
      
      console.log('✅ October 2025 Dashboard:');
      console.log('='.repeat(50));
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`📊 Days in Period: ${data.period.days}`);
      console.log(`🚗 Fleet: ${data.fleet_overview.active_vehicles}/${data.fleet_overview.total_vehicles} active`);
      console.log(`⚙️ Operating Hours: ${data.operational_metrics.total_operating_hours}h`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`📈 Total Sessions: ${data.operational_metrics.total_sessions}`);
      
      console.log('\n💼 Cost Centers:');
      data.cost_center_performance.forEach(cc => {
        console.log(`   - ${cc.cost_code}: ${cc.site_count} sites, R${cc.total_cost.toFixed(2)}`);
      });
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 2: Previous month
    console.log('2️⃣ Testing previous month (2025-09)...\n');
    
    const response2 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard?month=2025-09`);
    
    if (response2.data.success) {
      const data = response2.data.data;
      
      console.log('✅ September 2025 Dashboard:');
      console.log('='.repeat(50));
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`📊 Days in Period: ${data.period.days}`);
      console.log(`🚗 Fleet: ${data.fleet_overview.active_vehicles}/${data.fleet_overview.total_vehicles} active`);
      console.log(`⚙️ Operating Hours: ${data.operational_metrics.total_operating_hours}h`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`📈 Total Sessions: ${data.operational_metrics.total_sessions}`);
      
      if (data.cost_center_performance.length > 0) {
        console.log('\n💼 Cost Centers:');
        data.cost_center_performance.forEach(cc => {
          console.log(`   - ${cc.cost_code}: ${cc.site_count} sites, R${cc.total_cost.toFixed(2)}`);
        });
      } else {
        console.log('\n💼 No cost center data for September 2025');
      }
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 3: Older month
    console.log('3️⃣ Testing older month (2024-12)...\n');
    
    const response3 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard?month=2024-12`);
    
    if (response3.data.success) {
      const data = response3.data.data;
      
      console.log('✅ December 2024 Dashboard:');
      console.log('='.repeat(50));
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`📊 Days in Period: ${data.period.days}`);
      console.log(`🚗 Fleet: ${data.fleet_overview.active_vehicles}/${data.fleet_overview.total_vehicles} active`);
      console.log(`⚙️ Operating Hours: ${data.operational_metrics.total_operating_hours}h`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`📈 Total Sessions: ${data.operational_metrics.total_sessions}`);
      
      if (data.cost_center_performance.length > 0) {
        console.log('\n💼 Cost Centers:');
        data.cost_center_performance.forEach(cc => {
          console.log(`   - ${cc.cost_code}: ${cc.site_count} sites, R${cc.total_cost.toFixed(2)}`);
        });
      } else {
        console.log('\n💼 No cost center data for December 2024');
      }
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 4: Month with cost code filter
    console.log('4️⃣ Testing month + cost code filter (2025-10 + KFC-0001-0001-0003)...\n');
    
    const response4 = await axios.get(`${BASE_URL}/api/energy-rite/executive-dashboard?month=2025-10&costCode=KFC-0001-0001-0003`);
    
    if (response4.data.success) {
      const data = response4.data.data;
      
      console.log('✅ October 2025 + Cost Code Filter:');
      console.log('='.repeat(50));
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`📍 Month Filter: ${data.period.month_filter}`);
      console.log(`🏢 Cost Code Filter: ${data.period.cost_code_filter}`);
      console.log(`💰 Total Cost: R${data.operational_metrics.total_operating_cost}`);
      console.log(`📈 Total Sessions: ${data.operational_metrics.total_sessions}`);
    }

  } catch (error) {
    console.log('❌ Error:', error.response?.data?.error || error.message);
  }
}

testExecutiveMonths().catch(console.error);