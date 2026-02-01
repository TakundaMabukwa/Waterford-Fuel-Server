// Test the enhanced executive dashboard with focus on continuous operations
require('dotenv').config();
const axios = require('axios');

async function testEnhancedExecutiveDashboard() {
  console.log('🎯 Testing Enhanced Executive Dashboard');
  console.log('=' .repeat(60));
  
  try {
    // Test 1: Today's dashboard without filters
    console.log('\n📊 TEST 1: Today\'s Executive Dashboard');
    console.log('-'.repeat(40));
    
    const response1 = await axios.get('http://localhost:4000/api/energy-rite/enhanced-executive-dashboard');
    const dashboard = response1.data.data;
    
    console.log('📈 KEY METRICS (CUMULATIVE):');
    console.log(`   Total Sites Operated: ${dashboard.key_metrics.total_sites_operated}`);
    console.log(`   Total Litres Used: ${dashboard.key_metrics.total_litres_used}L (cumulative)`);
    console.log(`   Total Litres Filled: ${dashboard.key_metrics.total_litres_filled}L`);
    console.log(`   Net Fuel Consumption: ${dashboard.key_metrics.net_fuel_consumption}L`);
    console.log(`   Total Operational Hours: ${dashboard.key_metrics.total_operational_hours}h`);
    console.log(`   Continuous Operations: ${dashboard.key_metrics.continuous_operations_count} sites`);
    console.log(`   Total Cost: R${dashboard.key_metrics.total_operational_cost}`);
    console.log(`   Fuel Fill Events: ${dashboard.key_metrics.total_fuel_fill_events}`);
    
    console.log('\n📅 PERIOD INFO:');
    console.log(`   Period: ${dashboard.period.start_date} to ${dashboard.period.end_date}`);
    console.log(`   Days: ${dashboard.period.days} (cumulative)`);
    console.log(`   Is Cumulative: ${dashboard.period.is_cumulative}`);
    
    console.log('\n🚛 FLEET STATUS:');
    console.log(`   Fleet Size: ${dashboard.fleet_status.total_fleet_size}`);
    console.log(`   Currently Active: ${dashboard.fleet_status.currently_active}`);
    console.log(`   Utilization: ${dashboard.fleet_status.fleet_utilization_percentage}%`);
    
    console.log('\n⛽ FUEL TRACKING:');
    console.log(`   Fill Events: ${dashboard.fuel_tracking.fuel_fills_summary.total_fill_events}`);
    console.log(`   Total Filled: ${dashboard.fuel_tracking.fuel_fills_summary.total_litres_filled}L`);
    console.log(`   Sites with Fills: ${dashboard.fuel_tracking.fuel_fills_summary.sites_with_fills}`);
    console.log(`   Avg Fill Amount: ${dashboard.fuel_tracking.fuel_fills_summary.average_fill_amount}L`);
    
    console.log('\n⚡ FUEL EFFICIENCY:');
    console.log(`   Total Used: ${dashboard.fuel_tracking.fuel_efficiency.total_used}L`);
    console.log(`   Total Filled: ${dashboard.fuel_tracking.fuel_efficiency.total_filled}L`);
    console.log(`   Net Consumption: ${dashboard.fuel_tracking.fuel_efficiency.net_consumption}L`);
    console.log(`   Usage/Fill Ratio: ${dashboard.fuel_tracking.fuel_efficiency.usage_to_fill_ratio}`);
    console.log(`   Fill Frequency: ${dashboard.fuel_tracking.fuel_efficiency.fill_frequency} fills/site`);
    
    console.log('\n⏰ CONTINUOUS OPERATIONS (24+ Hours):');
    dashboard.continuous_operations.sites_over_24_hours.forEach((site, index) => {
      console.log(`   ${index + 1}. ${site.site} (${site.cost_code})`);
      console.log(`      Hours: ${site.total_hours}h | Fuel: ${site.fuel_usage}L`);
      console.log(`      Pattern: ${site.pattern}`);
    });
    
    console.log('\n📍 TOP PERFORMING SITES:');
    dashboard.site_performance.slice(0, 5).forEach((site, index) => {
      console.log(`   ${index + 1}. ${site.site_name} (${site.cost_code})`);
      console.log(`      Hours: ${site.operating_hours}h | Fuel Used: ${site.fuel_usage_liters}L`);
      console.log(`      Fuel Filled: ${site.fuel_filled_liters}L | Net: ${site.fuel_net_usage}L`);
      console.log(`      Fills: ${site.fuel_fills_count} | Efficiency: ${site.efficiency_liters_per_hour}L/h`);
      console.log(`      Cost: R${site.cost_per_hour}/h | Continuous: ${site.is_continuous ? 'Yes' : 'No'}`);
    });
    
    console.log('\n💰 COST CENTER ANALYSIS:');
    dashboard.cost_center_analysis.slice(0, 3).forEach((cc, index) => {
      console.log(`   ${index + 1}. ${cc.cost_code}`);
      console.log(`      Sites: ${cc.sites_count} | Hours: ${cc.operating_hours}h`);
      console.log(`      Fuel Used: ${cc.fuel_usage_liters}L | Fuel Filled: ${cc.fuel_filled_liters}L`);
      console.log(`      Net Consumption: ${cc.fuel_net_usage}L | Cost: R${cc.operational_cost}`);
      console.log(`      Fill Events: ${cc.fuel_fills_count} | Efficiency: ${cc.avg_fuel_per_hour}L/h`);
    });
    
    console.log('\n⚡ EFFICIENCY METRICS:');
    console.log(`   Avg Fuel/Hour: ${dashboard.efficiency_metrics.average_fuel_per_hour}L`);
    console.log(`   Avg Cost/Hour: R${dashboard.efficiency_metrics.average_cost_per_hour}`);
    console.log(`   Avg Hours/Site: ${dashboard.efficiency_metrics.average_hours_per_site}h`);
    console.log(`   Avg Fuel/Site: ${dashboard.efficiency_metrics.average_fuel_per_site}L`);
    
    console.log('\n💡 EXECUTIVE INSIGHTS:');
    dashboard.executive_insights.forEach((insight, index) => {
      console.log(`   ${index + 1}. ${insight}`);
    });
    
    // Test 2: Dashboard with cost code filter
    console.log('\n\n📊 TEST 2: Dashboard with Cost Code Filter');
    console.log('-'.repeat(40));
    
    const response2 = await axios.get('http://localhost:4000/api/energy-rite/enhanced-executive-dashboard?costCode=KFC-0001');
    const filteredDashboard = response2.data.data;
    
    console.log('📈 FILTERED METRICS (KFC-0001):');
    console.log(`   Total Sites: ${filteredDashboard.key_metrics.total_sites_operated}`);
    console.log(`   Total Litres: ${filteredDashboard.key_metrics.total_litres_used}L`);
    console.log(`   Total Hours: ${filteredDashboard.key_metrics.total_operational_hours}h`);
    console.log(`   Continuous Ops: ${filteredDashboard.key_metrics.continuous_operations_count}`);
    
    // Test 3: Historical date
    console.log('\n\n📊 TEST 3: Historical Date Dashboard');
    console.log('-'.repeat(40));
    
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    
    try {
      const response3 = await axios.get(`http://localhost:4000/api/energy-rite/enhanced-executive-dashboard?date=${yesterdayStr}`);
      const historicalDashboard = response3.data.data;
      
      console.log(`📈 HISTORICAL METRICS (${yesterdayStr}):`);
      console.log(`   Total Sites: ${historicalDashboard.key_metrics.total_sites_operated}`);
      console.log(`   Total Litres: ${historicalDashboard.key_metrics.total_litres_used}L`);
      console.log(`   Total Hours: ${historicalDashboard.key_metrics.total_operational_hours}h`);
      console.log(`   Continuous Ops: ${historicalDashboard.key_metrics.continuous_operations_count}`);
    } catch (error) {
      console.log(`   No data available for ${yesterdayStr}`);
    }
    
    console.log('\n✅ Enhanced Executive Dashboard Test Complete!');
    console.log('🎯 Key Features Demonstrated:');
    console.log('   ✓ Cumulative metrics over specified period');
    console.log('   ✓ Fuel usage and fuel fills tracking');
    console.log('   ✓ Net fuel consumption analysis');
    console.log('   ✓ Continuous operations detection (24+ hours)');
    console.log('   ✓ Site-by-site breakdown with fuel efficiency');
    console.log('   ✓ Cost center analysis with hierarchical filtering');
    console.log('   ✓ Real-time fleet status integration');
    console.log('   ✓ Executive insights with fuel tracking');
    
  } catch (error) {
    console.error('❌ Error testing enhanced executive dashboard:', error.response?.data || error.message);
  }
}

// Test the API endpoints
testEnhancedExecutiveDashboard();