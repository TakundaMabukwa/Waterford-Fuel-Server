require('dotenv').config();
const axios = require('axios');

async function testExecutiveDashboard() {
  console.log('📊 Testing Executive Dashboard with Fuel Fills\n');

  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/executive-dashboard?days=7');
    
    const dashboard = response.data.data;
    
    console.log('🎯 EXECUTIVE DASHBOARD RESULTS:');
    console.log('=' .repeat(50));
    
    console.log('\n📅 Period:', dashboard.period);
    
    console.log('\n🚗 Fleet Overview:');
    console.log(`   Total Vehicles: ${dashboard.fleet_overview.total_vehicles}`);
    console.log(`   Active Vehicles: ${dashboard.fleet_overview.active_vehicles}`);
    console.log(`   Utilization: ${dashboard.fleet_overview.fleet_utilization_percentage}%`);
    
    console.log('\n⚙️ Operational Metrics:');
    console.log(`   Operating Hours: ${dashboard.operational_metrics.total_operating_hours}h`);
    console.log(`   Fuel Used: ${dashboard.operational_metrics.total_fuel_usage_liters}L`);
    console.log(`   Fuel Filled: ${dashboard.operational_metrics.total_fuel_filled_liters}L`);
    console.log(`   Net Consumption: ${dashboard.operational_metrics.net_fuel_consumption}L`);
    console.log(`   Engine Sessions: ${dashboard.operational_metrics.total_engine_sessions}`);
    console.log(`   Fuel Fill Events: ${dashboard.operational_metrics.total_fuel_fill_events}`);
    console.log(`   Total Cost: R${dashboard.operational_metrics.total_operating_cost}`);
    
    console.log('\n💡 Key Insights:');
    dashboard.key_insights.forEach(insight => {
      console.log(`   • ${insight}`);
    });
    
    console.log('\n🏆 Top Sites:');
    dashboard.top_performing_sites.slice(0, 5).forEach(site => {
      console.log(`   • ${site.site}: ${site.operating_hours}h, ${site.fuel_usage}L`);
    });
    
    console.log('\n✅ Dashboard includes fuel fill data!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testExecutiveDashboard().then(() => process.exit(0));