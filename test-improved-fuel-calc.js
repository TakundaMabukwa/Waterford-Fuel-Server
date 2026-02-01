require('dotenv').config();
const axios = require('axios');

async function testImprovedFuelCalculation() {
  try {
    console.log('🧪 Testing improved fuel calculation logic...\n');
    
    // Test with a date that has session data
    const testDate = '2025-10-31';
    
    console.log(`📅 Testing date: ${testDate}`);
    console.log(`🏪 Testing all sites\n`);
    
    // Call the activity report endpoint for all sites
    const response = await axios.get(`http://localhost:4000/api/energy-rite/reports/activity`, {
      params: {
        date: testDate
      }
    });
    
    if (response.data.success) {
      const data = response.data.data;
      
      console.log('📊 Activity Report Results:');
      console.log(`Date: ${data.date}`);
      console.log(`Site: ${data.site_id || 'All sites'}`);
      console.log(`Total sites: ${data.summary.total_sites}\n`);
      
      console.log('⛽ Fuel Analysis:');
      console.log(`Daily total consumption: ${data.fuel_analysis.daily_total_consumption}L`);
      console.log(`Daily total cost: R${data.fuel_analysis.daily_total_cost}`);
      console.log(`Fuel cost per liter: R${data.fuel_analysis.fuel_cost_per_liter}\n`);
      
      console.log('📈 Period Breakdown:');
      console.log(`Morning (6AM-12PM): ${data.fuel_analysis.period_breakdown.morning.fuel_usage}L (R${data.fuel_analysis.period_breakdown.morning.fuel_cost})`);
      console.log(`Afternoon (12PM-5PM): ${data.fuel_analysis.period_breakdown.afternoon.fuel_usage}L (R${data.fuel_analysis.period_breakdown.afternoon.fuel_cost})`);
      console.log(`Evening (5PM-11PM): ${data.fuel_analysis.period_breakdown.evening.fuel_usage}L (R${data.fuel_analysis.period_breakdown.evening.fuel_cost})\n`);
      
      console.log('🏪 Site Details:');
      data.sites.forEach(site => {
        console.log(`${site.branch}:`);
        console.log(`  Sessions: ${site.session_count}`);
        console.log(`  Operating hours: ${site.total_operating_hours.toFixed(2)}h`);
        console.log(`  Fuel usage: ${site.total_fuel_usage.toFixed(2)}L`);
        console.log(`  Cost: R${site.total_cost.toFixed(2)}`);
      });
      
      // Check for fuel fill detections in financial breakdown
      console.log('\n🔍 Checking for fuel fill detections:');
      const periods = ['morning', 'afternoon', 'evening', 'daily_total'];
      periods.forEach(period => {
        const costPerSite = data.fuel_analysis.financial_breakdown[period].cost_per_site;
        Object.entries(costPerSite).forEach(([site, cost]) => {
          if (typeof cost === 'string' && cost.includes('FUEL_FILL')) {
            console.log(`⛽ ${period.toUpperCase()}: ${site} - ${cost}`);
          }
        });
      });
      
    } else {
      console.error('❌ API Error:', response.data.error);
    }
    
  } catch (error) {
    console.error('❌ Test Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
  }
}

testImprovedFuelCalculation();