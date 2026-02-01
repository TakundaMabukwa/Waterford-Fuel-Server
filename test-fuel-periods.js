require('dotenv').config();
const axios = require('axios');

async function testFuelPeriods() {
  console.log('⛽ Testing Activity Report with Fuel Consumption Periods...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30'
      }
    });
    
    console.log('✅ Activity report with fuel analysis working!');
    console.log('📅 Date:', response.data.data.date);
    
    console.log('\n⛽ Fuel Analysis:');
    const fuelAnalysis = response.data.data.fuel_analysis;
    console.log(`   📊 Daily Total: ${fuelAnalysis.daily_total_consumption}L`);
    console.log(`   🏆 Peak Period: ${fuelAnalysis.peak_usage_period.name} (${fuelAnalysis.peak_usage_period.usage.toFixed(2)}L)`);
    console.log(`   🏪 Peak Site: ${fuelAnalysis.peak_usage_site.site} (${fuelAnalysis.peak_usage_site.usage.toFixed(2)}L)`);
    
    console.log('\n📊 Period Breakdown:');
    console.log(`   🌅 Morning (6AM-12PM): ${fuelAnalysis.period_breakdown.morning}L`);
    console.log(`   ☀️ Afternoon (12PM-5PM): ${fuelAnalysis.period_breakdown.afternoon}L`);
    console.log(`   🌙 Evening (5PM-11PM): ${fuelAnalysis.period_breakdown.evening}L`);
    
    console.log('\n🕐 Time Periods with Fuel Data:');
    Object.entries(response.data.data.time_periods).forEach(([period, data]) => {
      console.log(`\n${data.name}:`);
      if (data.fuel_consumption) {
        console.log(`   ⛽ Consumption: ${data.fuel_consumption.usage.toFixed(2)}L`);
        console.log(`   🏪 Sites: ${Object.keys(data.fuel_consumption.sites).length}`);
      }
      if (data.data) {
        console.log(`   🚗 Active: ${data.data.active_vehicles}/${data.data.total_vehicles}`);
      }
    });
    
  } catch (error) {
    console.error('❌ Fuel periods test failed:', error.response?.data || error.message);
  }
}

testFuelPeriods();