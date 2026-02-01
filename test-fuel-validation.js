require('dotenv').config();
const axios = require('axios');

async function testFuelValidation() {
  console.log('🔍 Testing Fuel Consumption with Validation...\n');
  
  try {
    // Test with a date that has fuel data
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30',
        site_id: 'MILNERTON'
      }
    });
    
    console.log('✅ Activity report with fuel validation working!');
    console.log('📅 Date:', response.data.data.date);
    console.log('🎯 Site:', response.data.data.site_id);
    
    const fuelAnalysis = response.data.data.fuel_analysis;
    console.log('\n⛽ Fuel Analysis Results:');
    console.log(`   📊 Daily Total: ${fuelAnalysis.daily_total_consumption}L`);
    console.log(`   🏆 Peak Period: ${fuelAnalysis.peak_usage_period.name} (${fuelAnalysis.peak_usage_period.usage.toFixed(2)}L)`);
    console.log(`   🏪 Peak Site: ${fuelAnalysis.peak_usage_site.site || 'None'} (${fuelAnalysis.peak_usage_site.usage.toFixed(2)}L)`);
    
    console.log('\n📊 Period Breakdown:');
    console.log(`   🌅 Morning: ${fuelAnalysis.period_breakdown.morning}L`);
    console.log(`   ☀️ Afternoon: ${fuelAnalysis.period_breakdown.afternoon}L`);
    console.log(`   🌙 Evening: ${fuelAnalysis.period_breakdown.evening}L`);
    
    // Check if we have any consumption data
    const totalConsumption = parseFloat(fuelAnalysis.daily_total_consumption);
    if (totalConsumption > 0) {
      console.log('\n✅ Fuel consumption data captured successfully!');
    } else {
      console.log('\n⚠️ No fuel consumption detected - may need data for the specified date/site');
    }
    
  } catch (error) {
    console.error('❌ Fuel validation test failed:', error.response?.data || error.message);
  }
}

testFuelValidation();