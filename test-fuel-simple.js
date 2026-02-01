// Load environment variables first
require('dotenv').config();

const axios = require('axios');

async function testFuelUsageSimple() {
  console.log('⛽ SIMPLE FUEL USAGE TEST');
  console.log('=' .repeat(40));

  try {
    // Test 1: Basic fuel usage calculation
    console.log('\n🧪 Testing fuel usage calculation...');
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/snapshots?calculate_fuel_usage=true&limit=3');
    
    console.log('✅ Status:', response.status);
    console.log('📊 Response received');
    
    if (response.data.success) {
      const analysis = response.data.data.fuel_usage_analysis;
      
      if (analysis && !analysis.error) {
        console.log('\n⛽ FUEL ANALYSIS RESULTS:');
        console.log(`📅 Date: ${analysis.date}`);
        console.log(`🔥 Total Fuel Used: ${analysis.total_fuel_used}L`);
        console.log(`📋 Periods Analyzed: ${analysis.periods.length}`);
        
        analysis.periods.forEach((period, index) => {
          console.log(`\n   📊 Period ${index + 1}: ${period.period_name}`);
          console.log(`   ⛽ Fuel Used: ${period.total_fuel_used.toFixed(2)}L`);
          console.log(`   🚗 Vehicles: ${period.vehicles.length}`);
        });
        
        console.log('\n✅ FUEL USAGE CALCULATION SUCCESS!');
      } else {
        console.log('❌ Fuel analysis error:', analysis?.error || 'No data');
      }
    } else {
      console.log('❌ API Error:', response.data.error);
    }

  } catch (error) {
    if (error.response) {
      console.error('❌ API Error:', error.response.status, error.response.data);
    } else {
      console.error('❌ Test failed:', error.message);
    }
  }
}

// Run the test
testFuelUsageSimple();