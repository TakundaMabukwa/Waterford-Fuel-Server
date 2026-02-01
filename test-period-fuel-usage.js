// Load environment variables first
require('dotenv').config();

const axios = require('axios');

async function testPeriodFuelUsageCalculations() {
  console.log('⛽ TESTING PERIOD-BASED FUEL USAGE CALCULATIONS');
  console.log('='.repeat(70));

  const baseUrl = 'http://localhost:4000/api/energy-rite/reports/snapshots';

  try {
    // Test 1: Get snapshots with fuel usage calculations
    console.log('\n🧪 TEST 1: All Periods Fuel Usage Analysis');
    console.log('📞 GET', `${baseUrl}?calculate_fuel_usage=true`);
    
    const allPeriodsResponse = await axios.get(`${baseUrl}?calculate_fuel_usage=true`);
    
    console.log('✅ Response Status:', allPeriodsResponse.status);
    console.log('📊 Snapshots Found:', allPeriodsResponse.data.data.summary.total_snapshots);
    
    const fuelAnalysis = allPeriodsResponse.data.data.fuel_usage_analysis;
    
    if (fuelAnalysis && !fuelAnalysis.error) {
      console.log('\n⛽ FUEL USAGE ANALYSIS:');
      console.log(`📅 Date: ${fuelAnalysis.date}`);
      console.log(`🔥 Total Fuel Used: ${fuelAnalysis.total_fuel_used}L`);
      console.log(`📋 Calculation Method: ${fuelAnalysis.calculation_method}`);
      console.log(`🕒 Period Definitions: ${fuelAnalysis.period_definitions}`);
      
      // Show period breakdown
      console.log('\n📊 FUEL USAGE BY PERIOD:');
      fuelAnalysis.periods.forEach(period => {
        console.log(`\n   🕒 ${period.period_name} (${period.start_snapshot} → ${period.end_snapshot})`);
        console.log(`   ⛽ Total Fuel Used: ${period.total_fuel_used.toFixed(2)}L`);
        console.log(`   🚗 Vehicles: ${period.vehicles.length}`);
        console.log(`   💼 Cost Centers: ${period.cost_centers.length}`);
        
        // Show vehicle details
        if (period.vehicles.length > 0) {
          console.log(`   📋 Vehicle Breakdown:`);
          period.vehicles.slice(0, 3).forEach(vehicle => {
            console.log(`     - ${vehicle.vehicle}: ${vehicle.fuel_used.toFixed(2)}L used`);
            console.log(`       Start: ${vehicle.start_fuel_volume}L → End: ${vehicle.end_fuel_volume}L`);
            console.log(`       Cost Code: ${vehicle.cost_code || 'N/A'}`);
          });
          if (period.vehicles.length > 3) {
            console.log(`     ... and ${period.vehicles.length - 3} more vehicles`);
          }
        }
        
        // Show cost center breakdown
        if (period.cost_centers.length > 0) {
          console.log(`   💼 Cost Center Breakdown:`);
          period.cost_centers.forEach(cc => {
            console.log(`     - ${cc.cost_code || 'NO_COST_CODE'}: ${cc.total_fuel_used.toFixed(2)}L (${cc.vehicles.length} vehicles)`);
          });
        }
      });
      
      // Show overall cost code summary
      console.log('\n💼 OVERALL COST CODE SUMMARY:');
      fuelAnalysis.cost_code_summary.forEach(cc => {
        console.log(`\n   Cost Code: ${cc.cost_code || 'NO_COST_CODE'}`);
        console.log(`   Total Fuel Used: ${cc.total_fuel_used.toFixed(2)}L`);
        console.log(`   Vehicles: ${cc.vehicles.join(', ')}`);
        console.log(`   Period Breakdown:`);
        Object.entries(cc.periods).forEach(([period, usage]) => {
          console.log(`     ${period}: ${usage.toFixed(2)}L`);
        });
      });
    } else {
      console.log('❌ Fuel usage analysis failed:', fuelAnalysis?.error || 'No data');
    }

    // Test 2: Specific period analysis
    console.log('\n🧪 TEST 2: Specific Period Analysis (MORNING)');
    console.log('📞 GET', `${baseUrl}?calculate_fuel_usage=true&snapshot_type=MORNING`);
    
    const morningResponse = await axios.get(`${baseUrl}?calculate_fuel_usage=true&snapshot_type=MORNING`);
    
    console.log('✅ Response Status:', morningResponse.status);
    const morningAnalysis = morningResponse.data.data.fuel_usage_analysis;
    
    if (morningAnalysis && !morningAnalysis.error) {
      console.log(`\n⛽ MORNING PERIOD ANALYSIS:`);
      console.log(`   Specific Period: ${morningAnalysis.specific_period || 'MORNING'}`);
      console.log(`   Total Fuel Used: ${morningAnalysis.total_fuel_used}L`);
      console.log(`   Periods Analyzed: ${morningAnalysis.periods.length}`);
      
      if (morningAnalysis.periods.length > 0) {
        const period = morningAnalysis.periods[0];
        console.log(`\n   📊 ${period.period_name} Details:`);
        console.log(`   Vehicles: ${period.vehicles.length}`);
        console.log(`   Cost Centers: ${period.cost_centers.length}`);
        console.log(`   Fuel Used: ${period.total_fuel_used.toFixed(2)}L`);
      }
    }

    // Test 3: Combine with cost code filtering
    console.log('\n🧪 TEST 3: Cost Code + Fuel Usage Analysis');
    console.log('📞 GET', `${baseUrl}?cost_code=KFC-0001-0001-0003&calculate_fuel_usage=true`);
    
    const costCodeResponse = await axios.get(`${baseUrl}?cost_code=KFC-0001-0001-0003&calculate_fuel_usage=true`);
    
    console.log('✅ Response Status:', costCodeResponse.status);
    const costCodeAnalysis = costCodeResponse.data.data.fuel_usage_analysis;
    
    if (costCodeAnalysis && !costCodeAnalysis.error) {
      console.log(`\n💼 COST CODE SPECIFIC ANALYSIS:`);
      console.log(`   Total Fuel Used: ${costCodeAnalysis.total_fuel_used}L`);
      console.log(`   Cost Centers: ${costCodeAnalysis.cost_code_summary.length}`);
      
      costCodeAnalysis.cost_code_summary.forEach(cc => {
        console.log(`\n   Cost Code: ${cc.cost_code}`);
        console.log(`   Vehicles: ${cc.vehicles.join(', ')}`);
        console.log(`   Total Usage: ${cc.total_fuel_used.toFixed(2)}L`);
        console.log(`   By Period:`);
        Object.entries(cc.periods).forEach(([period, usage]) => {
          console.log(`     ${period}: ${usage.toFixed(2)}L`);
        });
      });
    }

    // Test 4: Hierarchy + Fuel Usage
    console.log('\n🧪 TEST 4: Hierarchy + Fuel Usage Analysis');
    console.log('📞 GET', `${baseUrl}?cost_code=KFC-0001-0001&calculate_fuel_usage=true&include_hierarchy=true`);
    
    const hierarchyResponse = await axios.get(`${baseUrl}?cost_code=KFC-0001-0001&calculate_fuel_usage=true&include_hierarchy=true`);
    
    console.log('✅ Response Status:', hierarchyResponse.status);
    console.log('🏗️ Hierarchy Enabled:', hierarchyResponse.data.data.hierarchy.hierarchy_enabled);
    console.log('🔐 Accessible Codes:', hierarchyResponse.data.data.hierarchy.accessible_cost_codes.length);
    
    const hierarchyAnalysis = hierarchyResponse.data.data.fuel_usage_analysis;
    if (hierarchyAnalysis && !hierarchyAnalysis.error) {
      console.log(`\n🏗️ HIERARCHY FUEL ANALYSIS:`);
      console.log(`   Total Fuel Used: ${hierarchyAnalysis.total_fuel_used}L`);
      console.log(`   Cost Centers Found: ${hierarchyAnalysis.cost_code_summary.length}`);
    }

    console.log('\n🎉 PERIOD FUEL USAGE TESTING COMPLETED!');
    console.log('\n📚 FUEL USAGE FEATURES SUMMARY:');
    console.log('='.repeat(50));
    console.log('✅ Period-based fuel consumption calculations');
    console.log('✅ Start vs End fuel volume comparisons');
    console.log('✅ Fuel usage by cost center and vehicle');
    console.log('✅ Period breakdown (MORNING, MIDDAY, EVENING)');
    console.log('✅ Compatible with hierarchy and other filters');
    console.log('✅ Vehicle-level fuel consumption details');
    console.log('✅ Cost center fuel usage summaries');

    console.log('\n🔗 NEW FUEL USAGE PARAMETER:');
    console.log('   calculate_fuel_usage=true   - Enable fuel consumption analysis');
    console.log('   calculate_fuel_usage=false  - Standard snapshot data only (default)');

    console.log('\n💡 PERIOD CALCULATION LOGIC:');
    console.log('   MORNING: Night snapshot → Morning snapshot fuel difference');
    console.log('   MIDDAY:  Morning snapshot → Midday snapshot fuel difference');
    console.log('   EVENING: Midday snapshot → Evening snapshot fuel difference');

  } catch (error) {
    if (error.response) {
      console.error('❌ API Error:', error.response.status, error.response.data);
    } else if (error.code === 'ECONNREFUSED') {
      console.error('❌ Server not running. Start with: node server.js');
    } else {
      console.error('❌ Test failed:', error.message);
    }
  }
}

// Run the test
testPeriodFuelUsageCalculations();