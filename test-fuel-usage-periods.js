// Load environment variables first
require('dotenv').config();

const axios = require('axios');

async function testPeriodBasedFuelUsage() {
  console.log('⛽ TESTING PERIOD-BASED FUEL USAGE CALCULATIONS');
  console.log('='.repeat(60));

  const baseUrl = 'http://localhost:4000/api/energy-rite/reports/snapshots';

  try {
    // Test 1: Check current snapshots and fuel usage analysis
    console.log('\n🔍 Step 1: Test fuel usage calculation with existing data');
    console.log('📞 GET', `${baseUrl}?include_fuel_usage=true`);
    
    const response = await axios.get(`${baseUrl}?include_fuel_usage=true`);
    
    console.log('✅ Response Status:', response.status);
    console.log('📊 Total Snapshots:', response.data.data.summary.total_snapshots);
    
    // Check fuel usage analysis
    const fuelAnalysis = response.data.data.fuel_usage_analysis;
    if (fuelAnalysis) {
      console.log('\n⛽ FUEL USAGE ANALYSIS:');
      console.log(`📋 Analysis Status: ${fuelAnalysis.analysis_available ? 'Available' : 'Not Available'}`);
      console.log(`🔧 Analysis Method: ${fuelAnalysis.method}`);
      
      if (fuelAnalysis.periods && fuelAnalysis.periods.length > 0) {
        console.log(`\n📊 PERIOD-BASED FUEL CONSUMPTION:`);
        fuelAnalysis.periods.forEach(period => {
          console.log(`\n🕒 Period: ${period.period_name}`);
          console.log(`   Time Range: ${period.start_time} → ${period.end_time}`);
          console.log(`   Total Fuel Used: ${period.total_fuel_used.toFixed(2)}L`);
          console.log(`   Vehicles Analyzed: ${period.vehicle_count}`);
          
          if (period.cost_center_breakdown && period.cost_center_breakdown.length > 0) {
            console.log(`   Cost Center Breakdown:`);
            period.cost_center_breakdown.forEach(cc => {
              console.log(`     💼 ${cc.cost_code}: ${cc.fuel_used.toFixed(2)}L (${cc.vehicle_count} vehicles)`);
            });
          }
          
          if (period.vehicle_breakdown && period.vehicle_breakdown.length > 0) {
            console.log(`   Vehicle Breakdown:`);
            period.vehicle_breakdown.slice(0, 3).forEach(vehicle => {
              console.log(`     🚗 ${vehicle.vehicle_plate}: ${vehicle.fuel_used.toFixed(2)}L`);
              console.log(`         Start: ${vehicle.start_fuel}L → End: ${vehicle.end_fuel}L`);
            });
          }
        });
        
        // Summary
        const totalFuelUsed = fuelAnalysis.periods.reduce((sum, p) => sum + p.total_fuel_used, 0);
        console.log(`\n📈 DAILY SUMMARY:`);
        console.log(`   Total Fuel Consumed: ${totalFuelUsed.toFixed(2)}L`);
        console.log(`   Analysis Periods: ${fuelAnalysis.periods.length}`);
      }
      
      if (fuelAnalysis.cost_center_summary && fuelAnalysis.cost_center_summary.length > 0) {
        console.log(`\n💼 COST CENTER SUMMARY:`);
        fuelAnalysis.cost_center_summary.forEach(cc => {
          console.log(`   ${cc.cost_code}: ${cc.total_fuel_used.toFixed(2)}L across ${cc.periods.length} periods`);
        });
      }
      
    } else {
      console.log('\n⚠️ No fuel usage analysis available');
      console.log('💡 This could be because:');
      console.log('   - Not enough snapshot pairs (need start/end of periods)');
      console.log('   - All snapshots are from the same snapshot type');
      console.log('   - Fuel levels are identical between periods');
    }

    // Test 2: Filter by cost code with fuel usage
    console.log('\n🧪 Step 2: Test fuel usage with cost code filtering');
    const costCodeUrl = `${baseUrl}?cost_code=KFC-0001-0001-0003&include_fuel_usage=true`;
    console.log('📞 GET', costCodeUrl);
    
    const costCodeResponse = await axios.get(costCodeUrl);
    console.log('✅ Cost Code Filter Response Status:', costCodeResponse.status);
    console.log('📊 Filtered Snapshots:', costCodeResponse.data.data.summary.total_snapshots);
    
    const costCodeAnalysis = costCodeResponse.data.data.fuel_usage_analysis;
    if (costCodeAnalysis && costCodeAnalysis.analysis_available) {
      console.log('⛽ Fuel analysis available for specific cost code');
    } else {
      console.log('ℹ️ No fuel analysis for this cost code filter');
    }

    // Test 3: Test with hierarchy and fuel usage
    console.log('\n🧪 Step 3: Test hierarchy with fuel usage');
    const hierarchyUrl = `${baseUrl}?cost_code=KFC-0001-0001&include_hierarchy=true&include_fuel_usage=true`;
    console.log('📞 GET', hierarchyUrl);
    
    const hierarchyResponse = await axios.get(hierarchyUrl);
    console.log('✅ Hierarchy Response Status:', hierarchyResponse.status);
    console.log('📊 Hierarchy Snapshots:', hierarchyResponse.data.data.summary.total_snapshots);
    console.log('🏗️ Accessible Cost Codes:', hierarchyResponse.data.data.hierarchy.total_accessible_codes);

    // Test 4: Test specific period analysis
    console.log('\n🧪 Step 4: Test different snapshot types');
    
    const snapshotTypes = ['MORNING', 'MIDDAY', 'EVENING'];
    for (const type of snapshotTypes) {
      try {
        const typeUrl = `${baseUrl}?snapshot_type=${type}&include_fuel_usage=true&limit=3`;
        const typeResponse = await axios.get(typeUrl);
        console.log(`   ${type}: ${typeResponse.data.data.summary.total_snapshots} snapshots`);
      } catch (error) {
        console.log(`   ${type}: Error - ${error.response?.status || error.message}`);
      }
    }

    // Show snapshot breakdown for analysis
    console.log('\n📋 SNAPSHOT DATA BREAKDOWN:');
    console.log('Current snapshots by type:');
    Object.entries(response.data.data.breakdowns.by_snapshot_type).forEach(([type, count]) => {
      console.log(`   ${type}: ${count} snapshots`);
    });

    console.log('\n📋 SNAPSHOT TIMES FOR ANALYSIS:');
    const allSnapshots = response.data.data.snapshots;
    const snapshotsByType = {};
    
    allSnapshots.forEach(snapshot => {
      if (!snapshotsByType[snapshot.snapshot_type]) {
        snapshotsByType[snapshot.snapshot_type] = [];
      }
      snapshotsByType[snapshot.snapshot_type].push({
        time: snapshot.snapshot_time,
        vehicle: snapshot.vehicle_plate,
        fuel: snapshot.fuel_volume
      });
    });

    Object.entries(snapshotsByType).forEach(([type, snapshots]) => {
      console.log(`\n   ${type} snapshots:`);
      snapshots.forEach(s => {
        const time = new Date(s.time).toLocaleTimeString();
        console.log(`     ${time} - ${s.vehicle}: ${s.fuel}L`);
      });
    });

    console.log('\n🎯 FUEL USAGE CALCULATION REQUIREMENTS:');
    console.log('To calculate fuel usage between periods, we need:');
    console.log('✅ Start snapshot (e.g., MORNING)');
    console.log('✅ End snapshot (e.g., MIDDAY)');
    console.log('✅ Same vehicle in both snapshots');
    console.log('✅ Different fuel levels to calculate consumption');
    
    const hasMultipleTypes = Object.keys(snapshotsByType).length > 1;
    console.log(`\n📊 Analysis Status: ${hasMultipleTypes ? '✅ Multiple periods available' : '❌ Need multiple period types'}`);

    console.log('\n🎉 PERIOD-BASED FUEL USAGE TESTING COMPLETED!');

  } catch (error) {
    if (error.response) {
      console.error('❌ API Error:', error.response.status);
      console.error('📋 Error Details:', error.response.data);
    } else if (error.code === 'ECONNREFUSED') {
      console.error('❌ Server not running. Make sure server is started.');
    } else {
      console.error('❌ Test failed:', error.message);
    }
  }
}

// Run the test
testPeriodBasedFuelUsage();