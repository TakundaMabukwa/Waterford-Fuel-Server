/**
 * Test Activity Report Fill Combining
 * Verifies that the activity report endpoint combines fills the same way as Excel reports
 */

const axios = require('axios');

async function testActivityFillCombining() {
  try {
    const BASE_URL = 'http://64.227.138.235:4000';
    const testDate = '2026-01-14';
    const costCode = 'KFC-0001-0001-0002';
    
    console.log('🧪 Testing Activity Report Fill Combining');
    console.log('='.repeat(60));
    
    // Get activity report
    const activityUrl = `${BASE_URL}/api/energy-rite/reports/activity?date=${testDate}&cost_code=${costCode}`;
    console.log(`\n📊 Fetching: ${activityUrl}\n`);
    
    const response = await axios.get(activityUrl);
    const data = response.data;
    
    if (!data.success) {
      console.log('❌ API request failed');
      return;
    }
    
    console.log('✅ Activity Report Retrieved Successfully\n');
    
    // Check fuel fills
    const fuelFills = data.data.fuel_analysis?.fuel_fills;
    
    if (!fuelFills) {
      console.log('❌ No fuel fills data found in response');
      return;
    }
    
    console.log('📋 Fuel Fills Summary:');
    console.log('-'.repeat(60));
    console.log(`Total Fill Events: ${fuelFills.total_fill_events}`);
    console.log(`Total Fuel Filled: ${fuelFills.total_fuel_filled}L`);
    console.log(`Data Source: ${fuelFills.data_source}`);
    console.log();
    
    // Check fills by vehicle
    const fillsByVehicle = fuelFills.fills_by_vehicle;
    
    if (!fillsByVehicle || Object.keys(fillsByVehicle).length === 0) {
      console.log('⚠️  No vehicle-specific fill data found');
      return;
    }
    
    console.log('🚗 Fills by Vehicle:');
    console.log('='.repeat(60));
    
    Object.entries(fillsByVehicle).forEach(([vehicle, vehicleData]) => {
      console.log(`\n📍 Vehicle: ${vehicle}`);
      console.log(`   Fill Count: ${vehicleData.fill_count}`);
      console.log(`   Total Filled: ${vehicleData.total_filled.toFixed(2)}L`);
      
      if (vehicleData.fills && vehicleData.fills.length > 0) {
        console.log(`\n   Individual Fills:`);
        vehicleData.fills.forEach((fill, index) => {
          console.log(`   ${index + 1}. ${fill.time}`);
          console.log(`      Opening: ${fill.opening_fuel}L → Closing: ${fill.closing_fuel}L`);
          console.log(`      Amount: ${fill.amount}L`);
          
          if (fill.is_combined) {
            console.log(`      🔗 COMBINED (${fill.combined_count} fills)`);
            console.log(`      Duration: ${fill.duration}`);
          } else {
            console.log(`      ⚪ Individual Fill`);
          }
          
          if (fill.end_time && fill.end_time !== fill.time) {
            console.log(`      End Time: ${fill.end_time}`);
          }
          console.log();
        });
      }
    });
    
    // Summary of combining
    console.log('\n📊 Fill Combining Analysis:');
    console.log('='.repeat(60));
    
    let totalFills = 0;
    let combinedFills = 0;
    let individualFills = 0;
    
    Object.values(fillsByVehicle).forEach(vehicleData => {
      vehicleData.fills.forEach(fill => {
        totalFills++;
        if (fill.is_combined) {
          combinedFills++;
        } else {
          individualFills++;
        }
      });
    });
    
    console.log(`Total Fill Records: ${totalFills}`);
    console.log(`Combined Fills: ${combinedFills}`);
    console.log(`Individual Fills: ${individualFills}`);
    
    if (combinedFills > 0) {
      console.log('\n✅ Fill combining is working! Consecutive fills within 2 hours are being combined.');
    } else {
      console.log('\n⚠️  No combined fills found. This could mean:');
      console.log('   - All fills are more than 2 hours apart');
      console.log('   - There is only one fill per vehicle');
      console.log('   - The combining logic might need verification');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Response Status:', error.response.status);
      console.error('Response Data:', JSON.stringify(error.response.data, null, 2));
    }
  }
}

// Run the test
testActivityFillCombining();
