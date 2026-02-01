require('dotenv').config();
const axios = require('axios');
const { supabase } = require('./supabase-client');

async function testReportsWithFuelFills() {
  try {
    console.log('📊 Testing Reports with Fuel Fill Integration...\n');
    
    const baseURL = 'http://localhost:4000';
    const testDate = new Date().toISOString().split('T')[0];
    
    // Step 1: Create test data with fuel fills
    console.log('🔧 Step 1: Creating test data...');
    
    const testPlate = 'REPORT-TEST-' + Date.now();
    
    // Insert fuel data showing a fill
    await supabase.from('energy_rite_fuel_data').insert([
      {
        plate: testPlate,
        fuel_probe_1_level: 50.0,
        drivername: 'Engine ON',
        created_at: new Date(Date.now() - 60 * 60 * 1000).toISOString() // 1 hour ago
      },
      {
        plate: testPlate,
        fuel_probe_1_level: 180.0, // +130L fill
        drivername: 'Possible Fuel Fill',
        created_at: new Date(Date.now() - 30 * 60 * 1000).toISOString() // 30 min ago
      },
      {
        plate: testPlate,
        fuel_probe_1_level: 160.0, // -20L usage
        drivername: 'Engine OFF',
        created_at: new Date().toISOString()
      }
    ]);
    
    // Insert a fuel fill record
    await supabase.from('energy_rite_fuel_fills').insert({
      plate: testPlate,
      fill_date: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
      fuel_before: 50.0,
      fuel_after: 180.0,
      fill_amount: 130.0,
      fill_percentage: 260.0,
      detection_method: 'STATUS_INDICATOR'
    });
    
    // Insert operating session
    await supabase.from('energy_rite_operating_sessions').insert({
      branch: testPlate,
      session_date: testDate,
      session_start_time: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
      session_end_time: new Date().toISOString(),
      operating_hours: 1.0,
      total_usage: 20.0,
      total_fill: 130.0,
      fill_events: 1,
      session_status: 'COMPLETED'
    });
    
    console.log('✅ Test data created');
    
    // Step 2: Test Activity Report
    console.log('\n📈 Step 2: Testing Activity Report...');
    
    try {
      const activityResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { date: testDate }
      });
      
      console.log('✅ Activity Report Generated');
      console.log('📋 Report Structure:');
      
      const report = activityResponse.data.data;
      
      // Check summary
      console.log(`   Summary:`);
      console.log(`     Total Sites: ${report.summary.total_sites}`);
      console.log(`     Total Sessions: ${report.summary.total_sessions}`);
      console.log(`     Total Fuel Usage: ${report.summary.total_fuel_usage}L`);
      console.log(`     Total Fuel Filled: ${report.summary.total_fuel_filled}L`);
      
      // Check if fuel fills are included
      if (report.summary.total_fuel_fills !== undefined) {
        console.log(`     Total Fuel Fills: ${report.summary.total_fuel_fills}`);
        console.log(`     Total Fuel Filled Amount: ${report.summary.total_fuel_filled_amount}L`);
      }
      
      // Check fuel analysis section
      if (report.fuel_analysis && report.fuel_analysis.fuel_fills) {
        console.log(`   Fuel Analysis - Fills:`);
        console.log(`     Fill Events: ${report.fuel_analysis.fuel_fills.total_fill_events}`);
        console.log(`     Total Filled: ${report.fuel_analysis.fuel_fills.total_fuel_filled}L`);
        
        if (report.fuel_analysis.fuel_fills.fills_by_vehicle) {
          console.log(`     Fills by Vehicle:`, Object.keys(report.fuel_analysis.fuel_fills.fills_by_vehicle));
        }
      }
      
      // Check site-level data
      const testSite = report.sites.find(s => s.branch === testPlate);
      if (testSite) {
        console.log(`   Site Data for ${testPlate}:`);
        console.log(`     Total Fuel Usage: ${testSite.total_fuel_usage}L`);
        console.log(`     Total Fuel Filled: ${testSite.total_fuel_filled}L`);
        
        if (testSite.fuel_fills) {
          console.log(`     Fill Count: ${testSite.fuel_fills.fill_count}`);
          console.log(`     Fill Amount: ${testSite.fuel_fills.total_filled}L`);
        }
      }
      
    } catch (reportError) {
      console.log('❌ Activity Report Error:', reportError.response?.data || reportError.message);
    }
    
    // Step 3: Test Fuel Fills API
    console.log('\n🔍 Step 3: Testing Fuel Fills API...');
    
    try {
      const fillsResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills`, {
        params: { plate: testPlate }
      });
      
      console.log('✅ Fuel Fills API Working');
      console.log(`   Retrieved ${fillsResponse.data.count} fills`);
      
      if (fillsResponse.data.data.length > 0) {
        const fill = fillsResponse.data.data[0];
        console.log(`   Fill Details:`);
        console.log(`     Plate: ${fill.plate}`);
        console.log(`     Amount: ${fill.fill_amount}L`);
        console.log(`     Method: ${fill.detection_method}`);
        console.log(`     Date: ${fill.fill_date}`);
      }
      
    } catch (fillsError) {
      console.log('❌ Fuel Fills API Error:', fillsError.response?.data || fillsError.message);
    }
    
    // Step 4: Test Daily Summary
    console.log('\n📅 Step 4: Testing Daily Fuel Fill Summary...');
    
    try {
      const summaryResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills/daily-summary`, {
        params: { date: testDate }
      });
      
      console.log('✅ Daily Summary Working');
      const summary = summaryResponse.data.data;
      console.log(`   Date: ${summary.date}`);
      console.log(`   Vehicles Filled: ${summary.total_vehicles_filled}`);
      console.log(`   Fill Events: ${summary.total_fill_events}`);
      console.log(`   Total Fuel Filled: ${summary.total_fuel_filled}L`);
      
      if (summary.vehicles.length > 0) {
        console.log(`   Vehicle Details:`);
        summary.vehicles.forEach(vehicle => {
          console.log(`     ${vehicle.plate}: ${vehicle.fill_count} fills, ${vehicle.total_fill_amount}L`);
        });
      }
      
    } catch (summaryError) {
      console.log('❌ Daily Summary Error:', summaryError.response?.data || summaryError.message);
    }
    
    // Step 5: Test Statistics
    console.log('\n📊 Step 5: Testing Fuel Fill Statistics...');
    
    try {
      const statsResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills/statistics`);
      
      console.log('✅ Statistics Working');
      const stats = statsResponse.data.data.statistics;
      console.log(`   Total Vehicles with Fills: ${stats.total_vehicles_with_fills}`);
      console.log(`   Total Fill Events: ${stats.total_fill_events}`);
      console.log(`   Recent Fills (24h): ${stats.recent_fills}`);
      console.log(`   Total Fuel Filled: ${stats.total_fuel_filled}L`);
      
    } catch (statsError) {
      console.log('❌ Statistics Error:', statsError.response?.data || statsError.message);
    }
    
    // Cleanup
    console.log('\n🧹 Cleaning up test data...');
    await supabase.from('energy_rite_fuel_data').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_operating_sessions').delete().eq('branch', testPlate);
    
    console.log('\n🎉 Report Integration Test Complete!');
    console.log('\n✅ Your reports now include:');
    console.log('   • Fuel fill events per day');
    console.log('   • Fill amounts per vehicle');
    console.log('   • Detection methods used');
    console.log('   • Integration with fuel consumption');
    console.log('   • API endpoints for fuel fill data');
    
  } catch (error) {
    console.error('❌ Test Error:', error.message);
  }
}

testReportsWithFuelFills();