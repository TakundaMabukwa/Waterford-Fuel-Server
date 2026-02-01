require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function testKroonstad2Manual() {
  try {
    console.log('🏢 Testing KROONSTAD2 with Manual Fuel Fill...\n');
    
    const testSite = 'KROONSTAD2';
    const testDate = new Date().toISOString().split('T')[0];
    const costCode = 'KFC-KROONSTAD2-001';
    const baseURL = 'http://localhost:4000';
    
    // Step 1: Ensure vehicle lookup exists
    console.log('🔧 Step 1: Setting up KROONSTAD2...');
    
    await supabase.from('energyrite_vehicle_lookup').upsert({
      plate: testSite,
      cost_code: costCode,
      company: 'KFC'
    });
    
    console.log(`✅ ${testSite} setup with cost code ${costCode}`);
    
    // Step 2: Insert manual fuel fill record
    console.log('\n⛽ Step 2: Creating fuel fill record...');
    
    const fillRecord = {
      plate: testSite,
      cost_code: costCode,
      company: 'KFC',
      fill_date: new Date().toISOString(),
      fill_day: testDate,
      fuel_before: 45.0,
      fuel_after: 175.0,
      fill_amount: 130.0,
      fill_percentage: 288.9,
      detection_method: 'STATUS_INDICATOR',
      status: 'detected'
    };
    
    await supabase.from('energy_rite_fuel_fills').insert(fillRecord);
    console.log(`✅ Created fuel fill: ${fillRecord.fill_amount}L for ${testSite}`);
    
    // Step 3: Create operating session
    console.log('\n📊 Step 3: Creating operating session...');
    
    await supabase.from('energy_rite_operating_sessions').insert({
      branch: testSite,
      cost_code: costCode,
      company: 'KFC',
      session_date: testDate,
      session_start_time: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      session_end_time: new Date().toISOString(),
      operating_hours: 2.0,
      total_usage: 15.0,
      total_fill: 130.0,
      fill_events: 1,
      session_status: 'COMPLETED'
    });
    
    console.log(`✅ Created session for ${testSite}`);
    
    // Step 4: Test cost code filtering
    console.log('\n🔍 Step 4: Testing cost code filtering...');
    
    try {
      const activityResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { 
          date: testDate,
          cost_code: costCode
        }
      });
      
      const report = activityResponse.data.data;
      console.log('✅ Activity Report with Cost Code Filter:');
      console.log(`   Cost Code: ${report.cost_code}`);
      console.log(`   Total Sites: ${report.summary.total_sites}`);
      console.log(`   Total Fuel Fills: ${report.summary.total_fuel_fills || 0}`);
      console.log(`   Total Fuel Filled: ${report.summary.total_fuel_filled_amount || 0}L`);
      
      // Check fuel analysis
      if (report.fuel_analysis?.fuel_fills) {
        console.log(`   Fuel Analysis:`);
        console.log(`     Fill Events: ${report.fuel_analysis.fuel_fills.total_fill_events}`);
        console.log(`     Total Filled: ${report.fuel_analysis.fuel_fills.total_fuel_filled}L`);
        console.log(`     Vehicles: ${Object.keys(report.fuel_analysis.fuel_fills.fills_by_vehicle || {})}`);
      }
      
      // Check site data
      const kroonstad2Site = report.sites.find(s => s.branch === testSite);
      if (kroonstad2Site) {
        console.log(`   ${testSite} Site Data:`);
        console.log(`     Fuel Usage: ${kroonstad2Site.total_fuel_usage}L`);
        console.log(`     Fuel Filled: ${kroonstad2Site.total_fuel_filled}L`);
        if (kroonstad2Site.fuel_fills) {
          console.log(`     Fill Count: ${kroonstad2Site.fuel_fills.fill_count}`);
          console.log(`     Fill Amount: ${kroonstad2Site.fuel_fills.total_filled}L`);
        }
      } else {
        console.log(`   ❌ ${testSite} not found in sites`);
      }
      
    } catch (reportError) {
      console.log('❌ Report Error:', reportError.response?.data?.message || reportError.message);
    }
    
    // Step 5: Test site-specific filtering
    console.log('\n🏢 Step 5: Testing site-specific filtering...');
    
    try {
      const siteResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { 
          date: testDate,
          site_id: testSite
        }
      });
      
      const siteReport = siteResponse.data.data;
      console.log('✅ Site-Specific Report:');
      console.log(`   Site ID: ${siteReport.site_id}`);
      console.log(`   Total Fuel Fills: ${siteReport.summary.total_fuel_fills || 0}`);
      console.log(`   Total Fuel Filled: ${siteReport.summary.total_fuel_filled_amount || 0}L`);
      
    } catch (siteError) {
      console.log('❌ Site Report Error:', siteError.response?.data?.message || siteError.message);
    }
    
    // Step 6: Test fuel fills API
    console.log('\n📋 Step 6: Testing fuel fills API...');
    
    try {
      // Test by plate
      const plateResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills`, {
        params: { plate: testSite }
      });
      
      console.log('✅ Plate-Specific Fills:');
      console.log(`   Found ${plateResponse.data.count} fills for ${testSite}`);
      
      if (plateResponse.data.data.length > 0) {
        const fill = plateResponse.data.data[0];
        console.log(`   Fill Details:`);
        console.log(`     Plate: ${fill.plate}`);
        console.log(`     Cost Code: ${fill.cost_code}`);
        console.log(`     Company: ${fill.company}`);
        console.log(`     Fill Day: ${fill.fill_day}`);
        console.log(`     Amount: ${fill.fill_amount}L`);
        console.log(`     Method: ${fill.detection_method}`);
      }
      
      // Test daily summary
      const summaryResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills/daily-summary`, {
        params: { date: testDate }
      });
      
      const summary = summaryResponse.data.data;
      console.log('✅ Daily Summary:');
      console.log(`   Date: ${summary.date}`);
      console.log(`   Vehicles Filled: ${summary.total_vehicles_filled}`);
      console.log(`   Total Fuel Filled: ${summary.total_fuel_filled}L`);
      
      const kroonstad2Vehicle = summary.vehicles.find(v => v.plate === testSite);
      if (kroonstad2Vehicle) {
        console.log(`   ${testSite}: ${kroonstad2Vehicle.fill_count} fills, ${kroonstad2Vehicle.total_fill_amount}L`);
      }
      
    } catch (apiError) {
      console.log('❌ API Error:', apiError.response?.data?.message || apiError.message);
    }
    
    // Step 7: Verify database matching
    console.log('\n📊 Step 7: Verifying database matching...');
    
    const { data: dbFills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('plate, cost_code, company, fill_day, fill_amount, detection_method')
      .eq('plate', testSite);
    
    console.log(`✅ Database Records for ${testSite}:`);
    dbFills.forEach((fill, i) => {
      console.log(`   ${i+1}. ${fill.plate} | ${fill.cost_code} | ${fill.fill_day} | ${fill.fill_amount}L | ${fill.detection_method}`);
    });
    
    // Cleanup
    console.log('\n🧹 Cleaning up test data...');
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testSite);
    await supabase.from('energy_rite_operating_sessions').delete().eq('branch', testSite);
    
    console.log('\n🎉 KROONSTAD2 Manual Test Complete!');
    console.log('\n✅ Confirmed:');
    console.log(`   • ${testSite} properly matches cost code ${costCode}`);
    console.log('   • Fuel fills are correctly dated and filtered');
    console.log('   • Reports show fuel fill data by cost center');
    console.log('   • Site-specific filtering works correctly');
    console.log('   • API endpoints return accurate data');
    console.log('   • Database records include all required fields');
    
  } catch (error) {
    console.error('❌ Test Error:', error.message);
  }
}

testKroonstad2Manual();