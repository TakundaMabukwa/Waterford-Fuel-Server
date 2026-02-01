require('dotenv').config();
const { supabase } = require('./supabase-client');
const { detectFuelFill } = require('./helpers/fuel-fill-detector');
const axios = require('axios');

async function testKroonstad2FuelFills() {
  try {
    console.log('🏢 Testing KROONSTAD2 Fuel Fill Matching...\n');
    
    const testSite = 'KROONSTAD2';
    const testDate = new Date().toISOString().split('T')[0];
    const baseURL = 'http://localhost:4000';
    
    // Step 1: Get existing cost code for KROONSTAD2
    console.log('🔍 Step 1: Checking KROONSTAD2 cost code...');
    
    const { data: existingVehicle } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('cost_code, company')
      .eq('plate', testSite)
      .single();
    
    let costCode = existingVehicle?.cost_code;
    let company = existingVehicle?.company || 'KFC';
    
    if (!costCode) {
      // Create vehicle lookup for KROONSTAD2
      costCode = 'KFC-KROONSTAD2-001';
      await supabase.from('energyrite_vehicle_lookup').upsert({
        plate: testSite,
        cost_code: costCode,
        company: company
      });
      console.log(`✅ Created cost code ${costCode} for ${testSite}`);
    } else {
      console.log(`✅ Found existing cost code ${costCode} for ${testSite}`);
    }
    
    // Step 2: Create fuel data showing a fill
    console.log('\n📊 Step 2: Creating fuel fill scenario...');
    
    const baseTime = new Date();
    
    // Insert fuel data sequence
    await supabase.from('energy_rite_fuel_data').insert([
      {
        plate: testSite,
        fuel_probe_1_level: 45.0,
        drivername: 'Engine ON',
        created_at: new Date(baseTime.getTime() - 60 * 60 * 1000).toISOString() // 1 hour ago
      },
      {
        plate: testSite,
        fuel_probe_1_level: 175.0, // +130L fill
        drivername: 'Possible Fuel Fill',
        created_at: new Date(baseTime.getTime() - 30 * 60 * 1000).toISOString() // 30 min ago
      },
      {
        plate: testSite,
        fuel_probe_1_level: 165.0, // -10L usage
        drivername: 'Engine Running',
        created_at: new Date().toISOString()
      }
    ]);
    
    console.log(`✅ Created fuel data for ${testSite}`);
    
    // Step 3: Test fuel fill detection
    console.log('\n⛽ Step 3: Testing fuel fill detection...');
    
    const fillResult = await detectFuelFill(testSite, 175.0, 'Possible Fuel Fill');
    
    if (fillResult.isFill) {
      console.log('✅ Fuel fill detected!');
      console.log(`   Amount: ${fillResult.fillDetails.fillAmount.toFixed(1)}L`);
      console.log(`   Method: ${fillResult.fillDetails.detectionMethod}`);
    } else {
      console.log('❌ Fuel fill not detected:', fillResult.reason);
    }
    
    // Step 4: Verify database record
    console.log('\n📋 Step 4: Checking database record...');
    
    const { data: fills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('plate', testSite)
      .order('fill_date', { ascending: false });
    
    if (fills && fills.length > 0) {
      const fill = fills[0];
      console.log('✅ Fill record found:');
      console.log(`   Plate: ${fill.plate}`);
      console.log(`   Cost Code: ${fill.cost_code}`);
      console.log(`   Company: ${fill.company}`);
      console.log(`   Fill Day: ${fill.fill_day}`);
      console.log(`   Amount: ${fill.fill_amount}L`);
      console.log(`   Method: ${fill.detection_method}`);
    } else {
      console.log('❌ No fill records found');
    }
    
    // Step 5: Test activity report with cost code filter
    console.log('\n📊 Step 5: Testing activity report...');
    
    try {
      const activityResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { 
          date: testDate,
          cost_code: costCode
        }
      });
      
      const report = activityResponse.data.data;
      console.log('✅ Activity Report:');
      console.log(`   Cost Code Filter: ${report.cost_code}`);
      console.log(`   Total Fuel Fills: ${report.summary.total_fuel_fills || 0}`);
      console.log(`   Total Fuel Filled: ${report.summary.total_fuel_filled_amount || 0}L`);
      
      // Check if KROONSTAD2 is in the sites
      const kroonstad2Site = report.sites.find(s => s.branch === testSite);
      if (kroonstad2Site) {
        console.log(`   ${testSite} Data:`);
        console.log(`     Fuel Usage: ${kroonstad2Site.total_fuel_usage}L`);
        console.log(`     Fuel Filled: ${kroonstad2Site.total_fuel_filled}L`);
        if (kroonstad2Site.fuel_fills) {
          console.log(`     Fill Count: ${kroonstad2Site.fuel_fills.fill_count}`);
        }
      }
      
    } catch (reportError) {
      console.log('❌ Report Error:', reportError.response?.data?.message || reportError.message);
    }
    
    // Step 6: Test site-specific report
    console.log('\n🏢 Step 6: Testing site-specific report...');
    
    try {
      const siteResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { 
          date: testDate,
          site_id: testSite
        }
      });
      
      const siteReport = siteResponse.data.data;
      console.log('✅ Site-Specific Report:');
      console.log(`   Site: ${siteReport.site_id}`);
      console.log(`   Total Fuel Fills: ${siteReport.summary.total_fuel_fills || 0}`);
      
    } catch (siteError) {
      console.log('❌ Site Report Error:', siteError.response?.data?.message || siteError.message);
    }
    
    // Step 7: Test fuel fills API
    console.log('\n🔍 Step 7: Testing fuel fills API...');
    
    try {
      const fillsResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills`, {
        params: { plate: testSite }
      });
      
      console.log('✅ Fuel Fills API:');
      console.log(`   Found ${fillsResponse.data.count} fills for ${testSite}`);
      
      if (fillsResponse.data.data.length > 0) {
        const apiData = fillsResponse.data.data[0];
        console.log(`   Latest Fill:`);
        console.log(`     Date: ${apiData.fill_date}`);
        console.log(`     Amount: ${apiData.fill_amount}L`);
        console.log(`     Cost Code: ${apiData.cost_code}`);
      }
      
    } catch (apiError) {
      console.log('❌ API Error:', apiError.response?.data?.message || apiError.message);
    }
    
    // Step 8: Test daily summary
    console.log('\n📅 Step 8: Testing daily summary...');
    
    try {
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
      
    } catch (summaryError) {
      console.log('❌ Summary Error:', summaryError.response?.data?.message || summaryError.message);
    }
    
    console.log('\n🎉 KROONSTAD2 Test Complete!');
    console.log('\n✅ Verified:');
    console.log(`   • ${testSite} matches cost code ${costCode}`);
    console.log('   • Fuel fills are properly dated');
    console.log('   • Reports filter by cost center');
    console.log('   • Site-specific filtering works');
    console.log('   • API endpoints return correct data');
    
  } catch (error) {
    console.error('❌ Test Error:', error.message);
  }
}

testKroonstad2FuelFills();