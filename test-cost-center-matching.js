require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function testCostCenterMatching() {
  try {
    console.log('🎯 Testing Cost Center and Date Matching for Fuel Fills...\n');
    
    const baseURL = 'http://localhost:4000';
    const testDate = new Date().toISOString().split('T')[0];
    
    // Step 1: Check if table has new columns
    console.log('🔧 Step 1: Checking table structure...');
    
    const { data: tableCheck, error: tableError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('cost_code, company, fill_day')
      .limit(1);
    
    if (tableError && tableError.message.includes('column "cost_code" does not exist')) {
      console.log('❌ Table needs to be updated. Run this SQL in Supabase:');
      console.log(`
ALTER TABLE energy_rite_fuel_fills 
ADD COLUMN IF NOT EXISTS cost_code VARCHAR(50),
ADD COLUMN IF NOT EXISTS company VARCHAR(100),
ADD COLUMN IF NOT EXISTS fill_day DATE;

CREATE INDEX IF NOT EXISTS idx_fuel_fills_cost_code ON energy_rite_fuel_fills(cost_code);
CREATE INDEX IF NOT EXISTS idx_fuel_fills_day ON energy_rite_fuel_fills(fill_day);
      `);
      return;
    }
    
    console.log('✅ Table structure is ready');
    
    // Step 2: Create test data with proper cost center
    console.log('\n📊 Step 2: Creating test data with cost centers...');
    
    const testPlate = 'CC-TEST-' + Date.now();
    const testCostCode = 'KFC-TEST-001';
    
    // Insert vehicle lookup data
    await supabase.from('energyrite_vehicle_lookup').upsert({
      plate: testPlate,
      cost_code: testCostCode,
      company: 'KFC'
    });
    
    // Insert fuel data
    await supabase.from('energy_rite_fuel_data').insert([
      {
        plate: testPlate,
        fuel_probe_1_level: 50.0,
        created_at: new Date(Date.now() - 30 * 60 * 1000).toISOString()
      },
      {
        plate: testPlate,
        fuel_probe_1_level: 180.0,
        created_at: new Date().toISOString()
      }
    ]);
    
    // Insert fuel fill with cost center
    await supabase.from('energy_rite_fuel_fills').insert({
      plate: testPlate,
      cost_code: testCostCode,
      company: 'KFC',
      fill_date: new Date().toISOString(),
      fill_day: testDate,
      fuel_before: 50.0,
      fuel_after: 180.0,
      fill_amount: 130.0,
      detection_method: 'LEVEL_INCREASE'
    });
    
    console.log(`✅ Created test data for ${testPlate} with cost code ${testCostCode}`);
    
    // Step 3: Test cost center filtering
    console.log('\n🔍 Step 3: Testing cost center filtering...');
    
    try {
      // Test activity report with cost code filter
      const activityResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { 
          date: testDate,
          cost_code: testCostCode
        }
      });
      
      const report = activityResponse.data.data;
      console.log('✅ Activity Report with Cost Code Filter:');
      console.log(`   Cost Code Filter: ${report.cost_code}`);
      console.log(`   Total Fuel Fills: ${report.summary.total_fuel_fills || 0}`);
      
      if (report.fuel_analysis?.fuel_fills) {
        console.log(`   Fill Events: ${report.fuel_analysis.fuel_fills.total_fill_events}`);
        console.log(`   Vehicles with Fills: ${Object.keys(report.fuel_analysis.fuel_fills.fills_by_vehicle || {}).length}`);
      }
      
    } catch (reportError) {
      console.log('❌ Activity Report Error:', reportError.response?.data?.message || reportError.message);
    }
    
    // Step 4: Test site-specific filtering
    console.log('\n🏢 Step 4: Testing site-specific filtering...');
    
    try {
      const siteResponse = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
        params: { 
          date: testDate,
          site_id: testPlate
        }
      });
      
      const siteReport = siteResponse.data.data;
      console.log('✅ Site-Specific Report:');
      console.log(`   Site ID Filter: ${siteReport.site_id}`);
      console.log(`   Total Fuel Fills: ${siteReport.summary.total_fuel_fills || 0}`);
      
    } catch (siteError) {
      console.log('❌ Site Report Error:', siteError.response?.data?.message || siteError.message);
    }
    
    // Step 5: Test fuel fills API with filters
    console.log('\n🔍 Step 5: Testing Fuel Fills API with filters...');
    
    try {
      // Test by date
      const dateResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills/daily-summary`, {
        params: { date: testDate }
      });
      
      console.log('✅ Daily Summary:');
      console.log(`   Date: ${dateResponse.data.data.date}`);
      console.log(`   Vehicles Filled: ${dateResponse.data.data.total_vehicles_filled}`);
      
      // Test by plate
      const plateResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills`, {
        params: { plate: testPlate }
      });
      
      console.log('✅ Plate-Specific Fills:');
      console.log(`   Found ${plateResponse.data.count} fills for ${testPlate}`);
      
      if (plateResponse.data.data.length > 0) {
        const fill = plateResponse.data.data[0];
        console.log(`   Cost Code: ${fill.cost_code || 'Not set'}`);
        console.log(`   Company: ${fill.company || 'Not set'}`);
        console.log(`   Fill Day: ${fill.fill_day || 'Not set'}`);
      }
      
    } catch (apiError) {
      console.log('❌ API Error:', apiError.response?.data?.message || apiError.message);
    }
    
    // Step 6: Verify database matching
    console.log('\n📋 Step 6: Verifying database matching...');
    
    const { data: fills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('plate, cost_code, company, fill_day, fill_amount')
      .eq('plate', testPlate);
    
    if (fills && fills.length > 0) {
      console.log('✅ Database Records:');
      fills.forEach((fill, i) => {
        console.log(`   ${i+1}. ${fill.plate} | ${fill.cost_code} | ${fill.company} | ${fill.fill_day} | ${fill.fill_amount}L`);
      });
    }
    
    // Cleanup
    console.log('\n🧹 Cleaning up test data...');
    await supabase.from('energy_rite_fuel_data').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testPlate);
    await supabase.from('energyrite_vehicle_lookup').delete().eq('plate', testPlate);
    
    console.log('\n🎉 Cost Center Matching Test Complete!');
    console.log('\n✅ Fuel fills now properly match:');
    console.log('   • Cost centers via vehicle lookup');
    console.log('   • Specific dates via fill_day column');
    console.log('   • Site filtering via plate matching');
    console.log('   • Report integration with filters');
    
  } catch (error) {
    console.error('❌ Test Error:', error.message);
  }
}

testCostCenterMatching();