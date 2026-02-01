require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function testActivitySummary() {
  try {
    console.log('📊 Testing Activity Summary for Sites...\n');
    
    const testDate = new Date().toISOString().split('T')[0];
    const baseURL = 'http://localhost:4000';
    
    // Create test data for multiple sites
    const sites = ['KROONSTAD2', 'SITE-B', 'SITE-C'];
    
    for (const site of sites) {
      // Vehicle lookup
      await supabase.from('energyrite_vehicle_lookup').upsert({
        plate: site,
        cost_code: `KFC-${site}-001`,
        company: 'KFC'
      });
      
      // Operating session
      await supabase.from('energy_rite_operating_sessions').insert({
        branch: site,
        cost_code: `KFC-${site}-001`,
        session_date: testDate,
        session_start_time: new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString(),
        session_end_time: new Date().toISOString(),
        operating_hours: 4.0,
        total_usage: 25.0,
        total_fill: site === 'KROONSTAD2' ? 130.0 : 0,
        session_status: 'COMPLETED'
      });
      
      // Fuel fill (only for KROONSTAD2)
      if (site === 'KROONSTAD2') {
        await supabase.from('energy_rite_fuel_fills').insert({
          plate: site,
          cost_code: `KFC-${site}-001`,
          company: 'KFC',
          fill_date: new Date().toISOString(),
          fuel_before: 45,
          fuel_after: 175,
          fill_amount: 130,
          detection_method: 'STATUS_INDICATOR'
        });
      }
    }
    
    console.log('✅ Test data created for 3 sites');
    
    // Test activity report
    const response = await axios.get(`${baseURL}/api/energy-rite/reports/activity`, {
      params: { date: testDate }
    });
    
    const report = response.data.data;
    
    console.log('\n📋 Activity Summary:');
    console.log(`Date: ${report.date}`);
    console.log(`Total Sites: ${report.summary.total_sites}`);
    console.log(`Total Operating Hours: ${report.summary.total_operating_hours}h`);
    console.log(`Total Fuel Usage: ${report.summary.total_fuel_usage}L`);
    console.log(`Total Fuel Fills: ${report.summary.total_fuel_fills || 0}`);
    
    console.log('\n🏢 Sites Activity:');
    report.sites.forEach(site => {
      console.log(`\n${site.branch}:`);
      console.log(`  Operating Hours: ${site.total_operating_hours}h`);
      console.log(`  Fuel Usage: ${site.total_fuel_usage}L`);
      console.log(`  Fuel Filled: ${site.total_fuel_filled}L`);
      console.log(`  Status: ${site.total_operating_hours > 0 ? 'ACTIVE' : 'INACTIVE'}`);
    });
    
    // Cleanup
    for (const site of sites) {
      await supabase.from('energy_rite_operating_sessions').delete().eq('branch', site);
      await supabase.from('energy_rite_fuel_fills').delete().eq('plate', site);
    }
    
    console.log('\n🎯 Use this endpoint:');
    console.log(`GET ${baseURL}/api/energy-rite/reports/activity?date=${testDate}`);
    
  } catch (error) {
    console.error('❌ Error:', error.response?.data || error.message);
  }
}

testActivitySummary();