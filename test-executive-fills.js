/**
 * Test Executive Dashboard Fuel Fills with Cost Codes
 */

const axios = require('axios');

async function testExecutiveDashboard() {
  try {
    const baseUrl = 'http://localhost:4000';
    const costCode = 'KFC-0001-0001-0002';
    const startDate = '2025-12-31';
    const endDate = '2026-01-30';
    
    console.log('🧪 Testing Executive Dashboard Fuel Fills');
    console.log('='.repeat(70));
    console.log(`Cost Code: ${costCode}`);
    console.log(`Date Range: ${startDate} to ${endDate}`);
    console.log();
    
    const url = `${baseUrl}/api/energy-rite/enhanced-executive-dashboard?start_date=${startDate}&end_date=${endDate}&cost_code=${costCode}`;
    console.log(`📊 URL: ${url}\n`);
    
    const response = await axios.get(url);
    const data = response.data.data;
    
    console.log('✅ Response received\n');
    console.log('=' .repeat(70));
    console.log('KEY METRICS');
    console.log('='.repeat(70));
    console.log(`Total Sites Operated: ${data.key_metrics.total_sites_operated}`);
    console.log(`Total Litres Used: ${data.key_metrics.total_litres_used}L`);
    console.log(`Total Litres Filled: ${data.key_metrics.total_litres_filled}L`);
    console.log(`Total Fill Events: ${data.key_metrics.total_fuel_fill_events}`);
    console.log(`Sites with Fills: ${data.key_metrics.sites_with_fuel_fills}`);
    console.log(`Net Consumption: ${data.key_metrics.net_fuel_consumption}L`);
    console.log(`Operating Hours: ${data.key_metrics.total_operational_hours}h`);
    
    console.log('\n' + '='.repeat(70));
    console.log('FUEL FILLS SUMMARY');
    console.log('='.repeat(70));
    const fills = data.fuel_tracking.fuel_fills_summary;
    console.log(`Total Fill Events: ${fills.total_fill_events}`);
    console.log(`Total Litres Filled: ${fills.total_litres_filled}L`);
    console.log(`Sites with Fills: ${fills.sites_with_fills}`);
    console.log(`Average Fill Amount: ${fills.average_fill_amount}L`);
    
    console.log('\n' + '='.repeat(70));
    console.log('FUEL EFFICIENCY');
    console.log('='.repeat(70));
    const efficiency = data.fuel_tracking.fuel_efficiency;
    console.log(`Total Used: ${efficiency.total_used}L`);
    console.log(`Total Filled: ${efficiency.total_filled}L`);
    console.log(`Net Consumption: ${efficiency.net_consumption}L`);
    console.log(`Usage to Fill Ratio: ${efficiency.usage_to_fill_ratio}`);
    console.log(`Fill Frequency: ${efficiency.fill_frequency} fills/site`);
    
    console.log('\n' + '='.repeat(70));
    console.log('EXECUTIVE INSIGHTS');
    console.log('='.repeat(70));
    data.executive_insights.forEach((insight, i) => {
      console.log(`${i + 1}. ${insight}`);
    });
    
    console.log('\n' + '='.repeat(70));
    console.log('SITE PERFORMANCE (with fills)');
    console.log('='.repeat(70));
    const sitesWithFills = data.site_performance.filter(s => s.fuel_filled_liters > 0);
    if (sitesWithFills.length > 0) {
      console.log(`Found ${sitesWithFills.length} sites with fuel fills:\n`);
      sitesWithFills.slice(0, 5).forEach(site => {
        console.log(`  ${site.site_name}:`);
        console.log(`    Fills: ${site.fuel_fills_count}`);
        console.log(`    Filled: ${site.fuel_filled_liters.toFixed(2)}L`);
        console.log(`    Used: ${site.fuel_usage_liters.toFixed(2)}L`);
        console.log(`    Net: ${site.fuel_net_usage.toFixed(2)}L`);
        console.log();
      });
    } else {
      console.log('⚠️  No sites with fills found');
    }
    
    console.log('='.repeat(70));
    console.log('\n📊 RESULT:');
    
    const hasFills = fills.total_fill_events > 0;
    const hasFilledAmount = fills.total_litres_filled > 0;
    
    if (hasFills && hasFilledAmount) {
      console.log('✅ SUCCESS - Fuel fills are now showing correctly!');
      console.log(`   ${fills.total_fill_events} fill events with ${fills.total_litres_filled}L filled`);
    } else {
      console.log('❌ ISSUE - Still showing 0 fills');
      console.log('   This could mean:');
      console.log('   - No fuel fill sessions in this date range for this cost code');
      console.log('   - Cost code filtering might not be matching vehicles');
      console.log('   - Server needs to be restarted to load new code');
    }
    
  } catch (error) {
    console.error('\n❌ Error:', error.message);
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Data:', JSON.stringify(error.response.data, null, 2));
    }
  }
}

testExecutiveDashboard();
