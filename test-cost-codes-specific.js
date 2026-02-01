const axios = require('axios');

const baseUrl = 'http://localhost:4000'; // Local server

// Test cost codes from the frontend logs
const testCostCodes = [
  { code: 'KFC-0001-0001-0002', name: 'Gunret' },
  { code: 'KFC-0001-0001-0003', name: 'YUM Equity' }
];

async function testCostCode(costCode, name) {
  console.log('\n' + '='.repeat(70));
  console.log(`Testing Cost Code: ${costCode} (${name})`);
  console.log('='.repeat(70));
  
  try {
    // Test 1: Cumulative Snapshots
    console.log('\n📊 Test 1: Cumulative Snapshots (January 2026)');
    console.log('-'.repeat(70));
    const cumulativeUrl = `${baseUrl}/api/energy-rite/cumulative-snapshots/2026/1?cost_code=${costCode}`;
    console.log(`URL: ${cumulativeUrl}`);
    
    const cumulativeResponse = await axios.get(cumulativeUrl);
    const cumulativeData = cumulativeResponse.data.data;
    
    console.log('\n✅ Response:');
    console.log(`  Period: ${cumulativeData.period}`);
    console.log(`  Cost Code: ${cumulativeData.cost_code}`);
    console.log(`  Accessible Cost Codes: ${cumulativeData.accessible_cost_codes?.join(', ') || 'None'}`);
    console.log(`  Filtered Vehicles: ${cumulativeData.filtered_vehicles || 0}`);
    console.log(`  Total Snapshots: ${cumulativeData.total_snapshots}`);
    console.log('\n⛽ Monthly Fuel Usage:');
    console.log(`  Morning (7-12):    ${cumulativeData.monthly_fuel_usage.morning_7_12}L`);
    console.log(`  Afternoon (12-17): ${cumulativeData.monthly_fuel_usage.afternoon_12_17}L`);
    console.log(`  Evening (17-24):   ${cumulativeData.monthly_fuel_usage.evening_17_24}L`);
    console.log(`  Total Monthly:     ${cumulativeData.monthly_fuel_usage.total_monthly}L`);
    
    const hasData = cumulativeData.monthly_fuel_usage.total_monthly > 0;
    console.log(`\n${hasData ? '✅ HAS FUEL DATA' : '❌ NO FUEL DATA - ISSUE DETECTED'}`);
    
    // Test 2: Enhanced Executive Dashboard
    console.log('\n\n📈 Test 2: Enhanced Executive Dashboard (30 days)');
    console.log('-'.repeat(70));
    const dashboardUrl = `${baseUrl}/api/energy-rite/enhanced-executive-dashboard?cost_code=${costCode}&period=30`;
    console.log(`URL: ${dashboardUrl}`);
    
    const dashboardResponse = await axios.get(dashboardUrl);
    const dashboardData = dashboardResponse.data.data;
    
    console.log('\n✅ Response:');
    console.log(`  Period: ${dashboardData.period.start_date} to ${dashboardData.period.end_date}`);
    console.log(`  Total Sites: ${dashboardData.key_metrics.total_sites_operated}`);
    console.log(`  Litres Used: ${dashboardData.key_metrics.total_litres_used.toFixed(1)}L`);
    console.log(`  Litres Filled: ${dashboardData.key_metrics.total_litres_filled.toFixed(1)}L`);
    console.log(`  Op Hours: ${dashboardData.key_metrics.total_operational_hours.toFixed(1)}h`);
    
    const hasDashboardData = dashboardData.key_metrics.total_litres_used > 0;
    console.log(`\n${hasDashboardData ? '✅ HAS DATA' : '❌ NO DATA - ISSUE DETECTED'}`);
    
    // Test 3: Snapshots with date range
    console.log('\n\n📸 Test 3: Snapshots Endpoint (Date Range)');
    console.log('-'.repeat(70));
    const snapshotsUrl = `${baseUrl}/api/energy-rite/reports/snapshots?start_date=2026-01-01&end_date=2026-01-14&cost_code=${costCode}&include_hierarchy=true`;
    console.log(`URL: ${snapshotsUrl}`);
    
    const snapshotsResponse = await axios.get(snapshotsUrl);
    const snapshotsData = snapshotsResponse.data.data;
    
    console.log('\n✅ Response:');
    console.log(`  Total Snapshots: ${snapshotsData.summary.total_snapshots}`);
    console.log(`  Total Fuel Volume: ${snapshotsData.summary.total_fuel_volume}L`);
    console.log(`  Hierarchy Enabled: ${snapshotsData.hierarchy.hierarchy_enabled}`);
    console.log(`  Accessible Cost Codes: ${snapshotsData.hierarchy.total_accessible_codes}`);
    console.log(`  Direct Matches: ${snapshotsData.hierarchy.direct_matches}`);
    console.log(`  Hierarchy Matches: ${snapshotsData.hierarchy.hierarchy_matches}`);
    
    const hasSnapshotData = snapshotsData.summary.total_snapshots > 0;
    console.log(`\n${hasSnapshotData ? '✅ HAS DATA' : '❌ NO DATA - ISSUE DETECTED'}`);
    
    // Summary
    console.log('\n' + '='.repeat(70));
    console.log(`Summary for ${costCode} (${name})`);
    console.log('='.repeat(70));
    
    const allTestsPassed = hasData && hasDashboardData && hasSnapshotData;
    if (allTestsPassed) {
      console.log('✅ ALL TESTS PASSED - Cost code filtering is working correctly!');
    } else {
      console.log('❌ SOME TESTS FAILED:');
      if (!hasData) console.log('   - Cumulative Snapshots returned 0 fuel data');
      if (!hasDashboardData) console.log('   - Executive Dashboard returned 0 fuel data');
      if (!hasSnapshotData) console.log('   - Snapshots endpoint returned 0 records');
    }
    
    return allTestsPassed;
    
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', JSON.stringify(error.response.data, null, 2));
    }
    return false;
  }
}

async function testAllCostCodes() {
  console.log('\n🧪 Testing Cost Code Filtering with Specific Cost Codes');
  console.log('=' .repeat(70));
  console.log('Server:', baseUrl);
  console.log('Test Date: January 2026');
  console.log('=' .repeat(70));
  
  const results = [];
  
  for (const { code, name } of testCostCodes) {
    const passed = await testCostCode(code, name);
    results.push({ code, name, passed });
    
    // Wait a bit between tests
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  // Final Summary
  console.log('\n\n' + '='.repeat(70));
  console.log('FINAL TEST RESULTS');
  console.log('='.repeat(70));
  
  results.forEach(({ code, name, passed }) => {
    console.log(`${passed ? '✅' : '❌'} ${code} (${name})`);
  });
  
  const allPassed = results.every(r => r.passed);
  console.log('\n' + '='.repeat(70));
  if (allPassed) {
    console.log('✅ ALL COST CODES PASSED - Filtering is working correctly!');
  } else {
    console.log('❌ SOME COST CODES FAILED - Check server logs for details');
  }
  console.log('='.repeat(70) + '\n');
  
  process.exit(allPassed ? 0 : 1);
}

// Run tests
testAllCostCodes().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
