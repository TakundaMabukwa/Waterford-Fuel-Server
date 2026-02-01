// Load environment variables first
require('dotenv').config();

const axios = require('axios');

async function testSnapshotEndpoint() {
  console.log('🧪 TESTING SNAPSHOT DATA ENDPOINT WITH COST CODE FILTERING');
  console.log('='.repeat(70));

  const baseUrl = 'http://localhost:4000/api/energy-rite/reports/snapshots';

  try {
    // Test 1: Get all snapshots (no filters)
    console.log('\n🧪 TEST 1: Get All Snapshots');
    console.log('📞 GET', baseUrl);
    
    const allSnapshots = await axios.get(baseUrl);
    console.log('✅ Response Status:', allSnapshots.status);
    console.log('📊 Total Snapshots:', allSnapshots.data.data.summary.total_snapshots);
    console.log('💼 Cost Code Coverage:', allSnapshots.data.data.summary.cost_code_coverage_percentage + '%');
    console.log('⛽ Total Fuel:', allSnapshots.data.data.summary.total_fuel_volume + 'L');

    // Test 2: Filter by specific cost code
    const testCostCode = 'KFC-0001-0001-0003';
    console.log(`\n🧪 TEST 2: Filter by Cost Code (${testCostCode})`);
    console.log('📞 GET', `${baseUrl}?cost_code=${testCostCode}`);
    
    const costCodeSnapshots = await axios.get(`${baseUrl}?cost_code=${testCostCode}`);
    console.log('✅ Response Status:', costCodeSnapshots.status);
    console.log('📊 Filtered Snapshots:', costCodeSnapshots.data.data.summary.total_snapshots);
    
    // Show cost code breakdown
    if (costCodeSnapshots.data.data.breakdowns.by_cost_code.length > 0) {
      console.log('💼 Cost Code Breakdown:');
      costCodeSnapshots.data.data.breakdowns.by_cost_code.forEach(breakdown => {
        console.log(`   - ${breakdown.cost_code}: ${breakdown.count} snapshots, ${breakdown.total_fuel_volume.toFixed(1)}L fuel`);
        console.log(`     Vehicles: ${breakdown.vehicles.join(', ')}`);
      });
    }

    // Test 3: Filter by snapshot type
    console.log('\n🧪 TEST 3: Filter by Snapshot Type (MORNING)');
    console.log('📞 GET', `${baseUrl}?snapshot_type=MORNING`);
    
    const morningSnapshots = await axios.get(`${baseUrl}?snapshot_type=MORNING`);
    console.log('✅ Response Status:', morningSnapshots.status);
    console.log('📊 Morning Snapshots:', morningSnapshots.data.data.summary.total_snapshots);
    
    // Show type breakdown
    console.log('🕒 Snapshot Type Breakdown:');
    Object.entries(morningSnapshots.data.data.breakdowns.by_snapshot_type).forEach(([type, count]) => {
      console.log(`   - ${type}: ${count} snapshots`);
    });

    // Test 4: Combined filters (cost code + type)
    console.log(`\n🧪 TEST 4: Combined Filters (${testCostCode} + EVENING)`);
    console.log('📞 GET', `${baseUrl}?cost_code=${testCostCode}&snapshot_type=EVENING`);
    
    const combinedSnapshots = await axios.get(`${baseUrl}?cost_code=${testCostCode}&snapshot_type=EVENING`);
    console.log('✅ Response Status:', combinedSnapshots.status);
    console.log('📊 Combined Filter Snapshots:', combinedSnapshots.data.data.summary.total_snapshots);
    
    // Show detailed data for first few snapshots
    if (combinedSnapshots.data.data.snapshots.length > 0) {
      console.log('\n📋 Sample Snapshot Data:');
      combinedSnapshots.data.data.snapshots.slice(0, 3).forEach((snapshot, index) => {
        console.log(`\n   ${index + 1}. Vehicle: ${snapshot.vehicle_plate}`);
        console.log(`      Cost Code: ${snapshot.cost_code}`);
        console.log(`      Fuel: ${snapshot.fuel_level}% (${snapshot.fuel_volume}L)`);
        console.log(`      Engine: ${snapshot.engine_status}`);
        console.log(`      Time: ${new Date(snapshot.snapshot_time).toLocaleTimeString()}`);
      });
    }

    // Test 5: Pagination
    console.log('\n🧪 TEST 5: Pagination (limit=3, offset=0)');
    console.log('📞 GET', `${baseUrl}?limit=3&offset=0`);
    
    const paginatedSnapshots = await axios.get(`${baseUrl}?limit=3&offset=0`);
    console.log('✅ Response Status:', paginatedSnapshots.status);
    console.log('📊 Page Results:', paginatedSnapshots.data.data.snapshots.length);
    console.log('📄 Pagination Info:', paginatedSnapshots.data.data.pagination);

    // Test 6: Specific date
    const today = new Date().toISOString().split('T')[0];
    console.log(`\n🧪 TEST 6: Specific Date (${today})`);
    console.log('📞 GET', `${baseUrl}?date=${today}`);
    
    const dateSnapshots = await axios.get(`${baseUrl}?date=${today}`);
    console.log('✅ Response Status:', dateSnapshots.status);
    console.log('📊 Today\'s Snapshots:', dateSnapshots.data.data.summary.total_snapshots);
    console.log('📅 Date Filter Applied:', dateSnapshots.data.data.filters_applied.date);

    console.log('\n🎉 ALL ENDPOINT TESTS COMPLETED SUCCESSFULLY!');
    console.log('\n📚 ENDPOINT USAGE SUMMARY:');
    console.log('='.repeat(50));
    console.log('🔗 Base URL: /api/energy-rite/reports/snapshots');
    console.log('\n📋 Query Parameters:');
    console.log('   💼 cost_code     - Filter by specific cost code');
    console.log('   📅 date          - Filter by date (YYYY-MM-DD)');
    console.log('   🏷️  snapshot_type - Filter by type (MORNING, MIDDAY, EVENING)');
    console.log('   📊 limit         - Number of results per page (default: 50)');
    console.log('   📄 offset        - Starting position for pagination (default: 0)');
    console.log('\n✨ Examples:');
    console.log('   • All snapshots: GET /api/energy-rite/reports/snapshots');
    console.log('   • By cost code:  GET /api/energy-rite/reports/snapshots?cost_code=KFC-0001-0001-0003');
    console.log('   • Morning only:  GET /api/energy-rite/reports/snapshots?snapshot_type=MORNING');
    console.log('   • Combined:      GET /api/energy-rite/reports/snapshots?cost_code=KFC-001&snapshot_type=EVENING');

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
testSnapshotEndpoint();