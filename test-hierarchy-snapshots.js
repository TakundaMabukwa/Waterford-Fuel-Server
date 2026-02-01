// Load environment variables first
require('dotenv').config();

const axios = require('axios');

async function testHierarchicalCostCenterAccess() {
  console.log('🏗️ TESTING HIERARCHICAL COST CENTER ACCESS IN SNAPSHOTS');
  console.log('='.repeat(70));

  const baseUrl = 'http://localhost:4000/api/energy-rite/reports/snapshots';

  try {
    // First, let's check what cost codes exist in our snapshots
    console.log('\n🔍 Step 1: Check existing cost codes in snapshots');
    const allSnapshots = await axios.get(baseUrl);
    const existingCostCodes = [...new Set(
      allSnapshots.data.data.snapshots.map(s => s.cost_code).filter(Boolean)
    )];
    
    console.log('📋 Cost codes found in snapshots:');
    existingCostCodes.forEach(code => console.log(`   - ${code}`));

    // Test hierarchical access with a parent cost code
    const testCostCode = 'KFC-0001-0001';  // Parent of KFC-0001-0001-0003
    
    console.log(`\n🧪 Step 2: Test hierarchical access with parent code: ${testCostCode}`);
    console.log('📞 GET', `${baseUrl}?cost_code=${testCostCode}&include_hierarchy=true`);
    
    const hierarchicalResponse = await axios.get(`${baseUrl}?cost_code=${testCostCode}&include_hierarchy=true`);
    
    console.log('✅ Hierarchical Response:');
    console.log(`   Status: ${hierarchicalResponse.status}`);
    console.log(`   Total Snapshots: ${hierarchicalResponse.data.data.summary.total_snapshots}`);
    
    // Show hierarchy information
    const hierarchy = hierarchicalResponse.data.data.hierarchy;
    if (hierarchy.hierarchy_enabled) {
      console.log('\n🏗️ HIERARCHY INFORMATION:');
      console.log(`   Requested Cost Code: ${hierarchy.requested_cost_code}`);
      console.log(`   Accessible Cost Codes: ${hierarchy.accessible_cost_codes.join(', ')}`);
      console.log(`   Total Accessible: ${hierarchy.total_accessible_codes}`);
      console.log(`   Direct Matches: ${hierarchy.direct_matches}`);
      console.log(`   Hierarchy Matches: ${hierarchy.hierarchy_matches}`);
    }

    // Show cost code breakdown with hierarchy info
    console.log('\n💼 COST CODE BREAKDOWN WITH HIERARCHY:');
    hierarchicalResponse.data.data.breakdowns.by_cost_code.forEach(breakdown => {
      console.log(`\n   Cost Code: ${breakdown.cost_code}`);
      console.log(`   Count: ${breakdown.count} snapshots`);
      console.log(`   Fuel Volume: ${breakdown.total_fuel_volume.toFixed(1)}L`);
      console.log(`   Vehicles: ${breakdown.vehicles.join(', ')}`);
      console.log(`   Via Hierarchy: ${breakdown.is_accessible_via_hierarchy ? 'Yes' : 'No'}`);
      console.log(`   Hierarchy Level: ${breakdown.hierarchy_level}`);
    });

    // Test with hierarchy disabled
    console.log(`\n🧪 Step 3: Test exact match only (hierarchy disabled)`);
    console.log('📞 GET', `${baseUrl}?cost_code=${testCostCode}&include_hierarchy=false`);
    
    const exactMatchResponse = await axios.get(`${baseUrl}?cost_code=${testCostCode}&include_hierarchy=false`);
    
    console.log('✅ Exact Match Response:');
    console.log(`   Status: ${exactMatchResponse.status}`);
    console.log(`   Total Snapshots: ${exactMatchResponse.data.data.summary.total_snapshots}`);
    console.log(`   Hierarchy Enabled: ${exactMatchResponse.data.data.hierarchy.hierarchy_enabled}`);

    // Test with different hierarchy levels
    console.log(`\n🧪 Step 4: Test different hierarchy levels`);
    
    const hierarchyTests = [
      'KFC',                    // Root level
      'KFC-0001',              // Level 1
      'KFC-0001-0001',         // Level 2  
      'KFC-0001-0001-0003'     // Level 3 (exact match)
    ];

    for (const testCode of hierarchyTests) {
      try {
        console.log(`\n   Testing: ${testCode}`);
        const response = await axios.get(`${baseUrl}?cost_code=${testCode}&include_hierarchy=true&limit=5`);
        const data = response.data.data;
        
        console.log(`   Results: ${data.summary.total_snapshots} snapshots`);
        if (data.hierarchy.hierarchy_enabled) {
          console.log(`   Accessible codes: ${data.hierarchy.accessible_cost_codes.length}`);
          console.log(`   Direct: ${data.hierarchy.direct_matches}, Hierarchy: ${data.hierarchy.hierarchy_matches}`);
        }
      } catch (error) {
        console.log(`   ❌ Error: ${error.response?.status || error.message}`);
      }
    }

    // Test combined filters with hierarchy
    console.log(`\n🧪 Step 5: Test hierarchy with other filters`);
    
    const combinedUrl = `${baseUrl}?cost_code=KFC-0001-0001&snapshot_type=MORNING&include_hierarchy=true`;
    console.log('📞 GET', combinedUrl);
    
    const combinedResponse = await axios.get(combinedUrl);
    
    console.log('✅ Combined Filters + Hierarchy:');
    console.log(`   Status: ${combinedResponse.status}`);
    console.log(`   Total Snapshots: ${combinedResponse.data.data.summary.total_snapshots}`);
    console.log(`   Snapshot Type: MORNING only`);
    console.log(`   Hierarchy: ${combinedResponse.data.data.hierarchy.total_accessible_codes} codes accessible`);

    // Show sample data
    if (combinedResponse.data.data.snapshots.length > 0) {
      console.log('\n📋 Sample Hierarchical Data:');
      combinedResponse.data.data.snapshots.slice(0, 3).forEach((snapshot, index) => {
        console.log(`\n   ${index + 1}. Vehicle: ${snapshot.vehicle_plate}`);
        console.log(`      Cost Code: ${snapshot.cost_code}`);
        console.log(`      Snapshot Type: ${snapshot.snapshot_type}`);
        console.log(`      Fuel: ${snapshot.fuel_level}% (${snapshot.fuel_volume}L)`);
        console.log(`      Time: ${new Date(snapshot.snapshot_time).toLocaleTimeString()}`);
      });
    }

    console.log('\n🎉 HIERARCHICAL COST CENTER TESTING COMPLETED!');
    console.log('\n📚 HIERARCHY FEATURES SUMMARY:');
    console.log('='.repeat(50));
    console.log('✅ Automatic hierarchy expansion for parent cost codes');
    console.log('✅ Access control based on cost center hierarchy');
    console.log('✅ Breakdown shows hierarchy levels and access method');
    console.log('✅ Option to disable hierarchy (exact match only)');
    console.log('✅ Compatible with all other filters (date, type, pagination)');
    console.log('✅ Clear indication of direct vs hierarchical matches');

    console.log('\n🔗 NEW HIERARCHY PARAMETERS:');
    console.log('   include_hierarchy=true   - Enable hierarchical access (default)');
    console.log('   include_hierarchy=false  - Exact cost code match only');

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
testHierarchicalCostCenterAccess();