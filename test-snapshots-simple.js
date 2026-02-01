const axios = require('axios');

const baseUrl = 'http://localhost:4000';
const costCode = 'KFC-0001-0001-0002';

async function testSnapshots() {
  console.log('Testing Snapshots Endpoint...');
  const url = `${baseUrl}/api/energy-rite/reports/snapshots?start_date=2026-01-01&end_date=2026-01-14&cost_code=${costCode}&include_hierarchy=true`;
  console.log(`URL: ${url}`);
  
  try {
    const response = await axios.get(url);
    const data = response.data.data;
    
    console.log('\n✅ Response received:');
    console.log(`  Total Snapshots: ${data.summary.total_snapshots}`);
    console.log(`  Total Fuel Volume: ${data.summary.total_fuel_volume}L`);
    console.log(`  Hierarchy Enabled: ${data.hierarchy.hierarchy_enabled}`);
    console.log(`  Accessible Cost Codes: ${data.hierarchy.total_accessible_codes}`);
    console.log(`  Direct Matches: ${data.hierarchy.direct_matches}`);
    console.log(`  Hierarchy Matches: ${data.hierarchy.hierarchy_matches}`);
    
    if (data.filters_applied) {
      console.log(`\n  Filters Applied:`);
      console.log(`    Start Date: ${data.filters_applied.start_date}`);
      console.log(`    End Date: ${data.filters_applied.end_date}`);
      console.log(`    Cost Code: ${data.filters_applied.cost_code}`);
      console.log(`    Accessible Codes: ${data.filters_applied.accessible_cost_codes?.length || 0}`);
    }
    
    if (data.summary.total_snapshots > 0) {
      console.log('\n✅ SUCCESS - Snapshots found!');
    } else {
      console.log('\n⚠️  No snapshots found - This might be expected if there are no daily snapshots for this period');
      console.log('   (daily_snapshots table might not have data for January 2026)');
    }
  } catch (error) {
    console.error('\n❌ Error:', error.message);
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Data:', JSON.stringify(error.response.data, null, 2));
    }
  }
}

testSnapshots();
