const axios = require('axios');

const baseUrl = 'http://localhost:4000';
const costCode = 'KFC-0001-0001-0002';

async function testDashboard() {
  console.log('Testing Enhanced Executive Dashboard...');
  console.log(`URL: ${baseUrl}/api/energy-rite/enhanced-executive-dashboard?cost_code=${costCode}&period=30`);
  
  try {
    const response = await axios.get(`${baseUrl}/api/energy-rite/enhanced-executive-dashboard?cost_code=${costCode}&period=30`);
    const data = response.data.data;
    
    console.log('\n✅ Response received:');
    console.log(`  Period: ${data.period.start_date} to ${data.period.end_date}`);
    console.log(`  Total Sites: ${data.key_metrics.total_sites_operated}`);
    console.log(`  Litres Used: ${data.key_metrics.total_litres_used.toFixed(1)}L`);
    console.log(`  Litres Filled: ${data.key_metrics.total_litres_filled.toFixed(1)}L`);
    console.log(`  Op Hours: ${data.key_metrics.total_operational_hours.toFixed(1)}h`);
    
    if (data.key_metrics.total_litres_used > 0) {
      console.log('\n✅ SUCCESS - Dashboard has data!');
    } else {
      console.log('\n❌ FAIL - Dashboard returned 0 fuel usage');
    }
  } catch (error) {
    console.error('\n❌ Error:', error.message);
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Data:', JSON.stringify(error.response.data, null, 2));
    }
  }
}

testDashboard();
