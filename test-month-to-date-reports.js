require('dotenv').config();
const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testMonthToDateReports() {
  console.log('🧪 Testing Month-to-Date Report Generation\n');
  console.log('='.repeat(70));
  
  const tests = [
    {
      name: 'Previous Month Report',
      payload: {
        report_type: 'monthly',
        cost_code: 'KFC-0001-0001-0002',
        month_type: 'previous'
      }
    },
    {
      name: 'Month-to-Date Report (Current)',
      payload: {
        report_type: 'monthly',
        cost_code: 'KFC-0001-0001-0002',
        month_type: 'current'
      }
    },
    {
      name: 'Month-to-Date Report (All Sites)',
      payload: {
        report_type: 'monthly',
        month_type: 'current'
      }
    }
  ];
  
  for (const test of tests) {
    console.log(`\n📊 Test: ${test.name}`);
    console.log('-'.repeat(70));
    console.log('Payload:', JSON.stringify(test.payload, null, 2));
    
    try {
      const response = await axios.post(
        `${BASE_URL}/api/energy-rite/excel-reports/generate`,
        test.payload,
        {
          headers: { 'Content-Type': 'application/json' },
          timeout: 120000 // 2 minutes
        }
      );
      
      const data = response.data;
      
      if (data.success) {
        console.log('\n✅ SUCCESS');
        console.log(`   File: ${data.data.file_name}`);
        console.log(`   Period: ${data.data.period}`);
        console.log(`   Type: ${data.data.report_type}`);
        console.log(`   Sites: ${data.data.stats.total_sites}`);
        console.log(`   Sessions: ${data.data.stats.total_sessions}`);
        console.log(`   Hours: ${data.data.stats.total_operating_hours}`);
        console.log(`   Download: ${data.data.download_url}`);
      } else {
        console.log('\n❌ FAILED');
        console.log(`   Error: ${data.error || 'Unknown error'}`);
      }
      
    } catch (error) {
      console.log('\n❌ REQUEST FAILED');
      if (error.response) {
        console.log(`   Status: ${error.response.status}`);
        console.log(`   Error: ${error.response.data.error || error.response.data.message}`);
      } else {
        console.log(`   Error: ${error.message}`);
      }
    }
    
    // Wait between requests
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  console.log('\n' + '='.repeat(70));
  console.log('✅ All tests completed');
  console.log('='.repeat(70));
}

// Run tests
testMonthToDateReports().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
