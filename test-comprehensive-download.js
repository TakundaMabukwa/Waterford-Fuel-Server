// Comprehensive test for the updated download endpoint with optional cost_code
const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function runComprehensiveTest() {
  console.log('🚀 Comprehensive Download Endpoint Test\n');
  console.log('Testing updated functionality where cost_code is now optional...\n');

  const testCases = [
    {
      name: 'Date only (no cost_code) - Excel',
      params: { date: '2025-11-07' },
      expectedFilename: 'daily-activity-report-2025-11-07-all-centers.xlsx',
      expectedContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    },
    {
      name: 'Date only (no cost_code) - JSON',
      params: { date: '2025-11-07', format: 'json' },
      expectedFilename: 'daily-activity-report-2025-11-07-all-centers.json',
      expectedContentType: 'application/json'
    },
    {
      name: 'Date + specific cost_code - Excel',
      params: { date: '2025-11-07', cost_code: 'KFC-0001-0001-0003' },
      expectedFilename: 'daily-activity-report-2025-11-07-KFC-0001-0001-0003.xlsx',
      expectedContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    },
    {
      name: 'Date + specific cost_code - JSON',
      params: { date: '2025-11-07', cost_code: 'KFC-0001-0001-0003', format: 'json' },
      expectedFilename: 'daily-activity-report-2025-11-07-KFC-0001-0001-0003.json',
      expectedContentType: 'application/json'
    },
    {
      name: 'Date + cost_code + site_id - Excel',
      params: { date: '2025-11-07', cost_code: 'KFC-0001-0001-0003', site_id: 'WILLOW' },
      expectedFilename: 'daily-activity-report-2025-11-07-KFC-0001-0001-0003-WILLOW.xlsx',
      expectedContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
  ];

  let passedTests = 0;
  let totalTests = testCases.length;

  for (const testCase of testCases) {
    console.log(`🧪 Testing: ${testCase.name}`);
    
    try {
      const response = await axios.head(`${BASE_URL}/api/energy-rite/reports/activity/download`, {
        params: testCase.params
      });

      // Check status
      if (response.status !== 200) {
        console.log(`❌ FAIL: Expected status 200, got ${response.status}`);
        continue;
      }

      // Check content type
      const actualContentType = response.headers['content-type'];
      if (!actualContentType.includes(testCase.expectedContentType.split(';')[0])) {
        console.log(`❌ FAIL: Expected content type '${testCase.expectedContentType}', got '${actualContentType}'`);
        continue;
      }

      // Check filename
      const contentDisposition = response.headers['content-disposition'];
      const filenameMatch = contentDisposition.match(/filename="([^"]+)"/);
      const actualFilename = filenameMatch ? filenameMatch[1] : 'No filename found';
      
      if (!actualFilename.includes(testCase.expectedFilename)) {
        console.log(`❌ FAIL: Expected filename '${testCase.expectedFilename}', got '${actualFilename}'`);
        continue;
      }

      console.log(`✅ PASS: Status ${response.status}, Content-Type matches, Filename correct`);
      console.log(`   📁 Filename: ${actualFilename}`);
      passedTests++;

    } catch (error) {
      console.log(`❌ FAIL: ${error.response?.data?.message || error.message}`);
    }
    
    console.log('');
  }

  // Test error cases
  console.log('🚨 Testing Error Cases:\n');

  try {
    console.log('🧪 Testing: Missing date parameter');
    await axios.head(`${BASE_URL}/api/energy-rite/reports/activity/download`);
    console.log('❌ FAIL: Should have rejected missing date');
  } catch (error) {
    if (error.response?.status === 400) {
      console.log('✅ PASS: Correctly rejected missing date parameter');
      passedTests++;
      totalTests++;
    } else {
      console.log(`❌ FAIL: Wrong error response: ${error.response?.status || error.message}`);
      totalTests++;
    }
  }

  console.log('');

  // Summary
  console.log('📊 Test Summary:');
  console.log(`✅ Passed: ${passedTests}/${totalTests} tests`);
  console.log(`${passedTests === totalTests ? '🎉' : '⚠️'} ${passedTests === totalTests ? 'All tests passed!' : 'Some tests failed'}`);
  
  console.log('\n🔄 Changes Made:');
  console.log('• cost_code parameter is now OPTIONAL');
  console.log('• When no cost_code provided, returns data for ALL accessible cost centers');
  console.log('• Filename changes from "cost_code" to "all-centers" when cost_code not provided');
  console.log('• Excel sheet header shows "All Centers" when cost_code not provided');
  console.log('• date parameter still REQUIRED');
  console.log('• Excel format still default, JSON available with format=json');
}

// Run the comprehensive test
runComprehensiveTest().catch(console.error);