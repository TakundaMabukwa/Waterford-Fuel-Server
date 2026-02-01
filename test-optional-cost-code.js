// Test optional cost_code parameter in downloadActivityReport
const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testOptionalCostCode() {
  console.log('Testing optional cost_code parameter...\n');

  // Test 1: Date only (no cost_code) - should work
  try {
    console.log('1. Testing with date only (no cost_code)...');
    const response = await axios.get(`${BASE_URL}/api/energy-rite/reports/activity/download`, {
      params: {
        date: '2025-11-07'
      },
      headers: { 'Accept': 'application/json' },
      responseType: 'json'
    });
    
    console.log('✅ Request succeeded without cost_code');
    console.log(`Status: ${response.status}`);
    console.log(`Content Type: ${response.headers['content-type']}`);
    console.log(`Filename: ${response.headers['content-disposition']}`);
    console.log(`Sites found: ${response.data.sites ? response.data.sites.length : 'Unknown'}`);
    console.log();
  } catch (error) {
    console.log('❌ Request failed:', error.response?.data?.message || error.message);
    console.log();
  }

  // Test 2: Date + specific cost_code - should work
  try {
    console.log('2. Testing with date + specific cost_code...');
    const response = await axios.get(`${BASE_URL}/api/energy-rite/reports/activity/download`, {
      params: {
        date: '2025-11-07',
        cost_code: 'KFC-0001-0001-0003'
      },
      headers: { 'Accept': 'application/json' },
      responseType: 'json'
    });
    
    console.log('✅ Request succeeded with specific cost_code');
    console.log(`Status: ${response.status}`);
    console.log(`Content Type: ${response.headers['content-type']}`);
    console.log(`Filename: ${response.headers['content-disposition']}`);
    console.log(`Sites found: ${response.data.sites ? response.data.sites.length : 'Unknown'}`);
    console.log();
  } catch (error) {
    console.log('❌ Request failed:', error.response?.data?.message || error.message);
    console.log();
  }

  // Test 3: Date only, Excel format (should work)
  try {
    console.log('3. Testing Excel download with date only...');
    const response = await axios.head(`${BASE_URL}/api/energy-rite/reports/activity/download`, {
      params: {
        date: '2025-11-07'
      }
    });
    
    console.log('✅ Excel download succeeded without cost_code');
    console.log(`Status: ${response.status}`);
    console.log(`Content Type: ${response.headers['content-type']}`);
    console.log(`Filename: ${response.headers['content-disposition']}`);
    console.log();
  } catch (error) {
    console.log('❌ Excel download failed:', error.response?.data?.message || error.message);
    console.log();
  }

  // Test 4: No date (should still fail)
  try {
    console.log('4. Testing without date parameter...');
    const response = await axios.get(`${BASE_URL}/api/energy-rite/reports/activity/download`, {
      headers: { 'Accept': 'application/json' },
      responseType: 'json'
    });
    
    console.log('❌ Unexpectedly succeeded without date');
    console.log();
  } catch (error) {
    if (error.response?.status === 400) {
      console.log('✅ Correctly rejected:', error.response.data.error);
    } else {
      console.log('❌ Wrong error:', error.response?.data?.message || error.message);
    }
    console.log();
  }

  console.log('Test completed!');
}

// Run tests
testOptionalCostCode().catch(console.error);