const fetch = require('node-fetch');

async function testRequiredParameters() {
  console.log('🔬 Testing Required Parameters for Daily Activity Report Download');
  console.log('='.repeat(60));
  
  const baseUrl = 'http://localhost:4000';
  
  try {
    // Test 1: Missing both date and cost_code (should fail)
    console.log('\n1️⃣ Testing without date and cost_code (should fail)...');
    const noParamsUrl = `${baseUrl}/api/energy-rite/reports/activity/download`;
    const noParamsResponse = await fetch(noParamsUrl);
    const noParamsData = await noParamsResponse.json();
    
    if (!noParamsResponse.ok && noParamsData.error) {
      console.log(`✅ Correctly rejected: ${noParamsData.error} - ${noParamsData.message}`);
    } else {
      console.log('❌ Should have failed but didn\'t');
    }
    
    // Test 2: Missing date only (should fail)
    console.log('\n2️⃣ Testing without date only (should fail)...');
    const noDateUrl = `${baseUrl}/api/energy-rite/reports/activity/download?cost_code=KFC-0001-0001-0003`;
    const noDateResponse = await fetch(noDateUrl);
    const noDateData = await noDateResponse.json();
    
    if (!noDateResponse.ok && noDateData.error) {
      console.log(`✅ Correctly rejected: ${noDateData.error} - ${noDateData.message}`);
    } else {
      console.log('❌ Should have failed but didn\'t');
    }
    
    // Test 3: Missing cost_code only (should fail)
    console.log('\n3️⃣ Testing without cost_code only (should fail)...');
    const noCostCodeUrl = `${baseUrl}/api/energy-rite/reports/activity/download?date=2025-11-07`;
    const noCostCodeResponse = await fetch(noCostCodeUrl);
    const noCostCodeData = await noCostCodeResponse.json();
    
    if (!noCostCodeResponse.ok && noCostCodeData.error) {
      console.log(`✅ Correctly rejected: ${noCostCodeData.error} - ${noCostCodeData.message}`);
    } else {
      console.log('❌ Should have failed but didn\'t');
    }
    
    // Test 4: Valid parameters (should succeed and default to Excel)
    console.log('\n4️⃣ Testing with valid parameters (should succeed)...');
    const validUrl = `${baseUrl}/api/energy-rite/reports/activity/download?date=2025-11-07&cost_code=KFC-0001-0001-0003`;
    const validResponse = await fetch(validUrl);
    
    if (validResponse.ok) {
      const contentType = validResponse.headers.get('content-type');
      const contentDisposition = validResponse.headers.get('content-disposition');
      
      console.log(`✅ Request succeeded`);
      console.log(`📄 Content Type: ${contentType}`);
      console.log(`📥 Content Disposition: ${contentDisposition}`);
      
      if (contentType.includes('spreadsheetml') || contentType.includes('excel')) {
        console.log(`✅ Correctly defaults to Excel format`);
      }
      
      if (contentDisposition && contentDisposition.includes('2025-11-07-KFC-0001-0001-0003')) {
        console.log(`✅ Filename includes date and cost code: ${contentDisposition}`);
      }
    } else {
      const errorData = await validResponse.json();
      console.log(`❌ Valid request failed: ${errorData.error} - ${errorData.message}`);
    }
    
    // Test 5: Test JSON format (explicit)
    console.log('\n5️⃣ Testing JSON format with valid parameters...');
    const jsonUrl = `${baseUrl}/api/energy-rite/reports/activity/download?date=2025-11-07&cost_code=KFC-0001-0001-0003&format=json`;
    const jsonResponse = await fetch(jsonUrl);
    
    if (jsonResponse.ok) {
      const jsonData = await jsonResponse.json();
      console.log(`✅ JSON format succeeded`);
      console.log(`📊 Date: ${jsonData.data.date}`);
      console.log(`🏢 Cost Code: ${jsonData.data.cost_code}`);
      console.log(`📈 Active Sites: ${jsonData.data.summary.active_sites_today}`);
      console.log(`⚡ Total Sessions: ${jsonData.data.summary.total_sessions}`);
    } else {
      const jsonErrorData = await jsonResponse.json();
      console.log(`❌ JSON request failed: ${jsonErrorData.error}`);
    }
    
    // Test 6: Invalid cost code (should fail)
    console.log('\n6️⃣ Testing with invalid cost code (should fail)...');
    const invalidCostUrl = `${baseUrl}/api/energy-rite/reports/activity/download?date=2025-11-07&cost_code=INVALID-CODE`;
    const invalidCostResponse = await fetch(invalidCostUrl);
    
    if (!invalidCostResponse.ok) {
      const invalidCostData = await invalidCostResponse.json();
      console.log(`✅ Correctly rejected invalid cost code: ${invalidCostData.error}`);
    } else {
      console.log('❌ Should have failed with invalid cost code but didn\'t');
    }
    
    console.log('\n🎉 Required Parameters Testing Complete!');
    console.log('📋 Summary: Both date and cost_code are now REQUIRED parameters');
    console.log('📄 Default format: Excel (.xlsx)');
    console.log('🎯 Alternative format: JSON (explicit format=json)');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testRequiredParameters();