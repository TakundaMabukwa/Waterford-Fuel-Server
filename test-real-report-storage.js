require('dotenv').config();
const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testRealReportStorage() {
  console.log('🧪 Testing Real Report Storage System...\n');
  
  try {
    // Test with actual cost code from your system
    const costCode = 'KFC-0001-0001-0002-0004';
    const testDate = '2024-01-15';
    
    // Test 1: Generate daily report with real cost code
    console.log('1️⃣ Testing Daily Report with Real Cost Code...');
    const dailyResponse = await axios.post(`${BASE_URL}/api/energy-rite/report-storage/generate/daily`, {
      cost_code: costCode,
      date: testDate
    });
    
    console.log('✅ Daily Report Generated:');
    console.log(`   Report ID: ${dailyResponse.data.data.report_id}`);
    console.log(`   Cost Code: ${dailyResponse.data.data.cost_code}`);
    console.log(`   Report URL: ${dailyResponse.data.data.report_url}`);
    console.log(`   File Size: ${dailyResponse.data.data.file_size} bytes\n`);
    
    // Test 2: Generate weekly report
    console.log('2️⃣ Testing Weekly Report...');
    const weeklyResponse = await axios.post(`${BASE_URL}/api/energy-rite/report-storage/generate/weekly`, {
      cost_code: costCode,
      week: 3,
      year: 2024
    });
    
    console.log('✅ Weekly Report Generated:');
    console.log(`   Report ID: ${weeklyResponse.data.data.report_id}`);
    console.log(`   Report URL: ${weeklyResponse.data.data.report_url}`);
    console.log(`   File Size: ${weeklyResponse.data.data.file_size} bytes\n`);
    
    // Test 3: Get stored reports for this cost code
    console.log('3️⃣ Testing Stored Reports Retrieval...');
    const storedResponse = await axios.get(`${BASE_URL}/api/energy-rite/report-storage/stored?cost_code=${costCode}`);
    
    console.log('✅ Stored Reports Retrieved:');
    console.log(`   Total Reports: ${storedResponse.data.data.length}`);
    storedResponse.data.data.forEach((report, index) => {
      console.log(`   ${index + 1}. ${report.report_type.toUpperCase()} - ${report.report_date}`);
      console.log(`      URL: ${report.report_url} (${report.file_size} bytes)`);
    });
    console.log();
    
    // Test 4: Access report file content
    if (storedResponse.data.data.length > 0) {
      console.log('4️⃣ Testing Report File Content...');
      const firstReport = storedResponse.data.data[0];
      const fileName = firstReport.report_url.split('/').pop();
      
      const fileResponse = await axios.get(`${BASE_URL}/api/energy-rite/report-storage/files/${fileName}`);
      
      console.log('✅ Report File Content:');
      console.log(`   File Name: ${fileResponse.data.fileName}`);
      console.log(`   Report Type: ${fileResponse.data.data.report_type}`);
      console.log(`   Cost Code: ${fileResponse.data.data.cost_code}`);
      console.log(`   Sessions Count: ${fileResponse.data.data.sessions.length}`);
      console.log(`   Total Fuel Usage: ${fileResponse.data.data.summary.total_fuel_usage} liters`);
      console.log(`   Total Operating Hours: ${fileResponse.data.data.summary.total_operating_hours} hours`);
      
      // Show sample sessions
      if (fileResponse.data.data.sessions.length > 0) {
        console.log('\n   Sample Sessions:');
        fileResponse.data.data.sessions.slice(0, 5).forEach((session, index) => {
          console.log(`   ${index + 1}. ${session.branch} - Status: ${session.session_status || 'N/A'}`);
          console.log(`      Operating Hours: ${session.operating_hours || 0}`);
          console.log(`      Fuel Usage: ${session.total_usage || 0} liters`);
        });
      }
    }
    
    console.log('\n🎉 Real Report Storage Tests Completed Successfully!');
    
  } catch (error) {
    console.error('❌ Test Error:', error.response?.data || error.message);
    if (error.response?.data?.message) {
      console.error('   Details:', error.response.data.message);
    }
  }
}

testRealReportStorage();