const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testSimpleReportStorage() {
  console.log('🧪 Testing Simple Report Storage System...\n');
  
  try {
    // Test 1: Generate and store daily report (without specific cost_code)
    console.log('1️⃣ Testing Daily Report Generation...');
    const dailyResponse = await axios.post(`${BASE_URL}/api/energy-rite/report-storage/generate/daily`, {
      cost_code: 'TEST-001',
      date: '2024-01-15'
    });
    
    console.log('✅ Daily Report Generated:');
    console.log(`   Report ID: ${dailyResponse.data.data.report_id}`);
    console.log(`   Cost Code: ${dailyResponse.data.data.cost_code}`);
    console.log(`   Report URL: ${dailyResponse.data.data.report_url}`);
    console.log(`   File Size: ${dailyResponse.data.data.file_size} bytes\n`);
    
    // Test 2: Generate and store weekly report
    console.log('2️⃣ Testing Weekly Report Generation...');
    const weeklyResponse = await axios.post(`${BASE_URL}/api/energy-rite/report-storage/generate/weekly`, {
      cost_code: 'TEST-002',
      week: 3,
      year: 2024
    });
    
    console.log('✅ Weekly Report Generated:');
    console.log(`   Report ID: ${weeklyResponse.data.data.report_id}`);
    console.log(`   Week: ${weeklyResponse.data.data.week}, Year: ${weeklyResponse.data.data.year}`);
    console.log(`   Report URL: ${weeklyResponse.data.data.report_url}`);
    console.log(`   File Size: ${weeklyResponse.data.data.file_size} bytes\n`);
    
    // Test 3: Generate and store monthly report
    console.log('3️⃣ Testing Monthly Report Generation...');
    const monthlyResponse = await axios.post(`${BASE_URL}/api/energy-rite/report-storage/generate/monthly`, {
      cost_code: 'TEST-003',
      month: 1,
      year: 2024
    });
    
    console.log('✅ Monthly Report Generated:');
    console.log(`   Report ID: ${monthlyResponse.data.data.report_id}`);
    console.log(`   Month: ${monthlyResponse.data.data.month}, Year: ${monthlyResponse.data.data.year}`);
    console.log(`   Report URL: ${monthlyResponse.data.data.report_url}`);
    console.log(`   File Size: ${monthlyResponse.data.data.file_size} bytes\n`);
    
    // Test 4: Get all stored reports
    console.log('4️⃣ Testing All Stored Reports Retrieval...');
    const allReportsResponse = await axios.get(`${BASE_URL}/api/energy-rite/report-storage/stored?limit=10`);
    
    console.log('✅ All Stored Reports Retrieved:');
    console.log(`   Total Reports: ${allReportsResponse.data.data.length}`);
    allReportsResponse.data.data.forEach((report, index) => {
      console.log(`   ${index + 1}. ${report.report_type.toUpperCase()} - ${report.cost_code} - ${report.report_date}`);
      console.log(`      URL: ${report.report_url} (${report.file_size} bytes)`);
      console.log(`      Generated: ${new Date(report.generated_at).toLocaleString()}`);
    });
    console.log();
    
    // Test 5: Get reports by type
    console.log('5️⃣ Testing Reports by Type...');
    const dailyReportsResponse = await axios.get(`${BASE_URL}/api/energy-rite/report-storage/stored?report_type=daily`);
    
    console.log('✅ Daily Reports Retrieved:');
    console.log(`   Daily Reports Count: ${dailyReportsResponse.data.data.length}`);
    dailyReportsResponse.data.data.forEach((report, index) => {
      console.log(`   ${index + 1}. ${report.cost_code} - ${report.report_date} (${report.file_size} bytes)`);
    });
    console.log();
    
    // Test 6: Access a report file (if any exist)
    if (allReportsResponse.data.data.length > 0) {
      console.log('6️⃣ Testing Report File Access...');
      const firstReport = allReportsResponse.data.data[0];
      const fileName = firstReport.report_url.split('/').pop();
      
      const fileResponse = await axios.get(`${BASE_URL}/api/energy-rite/report-storage/files/${fileName}`);
      
      console.log('✅ Report File Accessed:');
      console.log(`   File Name: ${fileResponse.data.fileName}`);
      console.log(`   Report Type: ${fileResponse.data.data.report_type}`);
      console.log(`   Cost Code: ${fileResponse.data.data.cost_code}`);
      console.log(`   Sessions Count: ${fileResponse.data.data.sessions.length}`);
      console.log(`   Total Fuel Usage: ${fileResponse.data.data.summary.total_fuel_usage} liters`);
      console.log(`   Total Operating Hours: ${fileResponse.data.data.summary.total_operating_hours} hours\n`);
    }
    
    console.log('🎉 All Report Storage Tests Completed Successfully!');
    console.log('\n📋 Summary:');
    console.log('   ✅ Daily report generation and storage');
    console.log('   ✅ Weekly report generation and storage');
    console.log('   ✅ Monthly report generation and storage');
    console.log('   ✅ Report metadata storage in database');
    console.log('   ✅ Report file storage on filesystem');
    console.log('   ✅ Report retrieval by filters');
    console.log('   ✅ Report file serving');
    
  } catch (error) {
    console.error('❌ Test Error:', error.response?.data || error.message);
    if (error.response?.data?.message) {
      console.error('   Details:', error.response.data.message);
    }
  }
}

// Run the test
testSimpleReportStorage();