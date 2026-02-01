const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testReportStorage() {
  console.log('🧪 Testing Report Storage System...\n');
  
  try {
    // Test 1: Generate and store daily report
    console.log('1️⃣ Testing Daily Report Generation...');
    const dailyResponse = await axios.post(`${BASE_URL}/api/energy-rite/report-storage/generate/daily`, {
      cost_code: 'KFC-0001-0001-0002-0004',
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
      cost_code: 'KFC-0001-0001-0002-0004',
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
      cost_code: 'KFC-0001-0001-0002-0004',
      month: 1,
      year: 2024
    });
    
    console.log('✅ Monthly Report Generated:');
    console.log(`   Report ID: ${monthlyResponse.data.data.report_id}`);
    console.log(`   Month: ${monthlyResponse.data.data.month}, Year: ${monthlyResponse.data.data.year}`);
    console.log(`   Report URL: ${monthlyResponse.data.data.report_url}`);
    console.log(`   File Size: ${monthlyResponse.data.data.file_size} bytes\n`);
    
    // Test 4: Get stored reports list
    console.log('4️⃣ Testing Stored Reports Retrieval...');
    const storedResponse = await axios.get(`${BASE_URL}/api/energy-rite/report-storage/stored?cost_code=KFC-0001-0001-0002-0004`);
    
    console.log('✅ Stored Reports Retrieved:');
    console.log(`   Total Reports: ${storedResponse.data.data.length}`);
    storedResponse.data.data.forEach((report, index) => {
      console.log(`   ${index + 1}. ${report.report_type.toUpperCase()} - ${report.report_date} (${report.file_size} bytes)`);
      console.log(`      URL: ${report.report_url}`);
      console.log(`      Generated: ${new Date(report.generated_at).toLocaleString()}`);
    });
    console.log();
    
    // Test 5: Access a report file
    if (storedResponse.data.data.length > 0) {
      console.log('5️⃣ Testing Report File Access...');
      const firstReport = storedResponse.data.data[0];
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
    
  } catch (error) {
    console.error('❌ Test Error:', error.response?.data || error.message);
  }
}

// Run the test
testReportStorage();