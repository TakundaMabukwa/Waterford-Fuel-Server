require('dotenv').config();
const controller = require('./controllers/energy-rite/energyRiteReportStorageController');

async function testControllerDirect() {
  console.log('🧪 Testing Controller Methods Directly...\n');
  
  try {
    // Test daily report data generation
    console.log('1️⃣ Testing Daily Report Data Generation...');
    const dailyData = await controller.getDailyReportData('TEST-001', '2024-01-15');
    
    console.log('✅ Daily Report Data Generated:');
    console.log(`   Report Type: ${dailyData.report_type}`);
    console.log(`   Cost Code: ${dailyData.cost_code}`);
    console.log(`   Date: ${dailyData.date}`);
    console.log(`   Sessions Count: ${dailyData.sessions.length}`);
    console.log(`   Total Fuel Usage: ${dailyData.summary.total_fuel_usage} liters`);
    console.log(`   Total Operating Hours: ${dailyData.summary.total_operating_hours} hours\n`);
    
    // Show first few sessions
    if (dailyData.sessions.length > 0) {
      console.log('   Sample Sessions:');
      dailyData.sessions.slice(0, 3).forEach((session, index) => {
        console.log(`   ${index + 1}. ${session.branch} - Status: ${session.session_status || 'N/A'}`);
        console.log(`      Operating Hours: ${session.operating_hours || 0}`);
        console.log(`      Fuel Usage: ${session.total_usage || 0} liters`);
      });
    }
    
    console.log('\n🎉 Controller Test Completed Successfully!');
    
  } catch (error) {
    console.error('❌ Controller Test Error:', error.message);
    console.error('   Stack:', error.stack);
  }
}

// Run the test
testControllerDirect();