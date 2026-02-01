const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function testExcelGeneration() {
  console.log('🧪 Testing Excel Report Generation with Session Breakdown...\n');
  
  try {
    // Generate daily report for today
    console.log('📊 Generating daily Excel report...');
    const result = await ExcelReportGenerator.generateDailyReport();
    
    if (result.success) {
      console.log('✅ Excel report generated successfully!');
      console.log(`📁 File: ${result.file_name}`);
      console.log(`🔗 Download URL: ${result.download_url}`);
      console.log(`📊 Stats: ${result.stats.total_sites} sites, ${result.stats.total_sessions} sessions`);
      console.log(`⏱️  Total Hours: ${result.stats.total_operating_hours}h`);
      console.log(`📅 Period: ${result.period}`);
      console.log(`📋 Report ID: ${result.report_id}`);
    } else {
      console.log('❌ Failed to generate Excel report');
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testExcelGeneration();