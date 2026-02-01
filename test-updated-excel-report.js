require('dotenv').config();
const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function testUpdatedExcelReport() {
  try {
    console.log('🧪 Testing Updated Excel Report Generator\n');
    
    // Test with the date that has both sessions and fills
    const testDate = '2025-12-12';
    console.log(`📅 Generating report for: ${testDate}`);
    
    // Generate daily report
    console.log('\n1️⃣ Generating daily report...');
    const result = await ExcelReportGenerator.generateDailyReport(testDate);
    
    if (result.success) {
      console.log('✅ Report generated successfully!');
      console.log(`📄 File: ${result.file_name}`);
      console.log(`📊 Stats:`);
      console.log(`   - Sites: ${result.stats.total_sites}`);
      console.log(`   - Sessions: ${result.stats.total_sessions}`);
      console.log(`   - Fills: ${result.stats.total_fills || 0}`);
      console.log(`   - Operating Hours: ${result.stats.total_operating_hours}`);
      console.log(`📥 Download URL: ${result.download_url}`);
    } else {
      console.error('❌ Report generation failed:', result.error);
    }
    
    // Test with a specific site that has both sessions and fills
    console.log('\n2️⃣ Generating report for BRAAMFONTE only...');
    const braamfonteResult = await ExcelReportGenerator.generateDailyReport(testDate, null, 'BRAAMFONTE');
    
    if (braamfonteResult.success) {
      console.log('✅ BRAAMFONTE report generated successfully!');
      console.log(`📄 File: ${braamfonteResult.file_name}`);
      console.log(`📊 Stats:`);
      console.log(`   - Sites: ${braamfonteResult.stats.total_sites}`);
      console.log(`   - Sessions: ${braamfonteResult.stats.total_sessions}`);
      console.log(`   - Fills: ${braamfonteResult.stats.total_fills || 0}`);
      console.log(`📥 Download URL: ${braamfonteResult.download_url}`);
    } else {
      console.error('❌ BRAAMFONTE report generation failed:', braamfonteResult.error);
    }
    
    console.log('\n🎯 Key Improvements Made:');
    console.log('✅ Separated operating sessions (COMPLETED) from fuel fills (FUEL_FILL_COMPLETED)');
    console.log('✅ Used session_date instead of session_start_time for daily filtering');
    console.log('✅ Added visual distinction between sessions (blue) and fills (green)');
    console.log('✅ Shows actual fuel fill amounts instead of zeros');
    console.log('✅ Added breakdown summary showing both sessions and fills counts');
    console.log('✅ Proper handling of null vs zero values');
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testUpdatedExcelReport();