require('dotenv').config();
const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');
const emailService = require('./services/energy-rite/emailService');

async function testReportWithEmail() {
  try {
    console.log('📊 Testing Report Generation with Email...\n');
    
    // Generate a daily report
    console.log('1️⃣ Generating daily report...');
    const report = await ExcelReportGenerator.generateDailyReport(
      new Date('2025-10-30'), 
      'KFC-0001-0001-0002-0004'
    );
    
    console.log('✅ Report generated:', report.file_name);
    console.log('📊 Stats:', report.stats);
    console.log('🔗 Download URL:', report.download_url);
    console.log('');
    
    // Send email with report
    console.log('2️⃣ Sending report via email...');
    const emailResult = await emailService.sendReportEmail({
      reportType: 'daily',
      period: '2025-10-30',
      filePath: `./temp/${report.file_name}`,
      fileName: report.file_name,
      downloadUrl: report.public_url || report.download_url,
      costCode: 'KFC-0001-0001-0002-0004',
      stats: report.stats
    });
    
    if (emailResult.success) {
      console.log('✅ Email sent successfully!');
      console.log(`📧 Sent to ${emailResult.recipients} recipients`);
      console.log('📧 Message ID:', emailResult.messageId);
    } else {
      console.log('❌ Email failed:', emailResult.error);
    }
    
    console.log('\n🎉 Complete Report + Email Test Finished!');
    
  } catch (error) {
    console.error('❌ Test error:', error.message);
  }
}

testReportWithEmail();