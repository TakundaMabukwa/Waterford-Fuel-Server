require('dotenv').config();
const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function testWeekly() {
  try {
    console.log('📊 Generating weekly report...');
    
    const result = await ExcelReportGenerator.generateWeeklyReport(
      new Date('2024-12-29'), 
      'KFC-0001-0001-0002-0002'
    );
    
    console.log('✅ Weekly report result:', JSON.stringify(result, null, 2));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testWeekly();