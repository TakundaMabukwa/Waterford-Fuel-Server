require('dotenv').config();
const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function testReportGeneration() {
  try {
    console.log('📊 Testing report generation with Supabase storage...');
    
    const result = await ExcelReportGenerator.generateDailyReport(
      new Date('2024-12-26'), 
      'KFC-0001-0001-0002-0002'
    );
    
    console.log('✅ Report generation result:', JSON.stringify(result, null, 2));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testReportGeneration();