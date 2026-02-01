const excelGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function testTemplateReport() {
  try {
    console.log('🧪 Testing template-based Excel report generation...');
    
    // Generate a daily report using the template
    const result = await excelGenerator.generateDailyReport();
    
    console.log('✅ Template report generated successfully:');
    console.log(JSON.stringify(result, null, 2));
    
  } catch (error) {
    console.error('❌ Error testing template report:', error.message);
  }
}

testTemplateReport();