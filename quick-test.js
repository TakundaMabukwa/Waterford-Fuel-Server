const excelGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function quickTest() {
  try {
    console.log('Testing template report...');
    const result = await excelGenerator.generateDailyReport();
    console.log('Success:', result.file_name);
  } catch (error) {
    console.error('Error:', error.message);
  }
}

quickTest();