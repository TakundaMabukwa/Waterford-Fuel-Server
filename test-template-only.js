const ExcelJS = require('exceljs');
const path = require('path');

async function testTemplate() {
  try {
    console.log('Loading template...');
    const templatePath = path.join(__dirname, 'fuel_report.xlsx');
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(templatePath);
    
    console.log('Template loaded successfully');
    console.log('Worksheets:', workbook.worksheets.map(ws => ws.name));
    
    const worksheet = workbook.getWorksheet(1);
    console.log('First worksheet name:', worksheet.name);
    console.log('Row count:', worksheet.rowCount);
    
    // Save as new file quickly
    const outputPath = path.join(__dirname, 'temp', 'test_output.xlsx');
    await workbook.xlsx.writeFile(outputPath);
    console.log('Test file saved:', outputPath);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

testTemplate();