const XLSX = require('xlsx');

const workbook = XLSX.readFile('./historical-imports/Weekly (4).xlsx');
const sheetName = workbook.SheetNames[0];
const worksheet = workbook.Sheets[sheetName];
const data = XLSX.utils.sheet_to_json(worksheet);

console.log('📊 WEEKLY DATA DEBUG\n');
console.log('Sheet name:', sheetName);
console.log('Total rows:', data.length);
console.log('\nFirst 5 rows:');
data.slice(0, 5).forEach((row, i) => {
  console.log(`Row ${i + 1}:`, JSON.stringify(row, null, 2));
});

console.log('\nColumn headers found:');
if (data.length > 0) {
  console.log(Object.keys(data[0]));
}