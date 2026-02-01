require('dotenv').config();
const XLSX = require('xlsx');

const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
const worksheet = workbook.Sheets[workbook.SheetNames[0]];
const data = XLSX.utils.sheet_to_json(worksheet);

console.log('📊 First 20 rows of data:');
for (let i = 0; i < Math.min(20, data.length); i++) {
  const row = data[i];
  console.log(`Row ${i}:`, JSON.stringify(row, null, 2));
}

console.log('\n🔍 Looking for BALLYCLARE running times:');
for (let i = 0; i < data.length; i++) {
  const row = data[i];
  const site = row['FUEL REPORT SUMMARY'];
  const dateOrInfo = row['__EMPTY'];
  const timeInfo = row['__EMPTY_1'];
  
  if (site === 'BALLYCLARE' && (dateOrInfo === 'Running Time' || timeInfo?.includes('From:'))) {
    console.log(`Row ${i}: site="${site}", date="${dateOrInfo}", time="${timeInfo}"`);
  }
}