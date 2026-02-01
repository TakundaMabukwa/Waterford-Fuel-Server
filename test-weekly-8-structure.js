require('dotenv').config();
const XLSX = require('xlsx');

console.log('📂 Testing Weekly (8).xlsx structure...\n');

const workbook = XLSX.readFile('./historical-imports/Weekly (8).xlsx');
const sheetName = workbook.SheetNames[0];
const worksheet = workbook.Sheets[sheetName];
const data = XLSX.utils.sheet_to_json(worksheet, { raw: false });

console.log(`📊 Total rows: ${data.length}\n`);

// Find rows with 2026-01-11 and 2026-01-12
console.log('🔍 Looking for 2026-01-11 and 2026-01-12 data:\n');

for (let i = 0; i < data.length; i++) {
  const row = data[i];
  const col1 = row['FUEL REPORT SUMMARY'] || row[Object.keys(row)[0]];
  const col2 = row['__EMPTY'] || row[Object.keys(row)[1]];
  
  if (col2 === '2026-01-11' || col2 === '2026-01-12') {
    console.log(`Row ${i + 1}: ${col1} - ${col2}`);
    console.log('  Opening Fuel:', row['__EMPTY_3'] || row[Object.keys(row)[4]]);
    console.log('  Closing Fuel:', row['__EMPTY_5'] || row[Object.keys(row)[6]]);
    console.log('  Operating Hours:', row['__EMPTY_1'] || row[Object.keys(row)[2]]);
    console.log('  Full row:', JSON.stringify(row, null, 2));
    console.log('---\n');
  }
  
  // Check for Running Time rows after date rows
  if (col1 && col2 === 'Running Time') {
    console.log(`Row ${i + 1}: ${col1} - Running Time`);
    console.log('  From:', row['__EMPTY_1'] || row[Object.keys(row)[2]]);
    console.log('  To:', row['__EMPTY_2'] || row[Object.keys(row)[3]]);
    console.log('---\n');
  }
}