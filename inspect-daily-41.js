const XLSX = require('xlsx');

console.log('📊 Inspecting Daily (41).xlsx\n');

const workbook = XLSX.readFile('./historical-imports/Daily (41).xlsx');
const sheetName = workbook.SheetNames[0];
const worksheet = workbook.Sheets[sheetName];

console.log('📄 Sheet Name:', sheetName);
console.log('\n=== HEADERS ===');
const data = XLSX.utils.sheet_to_json(worksheet, { raw: false, defval: '' });
if (data.length > 0) {
  console.log('Column Headers:', Object.keys(data[0]));
}

console.log('\n=== FIRST 50 ROWS ===\n');
data.slice(0, 50).forEach((row, idx) => {
  console.log(`Row ${idx + 1}:`, JSON.stringify(row, null, 2));
  console.log('---');
});

console.log(`\n✅ Total rows in file: ${data.length}`);
