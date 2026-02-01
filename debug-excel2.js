require('dotenv').config();
const XLSX = require('xlsx');

const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
const worksheet = workbook.Sheets[workbook.SheetNames[0]];
const data = XLSX.utils.sheet_to_json(worksheet);

console.log('🔍 Looking for BALLYCLARE running times around rows 25-35:');
for (let i = 25; i < Math.min(35, data.length); i++) {
  const row = data[i];
  console.log(`Row ${i}:`, JSON.stringify(row, null, 2));
}