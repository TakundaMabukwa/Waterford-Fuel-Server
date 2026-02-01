#!/usr/bin/env node
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

async function diagnoseExcelFiles() {
  console.log('🔍 Diagnosing Excel Files...\n');
  
  const importFolder = './historical-imports';
  
  if (!fs.existsSync(importFolder)) {
    console.log('❌ historical-imports folder does not exist');
    return;
  }
  
  const allFiles = fs.readdirSync(importFolder);
  console.log(`📁 Files in folder: ${allFiles.length}`);
  allFiles.forEach(file => console.log(`  - ${file}`));
  
  const excelFiles = allFiles.filter(file => file.endsWith('.xlsx') || file.endsWith('.xls'));
  console.log(`\n📊 Excel files found: ${excelFiles.length}`);
  
  for (const file of excelFiles) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`📄 Analyzing: ${file}`);
    console.log(`${'='.repeat(60)}`);
    
    try {
      const filePath = path.join(importFolder, file);
      const stats = fs.statSync(filePath);
      console.log(`📏 File size: ${stats.size} bytes`);
      
      if (stats.size === 0) {
        console.log('❌ File is empty (0 bytes)');
        continue;
      }
      
      const workbook = XLSX.readFile(filePath);
      console.log(`📋 Sheet names: ${workbook.SheetNames.join(', ')}`);
      
      for (const sheetName of workbook.SheetNames) {
        console.log(`\n📊 Sheet: ${sheetName}`);
        const worksheet = workbook.Sheets[sheetName];
        
        // Get range
        const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1:A1');
        console.log(`📐 Range: ${worksheet['!ref'] || 'Empty'}`);
        console.log(`📏 Rows: ${range.e.r + 1}, Columns: ${range.e.c + 1}`);
        
        // Convert to JSON to see data
        const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
        console.log(`📊 Data rows: ${data.length}`);
        
        if (data.length > 0) {
          console.log('\n📝 First 3 rows:');
          data.slice(0, 3).forEach((row, index) => {
            console.log(`Row ${index + 1}: [${row.join(', ')}]`);
          });
          
          // Try with headers
          const dataWithHeaders = XLSX.utils.sheet_to_json(worksheet);
          console.log(`\n📋 With headers: ${dataWithHeaders.length} objects`);
          if (dataWithHeaders.length > 0) {
            console.log('🔑 Headers:', Object.keys(dataWithHeaders[0]));
            console.log('📄 First row data:', JSON.stringify(dataWithHeaders[0], null, 2));
          }
        } else {
          console.log('❌ No data found in sheet');
        }
      }
      
    } catch (error) {
      console.error(`❌ Error reading ${file}:`, error.message);
    }
  }
}

diagnoseExcelFiles();