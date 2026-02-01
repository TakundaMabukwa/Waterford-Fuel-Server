#!/usr/bin/env node
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

async function testActualExcelStructure() {
  console.log('🔍 Testing Actual Excel Structure...\n');
  
  const importFolder = './historical-imports';
  const excelFiles = fs.readdirSync(importFolder).filter(file => file.endsWith('.xlsx'));
  
  for (const file of excelFiles) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`📄 Analyzing: ${file}`);
    console.log(`${'='.repeat(60)}`);
    
    try {
      const filePath = path.join(importFolder, file);
      const workbook = XLSX.readFile(filePath);
      
      // Focus on the main fuel report sheet (not contact people)
      const mainSheet = workbook.SheetNames.find(name => 
        name.includes('Fuel') || name.includes('YUM') || name.includes('Alchemy') || name.includes('Gunret') || name.includes('Spur')
      );
      
      if (!mainSheet) continue;
      
      console.log(`📊 Analyzing sheet: ${mainSheet}`);
      
      const worksheet = workbook.Sheets[mainSheet];
      const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      
      console.log(`📏 Total rows: ${data.length}`);
      
      // Look for site data patterns
      let siteDataStart = -1;
      let headerRow = -1;
      
      for (let i = 0; i < Math.min(data.length, 50); i++) {
        const row = data[i];
        if (!row || row.length === 0) continue;
        
        const rowText = row.join(' ').toLowerCase();
        
        // Look for headers that might indicate site data
        if (rowText.includes('site') || rowText.includes('operating') || rowText.includes('hours')) {
          console.log(`🎯 Potential header at row ${i + 1}: [${row.join(', ')}]`);
          headerRow = i;
        }
        
        // Look for actual site names
        if (rowText.includes('bergbron') || rowText.includes('beyerspark') || rowText.includes('birchleigh')) {
          console.log(`🏢 Site data found at row ${i + 1}: [${row.join(', ')}]`);
          if (siteDataStart === -1) siteDataStart = i;
        }
      }
      
      // Show some sample rows around where we think the data is
      if (siteDataStart > -1) {
        console.log(`\n📊 Sample data around row ${siteDataStart + 1}:`);
        const startRow = Math.max(0, siteDataStart - 2);
        const endRow = Math.min(data.length, siteDataStart + 10);
        
        for (let i = startRow; i < endRow; i++) {
          const row = data[i];
          if (row && row.length > 0) {
            console.log(`Row ${i + 1}: [${row.join(' | ')}]`);
          }
        }
      }
      
      // Try to find the actual data structure
      console.log('\n🔍 Looking for operating hours pattern...');
      for (let i = 0; i < Math.min(data.length, 100); i++) {
        const row = data[i];
        if (!row || row.length === 0) continue;
        
        const rowText = row.join(' ').toLowerCase();
        if (rowText.includes('hours') && rowText.includes('minutes')) {
          console.log(`⏰ Operating hours found at row ${i + 1}: [${row.join(' | ')}]`);
        }
      }
      
    } catch (error) {
      console.error(`❌ Error analyzing ${file}:`, error.message);
    }
  }
}

testActualExcelStructure();