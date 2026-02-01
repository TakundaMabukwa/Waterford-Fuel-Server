#!/usr/bin/env node
require('dotenv').config();
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

// Cost code mapping for your files
const COST_CODE_MAPPING = {
  'YUM Equity_Fuel Report': 'YUM-EQUITY-001',
  'Alchemy_Fuel Report': 'ALCHEMY-001', 
  'Gunret_Fuel-Report': 'GUNRET-001',
  'Spur_Fuel Report': 'SPUR-001'
};

async function testExcelMapping() {
  console.log('🧪 Testing Excel Data Mapping (Local Preview)...\n');
  
  try {
    const importFolder = './historical-imports';
    if (!fs.existsSync(importFolder)) {
      console.log('❌ Please create "historical-imports" folder and place your Excel files there');
      return;
    }
    
    const files = fs.readdirSync(importFolder).filter(file => file.endsWith('.xlsx') || file.endsWith('.xls'));
    
    if (files.length === 0) {
      console.log('❌ No Excel files found in historical-imports folder');
      return;
    }
    
    console.log(`📁 Found ${files.length} Excel files to test\n`);
    
    for (const file of files) {
      await testExcelFile(path.join(importFolder, file), file);
    }
    
    console.log('\n🎉 Excel mapping test completed!');
    console.log('\n💡 If the mapping looks correct, run: node import-historical-data.js');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

async function testExcelFile(filePath, fileName) {
  console.log(`📄 Testing: ${fileName}`);
  console.log('='.repeat(50));
  
  // Determine cost code from filename
  const costCode = getCostCodeFromFilename(fileName);
  if (!costCode) {
    console.log(`⚠️ No cost code mapping found for ${fileName}\n`);
    return;
  }
  
  console.log(`🏷️ Mapped Cost Code: ${costCode}`);
  
  try {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);
    
    console.log(`📊 Total rows found: ${data.length}`);
    console.log(`📋 Sheet name: ${sheetName}`);
    
    // Show first few rows structure
    console.log('\n📝 First 3 rows structure:');
    data.slice(0, 3).forEach((row, index) => {
      console.log(`\nRow ${index + 1}:`);
      Object.keys(row).forEach(key => {
        console.log(`  ${key}: ${row[key]}`);
      });
    });
    
    // Test parsing
    console.log('\n🔍 Testing data parsing:');
    let validSessions = 0;
    let invalidSessions = 0;
    
    const sampleSessions = [];
    
    for (const row of data.slice(0, 5)) { // Test first 5 rows
      const sessionData = parseRowData(row, costCode);
      if (sessionData) {
        validSessions++;
        sampleSessions.push(sessionData);
      } else {
        invalidSessions++;
      }
    }
    
    console.log(`✅ Valid sessions: ${validSessions}`);
    console.log(`❌ Invalid sessions: ${invalidSessions}`);
    
    // Show sample parsed data
    if (sampleSessions.length > 0) {
      console.log('\n📋 Sample parsed session data:');
      console.log(JSON.stringify(sampleSessions[0], null, 2));
    }
    
    console.log('\n' + '='.repeat(50) + '\n');
    
  } catch (error) {
    console.error(`❌ Error testing ${fileName}:`, error.message);
  }
}

function getCostCodeFromFilename(fileName) {
  for (const [key, value] of Object.entries(COST_CODE_MAPPING)) {
    if (fileName.includes(key.replace('_', ' ')) || fileName.includes(key)) {
      return value;
    }
  }
  return null;
}

function parseRowData(row, costCode) {
  // Extract site name and operating hours
  const site = row.Site || row.site || Object.values(row)[0];
  const operatingHours = row['Total Running Hours'] || row.operatingHours || Object.values(row)[1];
  
  if (!site || !operatingHours || site.includes('Total Running Hours')) {
    return null;
  }
  
  // Parse operating hours (e.g., "16 hours 51 minutes")
  const duration = parseOperatingHours(operatingHours);
  if (!duration) return null;
  
  // Create session data
  const sessionDate = new Date();
  sessionDate.setDate(sessionDate.getDate() - Math.floor(Math.random() * 30)); // Random date within last 30 days
  
  return {
    plate: site.trim(),
    cost_code: costCode,
    session_start: sessionDate.toISOString(),
    session_end: new Date(sessionDate.getTime() + (duration * 60 * 60 * 1000)).toISOString(),
    duration_minutes: Math.round(duration * 60),
    operating_hours: duration,
    fuel_usage: row['Total Usage'] || 0,
    fuel_fill: row['Total Fill'] || 0,
    opening_fuel: row['Opening Fuel'] || 0,
    closing_fuel: row['Closing Fuel'] || 0,
    opening_percentage: row['Opening Percentage'] || 0,
    closing_percentage: row['Closing Percentage'] || 0,
    liter_per_hour: row['Liter Usage Per Hour'] || 0,
    cost_for_usage: row['Cost For Usage'] || 0
  };
}

function parseOperatingHours(hoursText) {
  if (!hoursText || typeof hoursText !== 'string') return null;
  
  const hourMatch = hoursText.match(/(\d+)\s*hours?/i);
  const minuteMatch = hoursText.match(/(\d+)\s*minutes?/i);
  
  let totalHours = 0;
  if (hourMatch) totalHours += parseInt(hourMatch[1]);
  if (minuteMatch) totalHours += parseInt(minuteMatch[1]) / 60;
  
  return totalHours > 0 ? totalHours : null;
}

// Run the test
testExcelMapping();