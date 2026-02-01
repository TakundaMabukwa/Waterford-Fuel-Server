#!/usr/bin/env node
require('dotenv').config();
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

// Debug function to see what's being parsed
function debugParseOperatingHours(hoursText) {
  console.log(`Input: "${hoursText}" (type: ${typeof hoursText})`);
  
  if (!hoursText) return null;
  
  // Skip summary rows and running time rows
  if (typeof hoursText === 'string' && 
      (hoursText.includes('Total Running Hours') || 
       hoursText.includes('Total Hours') ||
       hoursText.includes('Running Time') ||
       hoursText.includes('From:'))) {
    console.log('  -> Skipped (summary/running time row)');
    return null;
  }
  
  // Handle text formats
  if (typeof hoursText !== 'string') return null;
  
  let totalHours = 0;
  
  // Match hours and minutes patterns - be more specific
  const hourMatch = hoursText.match(/(\d+)\s*hours?/i);
  const minuteMatch = hoursText.match(/(\d+)\s*minutes?/i);
  
  // If we have both hours and minutes
  if (hourMatch && minuteMatch) {
    totalHours = parseInt(hourMatch[1]) + parseInt(minuteMatch[1]) / 60;
    console.log(`  -> Found ${hourMatch[1]} hours + ${minuteMatch[1]} minutes`);
  }
  // If we only have hours
  else if (hourMatch && !minuteMatch) {
    totalHours = parseInt(hourMatch[1]);
    console.log(`  -> Found ${hourMatch[1]} hours only`);
  }
  // If we only have minutes
  else if (minuteMatch && !hourMatch) {
    totalHours = parseInt(minuteMatch[1]) / 60;
    console.log(`  -> Found ${minuteMatch[1]} minutes only (${totalHours.toFixed(2)} hours)`);
  }
  // Handle formats like "2:30" (hours:minutes)
  else {
    const timeMatch = hoursText.match(/(\d+):(\d+)/);
    if (timeMatch) {
      totalHours = parseInt(timeMatch[1]) + parseInt(timeMatch[2]) / 60;
      console.log(`  -> Time format: ${timeMatch[1]}:${timeMatch[2]} = ${totalHours.toFixed(2)} hours`);
    }
    // Handle pure numeric values
    else {
      const numeric = parseFloat(hoursText);
      if (!isNaN(numeric) && numeric > 0) {
        totalHours = numeric;
        console.log(`  -> Numeric: ${numeric} hours`);
      }
    }
  }
  
  const result = totalHours > 0 ? Math.round(totalHours * 100) / 100 : null;
  console.log(`  -> Final result: ${result} hours\n`);
  return result;
}

async function debugImport() {
  console.log('🔍 Debug Import - Looking for BRACKENHURST...\n');
  
  const importFolder = './historical-imports';
  const files = fs.readdirSync(importFolder).filter(file => file.endsWith('.xlsx'));
  
  // Check the Weekly file which has the data
  const fileName = files.find(f => f.includes('Weekly')) || files[0];
  console.log(`📄 Debugging: ${fileName}\n`);
  
  const workbook = XLSX.readFile(path.join(importFolder, fileName));
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(worksheet);
  
  console.log('Looking for BRACKENHURST data:\n');
  
  for (const row of data) {
    const site = row['FUEL REPORT SUMMARY'];
    const date = row['__EMPTY'];
    const operatingHours = row['__EMPTY_1'];
    
    if (site && site.includes('BRACKENHUR') && operatingHours) {
      console.log(`BRACKENHURST Row:`);
      console.log(`  Site: ${site}`);
      console.log(`  Date: ${date}`);
      console.log(`  Operating Hours Raw: "${operatingHours}"`);
      console.log(`  Fuel Usage: ${row['__EMPTY_6']}`);
      
      const parsed = debugParseOperatingHours(operatingHours);
      console.log(`  Parsed Hours: ${parsed}`);
      console.log('---');
    }
  }
}

debugImport();