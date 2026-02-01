#!/usr/bin/env node
require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');
const fs = require('fs');
const path = require('path');

// Cost code and company mapping for your files
const COMPANY_MAPPING = {
  'YUM Equity_Fuel Report': { company: 'YUM-EQUITY', cost_code: 'KFC-0001-0001-0003' },
  'Alchemy_Fuel Report': { company: 'Alchemy', cost_code: 'KFC-0001-0001-0001' },
  'Gunret_Fuel-Report': { company: 'Gunret', cost_code: 'KFC-0001-0001-0002' },
  'Spur_Fuel Report': { company: 'Spurs', cost_code: 'KFC-0001-0002' }
};

async function importHistoricalFuelData() {
  console.log('📊 Starting Historical Fuel Data Import...\n');
  
  try {
    const importFolder = './historical-imports';
    const files = fs.readdirSync(importFolder).filter(file => file.endsWith('.xlsx'));
    
    console.log(`📁 Found ${files.length} Excel files to import\n`);
    
    let totalImported = 0;
    
    for (const file of files) {
      const imported = await processExcelFile(path.join(importFolder, file), file);
      totalImported += imported;
    }
    
    console.log(`\n🎉 Import completed! Total sessions imported: ${totalImported}`);
    
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

async function processExcelFile(filePath, fileName) {
  console.log(`📄 Processing: ${fileName}`);
  
  const companyData = getCompanyDataFromFilename(fileName);
  if (!companyData) {
    console.log(`⚠️ Skipping ${fileName} - no company mapping found`);
    return 0;
  }
  
  console.log(`🏷️ Company: ${companyData.company}, Cost Code: ${companyData.cost_code}`);
  
  try {
    const workbook = XLSX.readFile(filePath);
    
    // Find the main fuel report sheet
    const mainSheet = workbook.SheetNames.find(name => 
      name.includes('Fuel') || name.includes('YUM') || name.includes('Alchemy') || name.includes('Gunret') || name.includes('Spur')
    );
    
    if (!mainSheet) {
      console.log(`⚠️ No fuel report sheet found in ${fileName}`);
      return 0;
    }
    
    const worksheet = workbook.Sheets[mainSheet];
    const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
    
    console.log(`📊 Processing ${data.length} rows from sheet: ${mainSheet}`);
    
    let importedCount = 0;
    let currentSite = null;
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!row || row.length === 0) continue;
      
      // Check if this is a site summary row (contains "Total Running Hours")
      if (row[1] === 'Total Running Hours') {
        currentSite = row[0];
        const totalHours = row[2];
        
        console.log(`🏢 Found site: ${currentSite} - Total: ${totalHours}`);
        
        // Create a summary session for this site
        const sessionData = createSummarySession(currentSite, totalHours, companyData);
        if (sessionData) {
          await insertHistoricalSession(sessionData);
          importedCount++;
        }
      }
      
      // Check if this is a daily detail row (has a date in column 1)
      else if (currentSite && row[0] === currentSite && isValidDate(row[1])) {
        const sessionData = createDailySession(row, companyData);
        if (sessionData) {
          await insertHistoricalSession(sessionData);
          importedCount++;
        }
      }
    }
    
    console.log(`✅ Imported ${importedCount} sessions from ${fileName}\n`);
    return importedCount;
    
  } catch (error) {
    console.error(`❌ Error processing ${fileName}:`, error.message);
    return 0;
  }
}

function getCompanyDataFromFilename(fileName) {
  for (const [key, value] of Object.entries(COMPANY_MAPPING)) {
    if (fileName.includes(key.replace('_', ' ')) || fileName.includes(key)) {
      return value;
    }
  }
  return null;
}

function createSummarySession(site, totalHours, companyData) {
  const duration = parseOperatingHours(totalHours);
  if (!duration) return null;
  
  // Create a summary session for the past month
  const sessionDate = new Date();
  sessionDate.setDate(sessionDate.getDate() - 15); // 15 days ago as average
  
  return {
    branch: site.trim(),
    company: companyData.company,
    cost_code: companyData.cost_code,
    session_date: sessionDate.toISOString().split('T')[0],
    session_start_time: sessionDate.toISOString(),
    session_end_time: new Date(sessionDate.getTime() + (duration * 60 * 60 * 1000)).toISOString(),
    operating_hours: duration,
    total_usage: 0,
    total_fill: 0,
    session_status: 'COMPLETED',
    notes: 'Historical summary data'
  };
}

function createDailySession(row, companyData) {
  const site = row[0];
  const date = row[1];
  const operatingHours = row[2];
  const openingPercentage = parseFloat(row[3]) || 0;
  const openingFuel = parseFloat(row[4]) || 0;
  const closingPercentage = parseFloat(row[5]) || 0;
  const closingFuel = parseFloat(row[6]) || 0;
  const totalUsage = parseFloat(row[7]) || 0;
  const totalFill = parseFloat(row[8]) || 0;
  const literPerHour = parseFloat(row[9]) || 0;
  const costForUsage = row[10] ? parseFloat(row[10].toString().replace(/[^\d.-]/g, '')) || 0 : 0;
  
  const duration = parseOperatingHours(operatingHours);
  if (!duration) return null;
  
  // Parse the date
  const sessionDate = new Date(date);
  if (isNaN(sessionDate.getTime())) return null;
  
  return {
    branch: site.trim(),
    company: companyData.company,
    cost_code: companyData.cost_code,
    session_date: sessionDate.toISOString().split('T')[0],
    session_start_time: sessionDate.toISOString(),
    session_end_time: new Date(sessionDate.getTime() + (duration * 60 * 60 * 1000)).toISOString(),
    operating_hours: duration,
    opening_percentage: openingPercentage,
    opening_fuel: openingFuel,
    closing_percentage: closingPercentage,
    closing_fuel: closingFuel,
    total_usage: Math.abs(totalUsage),
    total_fill: totalFill,
    liter_usage_per_hour: literPerHour,
    cost_for_usage: costForUsage,
    session_status: 'COMPLETED',
    notes: 'Historical daily data'
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

function isValidDate(dateString) {
  if (!dateString || typeof dateString !== 'string') return false;
  return dateString.match(/^\d{4}-\d{2}-\d{2}$/);
}

async function insertHistoricalSession(sessionData) {
  try {
    const { error } = await supabase
      .from('energy_rite_operating_sessions')
      .insert([sessionData]);
    
    if (error) throw error;
    
  } catch (error) {
    console.error('❌ Error inserting session:', error.message);
  }
}

// Run the import
importHistoricalFuelData();