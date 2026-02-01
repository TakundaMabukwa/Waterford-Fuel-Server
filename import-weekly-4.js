require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importWeekly4() {
  console.log('📊 IMPORTING Weekly (4).xlsx\n');
  
  try {
    const workbook = XLSX.readFile('./Weekly (4).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet);
    
    console.log(`📄 Found ${data.length} rows in ${sheetName}`);
    console.log('📋 Sample row:', JSON.stringify(data[0], null, 2));
    
    let imported = 0;
    
    for (const row of data) {
      const session = parseWeeklyRow(row);
      if (session) {
        await insertSession(session);
        imported++;
      }
    }
    
    console.log(`\n✅ Imported ${imported} sessions from Weekly (4).xlsx`);
    
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

function parseWeeklyRow(row) {
  const site = row.Site;
  if (!site || site.includes('Total') || site.includes('Site')) return null;
  
  // Parse date from Excel
  const sessionDate = parseExcelDate(row.Date);
  if (!sessionDate) return null;
  
  const hours = parseFloat(row['Operating Hours']) || 0;
  if (hours <= 0) return null;
  
  const startTime = new Date(sessionDate);
  startTime.setHours(6, 0, 0, 0);
  const endTime = new Date(startTime.getTime() + (hours * 60 * 60 * 1000));
  
  return {
    branch: site.trim(),
    company: 'KFC', 
    cost_code: getCostCode(site),
    session_date: sessionDate.toISOString().split('T')[0],
    session_start_time: startTime.toISOString(),
    session_end_time: endTime.toISOString(),
    operating_hours: hours,
    opening_percentage: parseFloat(row['Opening Percentage']) || 0,
    opening_fuel: parseFloat(row['Opening Fuel']) || 0,
    closing_percentage: parseFloat(row['Closing Percentage']) || 0,
    closing_fuel: parseFloat(row['Closing Fuel']) || 0,
    total_usage: parseFloat(row['Total Usage']) || 0,
    total_fill: parseFloat(row['Total Fill']) || 0,
    liter_usage_per_hour: parseFloat(row['Liter Usage Per Hour']) || 0,
    cost_for_usage: parseFloat(row['Cost For Usage']) || 0,
    session_status: 'COMPLETED',
    notes: 'Imported from Weekly (4).xlsx'
  };
}

function parseExcelDate(dateValue) {
  if (!dateValue) return null;
  
  // Handle Excel date formats
  if (typeof dateValue === 'number') {
    // Excel serial date
    return new Date((dateValue - 25569) * 86400 * 1000);
  }
  
  if (typeof dateValue === 'string') {
    const parsed = new Date(dateValue);
    if (!isNaN(parsed.getTime())) return parsed;
  }
  
  return null;
}

function getCostCode(site) {
  // Map sites to cost codes based on common patterns
  const siteUpper = site.toUpperCase();
  
  if (siteUpper.includes('KFC')) return 'KFC-001';
  if (siteUpper.includes('YUM')) return 'YUM-001';
  if (siteUpper.includes('ALCHEMY')) return 'ALCHEMY-001';
  if (siteUpper.includes('SPUR')) return 'SPUR-001';
  
  return 'GENERAL-001'; // Default
}

async function insertSession(session) {
  try {
    const { error } = await supabase
      .from('energy_rite_operating_sessions')
      .insert(session);
    
    if (error) throw error;
    
  } catch (error) {
    console.error(`❌ Error inserting ${session.branch}:`, error.message);
  }
}

importWeekly4();