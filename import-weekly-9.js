require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

// Date range for this import
const DATE_START = '2026-01-13';
const DATE_END = '2026-01-19';

function parseNumber(str) {
  if (!str) return 0;
  // Handle European format: 897,500 -> 897.5
  return parseFloat(str.toString().replace(/,/g, '.')) || 0;
}

function parsePercentage(str) {
  if (!str) return 0;
  return parseFloat(str.toString().replace(',', '.').replace('%', '')) || 0;
}

function parseOperatingHours(timeStr) {
  if (!timeStr) return 0;
  
  // Handle "X hours Y minutes" format
  const hoursMinutes = timeStr.match(/(\d+)\s*hour[s]?\s*(\d+)\s*minute[s]?/);
  if (hoursMinutes) {
    return parseFloat(hoursMinutes[1]) + parseFloat(hoursMinutes[2]) / 60;
  }
  
  // Handle "X hour" format (no minutes)
  const hoursOnly = timeStr.match(/(\d+)\s*hour[s]?/);
  if (hoursOnly) {
    return parseFloat(hoursOnly[1]);
  }
  
  // Handle "X minutes" format (no hours)
  const minutesOnly = timeStr.match(/(\d+)\s*minute[s]?/);
  if (minutesOnly) {
    return parseFloat(minutesOnly[1]) / 60;
  }
  
  return 0;
}

async function deleteExistingSessions() {
  console.log(`🗑️ Deleting existing sessions from ${DATE_START} to ${DATE_END}...`);
  
  const { data: existing, error: fetchError } = await supabase
    .from('energy_rite_operating_sessions')
    .select('id, branch, session_date')
    .gte('session_date', DATE_START)
    .lte('session_date', DATE_END);
  
  if (fetchError) {
    console.error('❌ Error fetching existing sessions:', fetchError.message);
    return false;
  }
  
  console.log(`📊 Found ${existing?.length || 0} existing sessions to delete`);
  
  if (existing && existing.length > 0) {
    const { error: deleteError } = await supabase
      .from('energy_rite_operating_sessions')
      .delete()
      .gte('session_date', DATE_START)
      .lte('session_date', DATE_END);
    
    if (deleteError) {
      console.error('❌ Error deleting sessions:', deleteError.message);
      return false;
    }
    
    console.log(`✅ Deleted ${existing.length} sessions`);
  }
  
  return true;
}

async function importWeekly9() {
  try {
    console.log('📂 Reading Weekly (9).xlsx...');
    const workbook = XLSX.readFile('./historical-imports/Weekly (9).xlsx');
    const sheetName = workbook.SheetNames[0];
    console.log(`📋 Sheet: ${sheetName}`);
    
    const worksheet = workbook.Sheets[sheetName];
    
    // Read raw data to handle the structure correctly
    const rawData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
    console.log(`📊 Found ${rawData.length} total rows`);
    
    // Find header row (row with "Site", "Date", etc.)
    let headerRowIndex = -1;
    for (let i = 0; i < rawData.length; i++) {
      if (rawData[i][0] === 'Site' && rawData[i][1] === 'Date') {
        headerRowIndex = i;
        break;
      }
    }
    
    if (headerRowIndex === -1) {
      console.error('❌ Could not find header row');
      return;
    }
    
    console.log(`📋 Header found at row ${headerRowIndex + 1}`);
    console.log(`📋 Headers: ${rawData[headerRowIndex].join(', ')}`);
    
    // Column indices (0-based)
    // 0: Site, 1: Date, 2: Operating Hours, 3: Opening Percentage, 4: Opening Fuel
    // 5: Closing Percentage, 6: Closing Fuel, 7: Total Usage, 8: Total Fill, 9: Liter Usage Per Hour, 10: Cost For Usage
    
    // Delete existing sessions first
    const deleted = await deleteExistingSessions();
    if (!deleted) {
      console.log('⚠️ Continuing with import despite delete issues...');
    }
    
    let imported = 0;
    let skipped = 0;
    let currentSite = null;
    let pendingSessions = new Map();
    
    // Process data rows (after header)
    for (let i = headerRowIndex + 1; i < rawData.length; i++) {
      const row = rawData[i];
      if (!row || row.length === 0) continue;
      
      const site = row[0];
      const dateOrType = row[1];
      const col3 = row[2];
      
      if (!site) continue;
      
      // Track current site
      if (site !== currentSite) {
        currentSite = site;
      }
      
      // Skip "Total Running Hours" rows
      if (dateOrType === 'Total Running Hours') {
        console.log(`📍 Site: ${site} - Total: ${col3}`);
        continue;
      }
      
      // Skip monthly total rows
      if (dateOrType === 'Total Hours' || site === 'January') {
        continue;
      }
      
      // Process date rows (format: 2026-01-XX)
      const dateMatch = dateOrType?.match(/^(\d{4}-\d{2}-\d{2})$/);
      if (dateMatch) {
        const sessionDate = dateMatch[1];
        
        // Only import dates within our range
        if (sessionDate < DATE_START || sessionDate > DATE_END) {
          continue;
        }
        
        const openingFuel = parseNumber(row[4]);
        const closingFuel = parseNumber(row[6]);
        const operatingHours = parseOperatingHours(col3);
        
        // Read Total Usage and Total Fill from spreadsheet columns (indices 7 and 8)
        // Usage comes as negative in spreadsheet, convert to positive
        const rawUsage = parseNumber(row[7]);
        const rawFill = parseNumber(row[8]);
        
        // Convert negative usage to positive (same value, just positive)
        let actualUsage = Math.abs(rawUsage);
        let actualFill = Math.abs(rawFill);
        let sessionStatus = actualFill > 0 ? 'FUEL_FILL_COMPLETED' : 'COMPLETED';
        
        const sessionData = {
          branch: site,
          company: 'KFC',
          session_date: sessionDate,
          operating_hours: operatingHours,
          opening_percentage: parsePercentage(row[3]),
          opening_fuel: openingFuel,
          closing_percentage: parsePercentage(row[5]),
          closing_fuel: closingFuel,
          total_usage: actualUsage,
          total_fill: actualFill,
          liter_usage_per_hour: operatingHours > 0 ? actualUsage / operatingHours : 0,
          cost_for_usage: actualUsage * 19.44, // Using rate from spreadsheet
          cost_per_liter: 19.44,
          session_status: sessionStatus,
          session_start_time: `${sessionDate}T06:00:00Z`,
          session_end_time: `${sessionDate}T18:00:00Z`,
          running_times: [], // Will store multiple running time periods
          notes: `Imported from Weekly (9).xlsx - ${sessionDate}`
        };
        
        // Only store if there's actual activity (usage > 0 or fill > 0 or operating hours > 0)
        if (actualUsage > 0 || actualFill > 0 || operatingHours > 0) {
          pendingSessions.set(`${site}_${sessionDate}`, sessionData);
          console.log(`📋 Found session: ${site} ${sessionDate} - Usage:${actualUsage.toFixed(1)}L Fill:${actualFill.toFixed(1)}L Hours:${operatingHours.toFixed(2)}h`);
        }
        continue;
      }
      
      // Process Running Time rows
      if (dateOrType === 'Running Time') {
        const fromTime = col3?.replace('From: ', '');
        const toTime = row[3]?.replace('To: ', '');
        
        if (fromTime && toTime) {
          // Find the most recent session for this site
          for (const [key, sessionData] of [...pendingSessions.entries()].reverse()) {
            if (key.startsWith(site + '_')) {
              // Add this running time to the session
              sessionData.running_times.push({ from: fromTime, to: toTime });
              
              // Update session times with first running time if not already set
              if (sessionData.running_times.length === 1) {
                sessionData.session_start_time = `${sessionData.session_date}T${fromTime}Z`;
              }
              // Always update end time with the latest running time
              sessionData.session_end_time = `${sessionData.session_date}T${toTime}Z`;
              
              console.log(`  ⏰ Running: ${site} ${sessionData.session_date} ${fromTime} → ${toTime}`);
              break;
            }
          }
        }
        continue;
      }
    }
    
    // Insert all sessions
    console.log(`\n📤 Inserting ${pendingSessions.size} sessions...`);
    
    for (const [key, sessionData] of pendingSessions.entries()) {
      // Format running times in notes
      if (sessionData.running_times.length > 0) {
        const timeParts = sessionData.running_times.map(rt => `${rt.from}-${rt.to}`).join(', ');
        sessionData.notes = `Imported from Weekly (9).xlsx - ${sessionData.session_date} [${timeParts}]`;
      }
      
      // Remove the temporary running_times array before insert
      delete sessionData.running_times;
      
      const { error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert(sessionData);
      
      if (error) {
        console.error(`❌ ${sessionData.branch} ${sessionData.session_date}:`, error.message);
        skipped++;
      } else {
        imported++;
        console.log(`✅ ${sessionData.branch} ${sessionData.session_date} - Usage:${sessionData.total_usage.toFixed(1)}L Fill:${sessionData.total_fill.toFixed(1)}L (${sessionData.operating_hours.toFixed(2)}h)`);
      }
    }
    
    console.log(`\n✅ Complete: ${imported} sessions imported, ${skipped} skipped`);
    
  } catch (error) {
    console.error('❌ Import failed:', error.message);
    console.error(error.stack);
  }
}

importWeekly9();
