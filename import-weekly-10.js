require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importWeekly10() {
  console.log('📊 IMPORTING Weekly (10).xlsx (2026-01-19 to 2026-01-21)\n');
  
  try {
    // Delete existing sessions for these dates
    console.log('🗑️  Deleting existing sessions from 2026-01-19 to 2026-01-21...');
    const { error: deleteError } = await supabase
      .from('energy_rite_operating_sessions')
      .delete()
      .gte('session_date', '2026-01-19')
      .lte('session_date', '2026-01-21');
    
    if (deleteError) {
      console.error('❌ Delete failed:', deleteError.message);
      return;
    }
    console.log('✅ Deleted existing sessions\n');
    
    const workbook = XLSX.readFile('./historical-imports/Weekly (10).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet, { raw: false, defval: '' });
    
    console.log(`📄 Found ${data.length} rows\n`);
    
    let currentSite = '';
    let runningTimes = [];
    let imported = 0;
    let skipped = 0;
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const col1 = row['FUEL REPORT SUMMARY'] || '';
      const col2 = row['__EMPTY'] || '';
      
      // Track current site
      if (col1 && col1.includes('Total Running Hours')) {
        currentSite = col1.replace('Total Running Hours', '').trim();
        runningTimes = [];
        continue;
      }
      
      // Capture running times
      if (col1 && col1.includes('Running Time')) {
        const fromMatch = col1.match(/From:\s*(\d{2}:\d{2}:\d{2})/);
        const toMatch = col1.match(/To:\s*(\d{2}:\d{2}:\d{2})/);
        if (fromMatch && toMatch) {
          runningTimes.push({ start: fromMatch[1], end: toMatch[1] });
        }
        continue;
      }
      
      // Parse session data
      if (col1 && col2 && !col1.includes('Site') && !col2.includes('Date')) {
        const session = parseSessionRow(row, col1, runningTimes);
        if (session) {
          const { error } = await supabase
            .from('energy_rite_operating_sessions')
            .insert(session);
          
          if (error) {
            console.error(`❌ ${session.branch} ${session.session_date}:`, error.message);
            skipped++;
          } else {
            console.log(`✅ ${session.branch} ${session.session_date} - ${session.total_usage}L (${session.operating_hours.toFixed(2)}h)`);
            imported++;
            
            // Create separate fuel fill session if total_fill > 0
            if (session.total_fill > 0) {
              const fillSession = {
                branch: session.branch,
                company: session.company,
                cost_code: session.cost_code,
                session_date: session.session_date,
                session_start_time: session.session_start_time,
                session_end_time: session.session_end_time,
                operating_hours: 0.01,
                opening_percentage: session.opening_percentage,
                opening_fuel: session.opening_fuel,
                closing_percentage: session.closing_percentage,
                closing_fuel: session.opening_fuel + session.total_fill,
                total_usage: 0,
                total_fill: session.total_fill,
                liter_usage_per_hour: 0,
                cost_per_liter: session.cost_per_liter,
                cost_for_usage: 0,
                session_status: 'FUEL_FILL_COMPLETED',
                notes: `Fuel fill: ${session.total_fill}L. Imported from Weekly (10).xlsx`
              };
              
              const { error: fillError } = await supabase
                .from('energy_rite_operating_sessions')
                .insert(fillSession);
              
              if (!fillError) {
                console.log(`⛽ ${fillSession.branch} ${fillSession.session_date} - FILL: ${fillSession.total_fill}L`);
                imported++;
              }
            }
          }
          runningTimes = [];
        }
      }
    }
    
    console.log(`\n✅ Complete: ${imported} sessions imported, ${skipped} skipped`);
    
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

function parseSessionRow(row, site, runningTimes) {
  const dateStr = row['__EMPTY'];
  const operatingHours = row['__EMPTY_1'];
  
  if (!dateStr || !operatingHours) return null;
  
  const sessionDate = parseDate(dateStr);
  if (!sessionDate) return null;
  
  const hours = parseHours(operatingHours);
  if (!hours || hours <= 0) return null;
  
  // Calculate start/end times
  let startTime = new Date(sessionDate);
  let endTime = new Date(sessionDate);
  
  if (runningTimes.length > 0) {
    const [startH, startM, startS] = runningTimes[0].start.split(':').map(Number);
    const [endH, endM, endS] = runningTimes[0].end.split(':').map(Number);
    startTime.setHours(startH, startM, startS, 0);
    endTime.setHours(endH, endM, endS, 0);
  } else {
    startTime.setHours(6, 0, 0, 0);
    endTime = new Date(startTime.getTime() + (hours * 60 * 60 * 1000));
  }
  
  const openingPct = parseNumber(row['__EMPTY_2']);
  const openingFuel = parseNumber(row['__EMPTY_3']);
  const closingPct = parseNumber(row['__EMPTY_4']);
  const closingFuel = parseNumber(row['__EMPTY_5']);
  const totalUsage = Math.abs(parseNumber(row['__EMPTY_6']));
  const totalFill = parseNumber(row['__EMPTY_7']);
  const literPerHour = parseNumber(row['__EMPTY_8']);
  const costForUsage = parseNumber(row['__EMPTY_9']);
  
  return {
    branch: site.trim(),
    company: 'KFC',
    cost_code: getCostCode(site.trim()),
    session_date: sessionDate.toISOString().split('T')[0],
    session_start_time: startTime.toISOString(),
    session_end_time: endTime.toISOString(),
    operating_hours: hours,
    opening_percentage: openingPct,
    opening_fuel: openingFuel,
    closing_percentage: closingPct,
    closing_fuel: closingFuel,
    total_usage: totalUsage,
    total_fill: totalFill,
    liter_usage_per_hour: literPerHour || (hours > 0 ? totalUsage / hours : 0),
    cost_per_liter: 21.00,
    cost_for_usage: costForUsage || (totalUsage * 21.00),
    session_status: 'COMPLETED',
    notes: `Imported from Weekly (10).xlsx`
  };
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  
  // Filter only dates from 2026-01-19 to 2026-01-21
  const parsed = new Date(dateStr);
  if (!isNaN(parsed.getTime())) {
    const dateOnly = parsed.toISOString().split('T')[0];
    if (dateOnly >= '2026-01-19' && dateOnly <= '2026-01-21') {
      return parsed;
    }
  }
  
  return null;
}

function parseHours(hoursStr) {
  if (!hoursStr) return 0;
  
  // "35 minutes" -> 0.583
  const minMatch = hoursStr.match(/(\d+)\s*minute/i);
  if (minMatch) return parseFloat(minMatch[1]) / 60;
  
  // "2 hours 30 minutes" -> 2.5
  const hourMatch = hoursStr.match(/(\d+)\s*hour/i);
  const minMatch2 = hoursStr.match(/(\d+)\s*minute/i);
  if (hourMatch) {
    const h = parseFloat(hourMatch[1]);
    const m = minMatch2 ? parseFloat(minMatch2[1]) / 60 : 0;
    return h + m;
  }
  
  return 0;
}

function parseNumber(str) {
  if (!str) return 0;
  const cleaned = str.toString().replace(/,/g, '.').replace(/%/g, '').replace(/[^\d.-]/g, '');
  return parseFloat(cleaned) || 0;
}

function getCostCode(site) {
  const codes = {
    'ALEX': 'KFC-0001-0001-0001',
    'BALLYCLARE': 'KFC-0001-0001-0002-0004',
    'BERGBRON': 'KFC-0001-0001-0003',
    'BEYERSPARK': 'KFC-0001-0001-0003',
    'RANDBURG': 'KFC-0001-0001-0003',
    'MOBILE 3': 'KFC-0001-0001-0003',
    'FARRAMERE': 'KFC-0001-0001-0003'
  };
  return codes[site.toUpperCase()] || 'KFC-0001-0001-0003';
}

importWeekly10();
