require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

function parseNumber(str) {
  if (!str) return 0;
  return parseFloat(str.toString().replace(/,/g, '.').replace(/[^\d.-]/g, '')) || 0;
}

function parsePercentage(str) {
  if (!str) return 0;
  return parseFloat(str.toString().replace(',', '.').replace('%', '')) || 0;
}

function parseTime(timeStr) {
  if (!timeStr) return null;
  const match = timeStr.match(/(\d+)\s*hour[s]?\s*(\d+)\s*minute[s]?/);
  if (match) {
    return parseFloat(match[1]) + parseFloat(match[2]) / 60;
  }
  return 0;
}

async function importWeeklyData() {
  try {
    console.log('📂 Reading Daily (35).xlsx for 11th and 12th data...');
    const workbook = XLSX.readFile('./historical-imports/Daily (35).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet, { raw: false });

    let imported = 0;
    let skipped = 0;
    let pendingSessions = new Map(); // Store sessions waiting for Running Time

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const col1 = row['FUEL REPORT SUMMARY'];
      const col2 = row['__EMPTY'];

      // Process date rows for 2026-01-11 and 2026-01-12
      if (col1 && col2 && (col2 === '2026-01-11' || col2 === '2026-01-12')) {
        const sessionData = {
          branch: col1,
          company: 'KFC',
          session_date: col2,
          operating_hours: parseTime(row['__EMPTY_1']),
          opening_percentage: parsePercentage(row['__EMPTY_2']),
          opening_fuel: parseNumber(row['__EMPTY_3']),
          closing_percentage: parsePercentage(row['__EMPTY_4']),
          closing_fuel: parseNumber(row['__EMPTY_5']),
          total_usage: Math.abs(parseNumber(row['__EMPTY_6'])),
          total_fill: parseNumber(row['__EMPTY_7']),
          liter_usage_per_hour: parseNumber(row['__EMPTY_8']),
          cost_for_usage: Math.abs(parseNumber(row['__EMPTY_6'])) * 21.00,
          cost_per_liter: 21.00,
          session_status: 'COMPLETED',
          session_start_time: `${col2}T06:00:00Z`,
          session_end_time: `${col2}T18:00:00Z`,
          notes: `Imported from Daily (35).xlsx - ${col2} data`
        };

        // Store session, will be updated if Running Time found
        pendingSessions.set(`${col1}_${col2}`, sessionData);
        console.log(`📋 Found session: ${col1} ${col2} - ${sessionData.total_usage}L`);
      }

      // Process Running Time rows
      if (col1 && col2 === 'Running Time') {
        const fromTime = row['__EMPTY_1']?.replace('From: ', '');
        const toTime = row['__EMPTY_2']?.replace('To: ', '');

        if (fromTime && toTime) {
          // Find matching session for this site
          for (const [key, sessionData] of pendingSessions.entries()) {
            if (key.startsWith(col1 + '_')) {
              // Update with exact times
              sessionData.session_start_time = `${sessionData.session_date}T${fromTime}Z`;
              sessionData.session_end_time = `${sessionData.session_date}T${toTime}Z`;
              
              // Recalculate operating hours from actual times
              const start = new Date(sessionData.session_start_time);
              const end = new Date(sessionData.session_end_time);
              sessionData.operating_hours = (end - start) / (1000 * 60 * 60);
              sessionData.liter_usage_per_hour = sessionData.operating_hours > 0 ? sessionData.total_usage / sessionData.operating_hours : 0;
              sessionData.notes = `Imported from Daily (35).xlsx - ${sessionData.session_date} ${fromTime}-${toTime}`;
              
              console.log(`⏰ Updated times: ${col1} ${sessionData.session_date} ${fromTime} → ${toTime}`);
              break;
            }
          }
        }
      }
    }

    // Insert all sessions
    for (const [key, sessionData] of pendingSessions.entries()) {
      const { error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert(sessionData);

      if (error) {
        console.error(`❌ ${sessionData.branch} ${sessionData.session_date}:`, error.message);
        skipped++;
      } else {
        imported++;
        console.log(`✅ ${sessionData.branch} ${sessionData.session_date} - ${sessionData.total_usage}L (${sessionData.operating_hours.toFixed(2)}h)`);
      }
    }

    console.log(`\n✅ Complete: ${imported} sessions imported, ${skipped} skipped`);
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

importWeeklyData();