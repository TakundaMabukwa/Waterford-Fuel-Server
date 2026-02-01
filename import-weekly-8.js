require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

function parseNumber(str) {
  if (!str) return 0;
  // Handle European format: 897,500 -> 897.5
  return parseFloat(str.toString().replace(/,/g, '.')) || 0;
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

async function importWeekly8() {
  try {
    console.log('📂 Reading Weekly (8).xlsx...');
    const workbook = XLSX.readFile('./historical-imports/Weekly (8).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet, { raw: false });

    console.log(`📊 Found ${data.length} rows`);
    console.log('📋 Sample rows:');
    for (let i = 0; i < Math.min(5, data.length); i++) {
      console.log(`Row ${i + 1}:`, JSON.stringify(data[i], null, 2));
    }

    let imported = 0;
    let skipped = 0;
    let pendingSessions = new Map();

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const col1 = row['FUEL REPORT SUMMARY'] || row[Object.keys(row)[0]];
      const col2 = row['__EMPTY'] || row[Object.keys(row)[1]];

      // Process date rows (2026-01-07 to 2026-01-12)
      if (col1 && col2 && col2.match(/2026-01-(07|08|09|10|11|12)/)) {
        const openingFuel = parseNumber(row['__EMPTY_3'] || row[Object.keys(row)[4]]);
        const closingFuel = parseNumber(row['__EMPTY_5'] || row[Object.keys(row)[6]]);
        const fuelDiff = closingFuel - openingFuel;
        
        // Determine if this is usage or fill based on fuel difference
        let actualUsage = 0;
        let actualFill = 0;
        let sessionStatus = 'COMPLETED';
        
        if (fuelDiff > 5) {
          // Fuel increased significantly = FILL
          actualFill = fuelDiff;
          sessionStatus = 'FUEL_FILL_COMPLETED';
        } else if (fuelDiff < -5) {
          // Fuel decreased significantly = USAGE
          actualUsage = Math.abs(fuelDiff);
        }
        
        const sessionData = {
          branch: col1,
          company: 'KFC',
          session_date: col2,
          operating_hours: parseTime(row['__EMPTY_1'] || row[Object.keys(row)[2]]),
          opening_percentage: parsePercentage(row['__EMPTY_2'] || row[Object.keys(row)[3]]),
          opening_fuel: openingFuel,
          closing_percentage: parsePercentage(row['__EMPTY_4'] || row[Object.keys(row)[5]]),
          closing_fuel: closingFuel,
          total_usage: actualUsage,
          total_fill: actualFill,
          liter_usage_per_hour: parseTime(row['__EMPTY_1'] || row[Object.keys(row)[2]]) > 0 ? actualUsage / parseTime(row['__EMPTY_1'] || row[Object.keys(row)[2]]) : 0,
          cost_for_usage: actualUsage * 21.00,
          cost_per_liter: 21.00,
          session_status: sessionStatus,
          session_start_time: `${col2}T06:00:00Z`,
          session_end_time: `${col2}T18:00:00Z`,
          notes: `Imported from Weekly (8).xlsx - ${col2} ${fuelDiff > 5 ? 'FILL' : fuelDiff < -5 ? 'USAGE' : 'NO CHANGE'} (${fuelDiff.toFixed(1)}L)`
        };

        // Only store if there's actual activity (usage > 0 or fill > 0 or operating hours > 0)
        if (actualUsage > 0 || actualFill > 0 || sessionData.operating_hours > 0) {
          pendingSessions.set(`${col1}_${col2}`, sessionData);
          console.log(`📋 Found session: ${col1} ${col2} - Usage:${actualUsage}L Fill:${actualFill}L Hours:${sessionData.operating_hours.toFixed(2)}h`);
        } else {
          console.log(`⏭️ Skipped inactive: ${col1} ${col2} - no activity`);
        }
      }

      // Process Running Time rows
      if (col1 && col2 === 'Running Time') {
        const fromTime = (row['__EMPTY_1'] || row[Object.keys(row)[2]])?.replace('From: ', '');
        const toTime = (row['__EMPTY_2'] || row[Object.keys(row)[3]])?.replace('To: ', '');

        if (fromTime && toTime) {
          for (const [key, sessionData] of pendingSessions.entries()) {
            if (key.startsWith(col1 + '_')) {
              sessionData.session_start_time = `${sessionData.session_date}T${fromTime}Z`;
              sessionData.session_end_time = `${sessionData.session_date}T${toTime}Z`;
              
              const start = new Date(sessionData.session_start_time);
              const end = new Date(sessionData.session_end_time);
              sessionData.operating_hours = (end - start) / (1000 * 60 * 60);
              sessionData.liter_usage_per_hour = sessionData.operating_hours > 0 ? sessionData.total_usage / sessionData.operating_hours : 0;
              sessionData.notes = `Imported from Weekly (8).xlsx - ${sessionData.session_date} ${fromTime}-${toTime}`;
              
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

importWeekly8();