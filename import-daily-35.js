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

async function importDaily35() {
  try {
    console.log('📂 Reading Daily (35).xlsx...');
    const workbook = XLSX.readFile('./historical-imports/Daily (35).xlsx');
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(worksheet, { raw: false });

    console.log(`📊 Found ${data.length} rows\n`);

    let imported = 0;
    let skipped = 0;
    let currentSite = null;
    let currentDate = null;
    let currentSession = null;
    let sessionStartTime = null;
    let sessionEndTime = null;

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const col1 = row['FUEL REPORT SUMMARY'];
      const col2 = row['__EMPTY'];

      // Track current site
      if (col1 && !col2?.includes('Total') && !col1.includes('January')) {
        currentSite = col1;
      }

      // Check if this is a data row with date
      if (col1 && col2 && col2.match(/\d{4}-\d{2}-\d{2}/)) {
        currentDate = col2;
        currentSession = {
          branch: col1,
          company: 'KFC',
          session_date: col2,
          operating_hours: parseTime(row['__EMPTY_1']),
          opening_percentage: parsePercentage(row['__EMPTY_2']),
          opening_fuel: parseNumber(row['__EMPTY_3']),
          closing_percentage: parsePercentage(row['__EMPTY_4']),
          closing_fuel: parseNumber(row['__EMPTY_5']),
          total_usage: parseNumber(row['__EMPTY_6']),
          total_fill: parseNumber(row['__EMPTY_7']),
          liter_usage_per_hour: parseNumber(row['__EMPTY_8']),
          cost_for_usage: parseNumber(row['__EMPTY_9']?.split('=')[1]),
          cost_per_liter: 21.00,
          session_status: 'COMPLETED'
        };
      }

      // Check for "Running Time" rows with From/To times
      if (col1 === currentSite && col2 === 'Running Time') {
        const fromTime = row['__EMPTY_1']?.replace('From: ', '');
        const toTime = row['__EMPTY_2']?.replace('To: ', '');

        if (fromTime && toTime && currentSession && currentDate) {
          // Create individual session with exact times
          const sessionData = {
            ...currentSession,
            session_start_time: `${currentDate}T${fromTime}Z`,
            session_end_time: `${currentDate}T${toTime}Z`,
            notes: `Imported from Daily (35).xlsx - Individual session`
          };

          // Calculate operating hours from actual times
          const start = new Date(`${currentDate}T${fromTime}Z`);
          const end = new Date(`${currentDate}T${toTime}Z`);
          sessionData.operating_hours = (end - start) / (1000 * 60 * 60);

          const { error } = await supabase
            .from('energy_rite_operating_sessions')
            .insert(sessionData);

          if (error) {
            console.error(`❌ ${sessionData.branch} ${fromTime}-${toTime}:`, error.message);
            skipped++;
          } else {
            imported++;
            console.log(`✅ ${sessionData.branch} ${fromTime} → ${toTime} (${sessionData.operating_hours.toFixed(2)}h)`);
          }
        }
      }
    }

    console.log(`\n✅ Complete: ${imported} sessions imported, ${skipped} skipped`);
  } catch (error) {
    console.error('❌ Import failed:', error.message);
    console.error(error.stack);
  }
}

importDaily35();
