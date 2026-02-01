require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importMonthlyFinal() {
  try {
    console.log('📊 Importing Monthly (7).xlsx with correct structure...');
    
    const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet);

    console.log(`📊 Found ${data.length} rows`);

    let imported = 0;
    let currentSite = null;
    let pendingRunningTimes = [];

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const site = row['FUEL REPORT SUMMARY'];
      const dateOrInfo = row['__EMPTY'];
      const timeOrHours = row['__EMPTY_1'];
      
      if (!site) continue;

      // Skip total/summary rows
      if (dateOrInfo?.includes('Total Running Hours') || dateOrInfo?.includes('Total Hours')) {
        currentSite = site;
        continue;
      }

      // Capture running times
      if (dateOrInfo?.includes('Running Time')) {
        const fromTime = timeOrHours?.replace('From: ', '');
        const toTime = row['__EMPTY_2']?.replace('To: ', '');
        if (fromTime && toTime) {
          pendingRunningTimes.push({ from: fromTime, to: toTime });
        }
        continue;
      }

      // Process daily entries with dates (YYYY-MM-DD format)
      if (site && dateOrInfo?.match(/^\d{4}-\d{2}-\d{2}$/)) {
        currentSite = site;
        
        const operatingHours = parseTimeToHours(timeOrHours);
        let startTime = `${dateOrInfo}T06:00:00Z`;
        let endTime = `${dateOrInfo}T18:00:00Z`;
        
        // Use actual running times if available and operating hours > 0
        if (pendingRunningTimes.length > 0 && operatingHours > 0) {
          const firstTime = pendingRunningTimes[0];
          startTime = `${dateOrInfo}T${firstTime.from}Z`;
          
          // Calculate total duration from all running time periods
          let totalMinutes = 0;
          for (const period of pendingRunningTimes) {
            const start = parseTime(period.from);
            const end = parseTime(period.to);
            if (start && end) {
              totalMinutes += (end - start) / (1000 * 60);
            }
          }
          
          // Use calculated end time
          const startDate = new Date(`${dateOrInfo}T${firstTime.from}`);
          const endDate = new Date(startDate.getTime() + (totalMinutes * 60 * 1000));
          endTime = endDate.toISOString();
        }
        
        const sessionData = {
          branch: site.trim(),
          company: 'KFC',
          cost_code: getCostCode(site.trim()),
          session_date: dateOrInfo,
          session_start_time: startTime,
          session_end_time: endTime,
          operating_hours: operatingHours,
          opening_percentage: parseFloat(row['__EMPTY_2']?.replace('%', '').replace(',', '.')) || 0,
          opening_fuel: parseFloat(row['__EMPTY_3']?.replace(',', '.')) || 0,
          closing_percentage: parseFloat(row['__EMPTY_4']?.replace('%', '').replace(',', '.')) || 0,
          closing_fuel: parseFloat(row['__EMPTY_5']?.replace(',', '.')) || 0,
          total_usage: Math.abs(parseFloat(row['__EMPTY_6']?.replace(',', '.')) || 0),
          total_fill: parseFloat(row['__EMPTY_7']?.replace(',', '.')) || 0,
          liter_usage_per_hour: operatingHours > 0 ? Math.abs(parseFloat(row['__EMPTY_6']?.replace(',', '.')) || 0) / operatingHours : 0,
          cost_for_usage: parseCost(row['__EMPTY_9']),
          session_status: operatingHours > 0 ? 'COMPLETED' : 'NO_OPERATION'
        };

        const { error } = await supabase
          .from('energy_rite_operating_sessions')
          .insert(sessionData);

        if (!error) imported++;
        
        // Clear running times after processing the date entry
        pendingRunningTimes = [];
      }
    }

    console.log(`✅ Imported ${imported} sessions`);
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

function parseTimeToHours(timeText) {
  if (!timeText || typeof timeText !== 'string') return 0;
  
  const hoursMatch = timeText.match(/(\d+)\s*hours?/i);
  const minutesMatch = timeText.match(/(\d+)\s*minutes?/i);
  
  let totalHours = 0;
  if (hoursMatch) totalHours += parseInt(hoursMatch[1]);
  if (minutesMatch) totalHours += parseInt(minutesMatch[1]) / 60;
  
  return Math.round(totalHours * 100) / 100;
}

function parseTime(timeStr) {
  if (!timeStr) return null;
  const [hours, minutes, seconds] = timeStr.split(':').map(Number);
  const date = new Date();
  date.setHours(hours, minutes, seconds || 0, 0);
  return date;
}

function parseCost(costText) {
  if (!costText) return 0;
  // Handle format like "@R19.44 = 833,976"
  const match = costText.toString().match(/=\s*([\d,]+\.?\d*)/);
  return match ? parseFloat(match[1].replace(',', '.')) : 0;
}

function getCostCode(site) {
  const codes = {
    'BALLYCLARE': 'KFC-0001-0001-0002-0004',
    'EBONY': 'KFC-0001-0001-0002-0005',
    'DURBANVILLE': 'KFC-0001-0001-0002-0002'
  };
  return codes[site.toUpperCase()] || 'KFC-0001-0001-0003';
}

importMonthlyFinal();