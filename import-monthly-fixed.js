require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importMonthlyFixed() {
  try {
    console.log('📊 Importing Monthly (7).xlsx with proper time parsing...');
    
    // Clear existing data
    await supabase.from('energy_rite_operating_sessions').delete().neq('id', 0);
    
    const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });

    let sessions = [];
    let currentBranch = '';
    let runningTimes = [];
    let lastSessionData = null;

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!row || !row[0]) continue;
      
      const text = row[0].toString().trim();
      
      // Extract branch from "Total Running Hours" line
      if (text.includes('Total Running Hours')) {
        currentBranch = text.split(' Total Running Hours')[0].trim();
        runningTimes = [];
        continue;
      }
      
      // Extract running times
      if (text.includes('Running Time') && row[1] && row[1].toString().includes('From:')) {
        const fromTime = row[1].toString().replace('From: ', '').trim();
        const toTime = row[2] ? row[2].toString().replace('To: ', '').trim() : null;
        
        if (fromTime && toTime && fromTime.match(/\d{2}:\d{2}:\d{2}/) && toTime.match(/\d{2}:\d{2}:\d{2}/)) {
          runningTimes.push({ start: fromTime, end: toTime });
        }
        continue;
      }
      
      // Process daily session data (YYYY-MM-DD format)
      const dateMatch = text.match(/(\d{4}-\d{2}-\d{2})/);
      if (dateMatch && currentBranch) {
        console.log(`Processing: ${currentBranch} ${dateMatch[1]} - Row:`, row);
        const sessionDate = dateMatch[1];
        const operatingHoursText = row[2] ? row[2].toString() : '';
        
        // Parse operating hours
        let operatingHours = 0;
        const hoursMatch = operatingHoursText.match(/(\d+) hours? (\d+) minutes?/);
        if (hoursMatch) {
          operatingHours = parseFloat(hoursMatch[1]) + parseFloat(hoursMatch[2]) / 60;
        } else {
          const minutesMatch = operatingHoursText.match(/(\d+) minutes?/);
          if (minutesMatch) {
            operatingHours = parseFloat(minutesMatch[1]) / 60;
          }
        }
        
        // Set session times
        let sessionStartTime = `${sessionDate}T06:00:00+02:00`;
        let sessionEndTime = `${sessionDate}T18:00:00+02:00`;
        
        // Use actual running times if available and operating hours > 0
        if (runningTimes.length > 0 && operatingHours > 0) {
          const firstTime = runningTimes[0];
          sessionStartTime = `${sessionDate}T${firstTime.start}+02:00`;
          sessionEndTime = `${sessionDate}T${firstTime.end}+02:00`;
        }
        
        // Parse other data
        const openingPercentage = row[3] ? parseFloat(row[3].toString().replace('%', '').replace(',', '.')) : null;
        const openingFuel = row[4] ? parseFloat(row[4].toString().replace(',', '.')) : null;
        const closingPercentage = row[5] ? parseFloat(row[5].toString().replace('%', '').replace(',', '.')) : null;
        const closingFuel = row[6] ? parseFloat(row[6].toString().replace(',', '.')) : null;
        const totalUsage = row[7] ? Math.abs(parseFloat(row[7].toString().replace(',', '.'))) : null;
        const totalFill = row[8] ? parseFloat(row[8].toString().replace(',', '.')) : null;
        const literUsagePerHour = row[9] ? parseFloat(row[9].toString().replace(',', '.')) : null;
        
        // Parse cost
        let costForUsage = null;
        if (row[10] && row[10].toString().includes('@R')) {
          const costMatch = row[10].toString().match(/@R[\d,\.]+ = ([\d,\.]+)/);
          if (costMatch) {
            costForUsage = parseFloat(costMatch[1].replace(',', '.'));
          }
        }
        
        const sessionData = {
          branch: currentBranch,
          company: 'KFC',
          cost_code: getCostCode(currentBranch),
          session_date: sessionDate,
          session_start_time: sessionStartTime,
          session_end_time: sessionEndTime,
          operating_hours: operatingHours,
          opening_percentage: openingPercentage,
          opening_fuel: openingFuel,
          closing_percentage: closingPercentage,
          closing_fuel: closingFuel,
          total_usage: totalUsage,
          total_fill: totalFill,
          liter_usage_per_hour: literUsagePerHour,
          cost_per_liter: 19.44,
          cost_for_usage: costForUsage,
          session_status: 'COMPLETED'
        };
        
        sessions.push(sessionData);
        console.log(`Added session: ${currentBranch} ${sessionDate} ${operatingHours}h`);
        
        // Clear running times after using them
        if (operatingHours > 0) {
          runningTimes = [];
        }
      }
    }
    
    // Insert sessions
    if (sessions.length > 0) {
      const { error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert(sessions);
      
      if (error) throw error;
      console.log(`✅ Imported ${sessions.length} sessions with accurate times`);
    }
    
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

function getCostCode(site) {
  const codes = {
    'EBONY': 'KFC-0001-0001-0002-0005',
    'DURBANVILLE': 'KFC-0001-0001-0002-0002',
    'BLUEVALLEY': 'KFC-0001-0001-0002-0003'
  };
  return codes[site.toUpperCase()] || 'KFC-0001-0001-0003';
}

importMonthlyFixed();