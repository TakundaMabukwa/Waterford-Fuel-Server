require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importMonthlyAccurate() {
  try {
    console.log('📊 Importing Monthly (7).xlsx with accurate parsing...');
    
    const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet);

    console.log(`📊 Found ${data.length} rows`);

    let imported = 0;
    let errors = 0;
    let sessions = [];

    // First pass: collect all running times by site and date
    const runningTimesBySite = {};
    let currentSite = null;
    let currentDate = null;
    let debugCount = 0;
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const site = row['FUEL REPORT SUMMARY'];
      const dateOrInfo = row['__EMPTY'];
      const timeInfo = row['__EMPTY_1'];
      
      if (!site || !dateOrInfo) continue;
      
      // Track current site for running times
      if (dateOrInfo.match(/^\d{4}-\d{2}-\d{2}$/) && timeInfo) {
        currentSite = site;
        currentDate = dateOrInfo;
        if (debugCount < 5) {
          console.log(`📅 Found session: ${site} ${dateOrInfo} ${timeInfo}`);
          debugCount++;
        }
      }
      
      // Debug running time rows
      if (site && dateOrInfo === 'Running Time') {
        console.log(`🕐 Running Time row: site="${site}", timeInfo="${timeInfo}"`);
        console.log(`🎯 Current context: site="${currentSite}", date="${currentDate}"`);
      }
      
      // Capture running times - format: site="BALLYCLARE", date="Running Time", __EMPTY_1="From: 10:53:08", __EMPTY_2="To: 10:56:33"
      if (site === currentSite && dateOrInfo === 'Running Time' && timeInfo?.includes('From:')) {
        const fromMatch = timeInfo.match(/From: (\d{2}:\d{2}:\d{2})/);
        const toTime = row['__EMPTY_2'];
        const toMatch = toTime?.match(/To: (\d{2}:\d{2}:\d{2})/);
        if (fromMatch && toMatch && currentDate) {
          const key = `${currentSite}_${currentDate}`;
          if (!runningTimesBySite[key]) runningTimesBySite[key] = [];
          runningTimesBySite[key].push({ from: fromMatch[1], to: toMatch[1] });
          console.log(`✅ Captured running time for ${key}: ${fromMatch[1]} to ${toMatch[1]}`);
        }
      }
    }
    
    console.log(`📊 Total running time entries found: ${Object.keys(runningTimesBySite).length}`);
    
    // Second pass: process daily entries with running times
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const site = row['FUEL REPORT SUMMARY'];
      const dateOrInfo = row['__EMPTY'];
      const timeInfo = row['__EMPTY_1'];
      
      if (!site || !dateOrInfo) continue;

      // Process daily entries
      if (site && dateOrInfo.match(/^\d{4}-\d{2}-\d{2}$/)) {
        const operatingHours = parseTimeToHours(timeInfo);
        
        // Skip sessions with 0 operating hours
        if (operatingHours === 0) {
          console.log(`🔄 No session for ${site} ${dateOrInfo}`);
          continue;
        }
        
        const key = `${site}_${dateOrInfo}`;
        const runningTimes = runningTimesBySite[key] || [];
        
        let startTime, endTime;
        
        // Use actual running times if available
        if (runningTimes.length > 0) {
          const startTimes = runningTimes.map(rt => rt.from).sort();
          const endTimes = runningTimes.map(rt => rt.to).sort();
          startTime = `${dateOrInfo}T${startTimes[0]}`;
          endTime = `${dateOrInfo}T${endTimes[endTimes.length - 1]}`;
          console.log(`🎯 Using running times for ${site} ${dateOrInfo}: ${startTime} to ${endTime}`);
        } else {
          console.log(`⏰ No running times for ${site} ${dateOrInfo}, using calculated times`);
          // Calculate based on operating hours from 06:00
          const startHour = 6;
          const endHour = Math.min(startHour + operatingHours, 23);
          startTime = `${dateOrInfo}T${String(startHour).padStart(2, '0')}:00:00`;
          endTime = `${dateOrInfo}T${String(Math.floor(endHour)).padStart(2, '0')}:${String(Math.round((endHour % 1) * 60)).padStart(2, '0')}:00`;
        }
        
        const sessionData = {
          branch: site.trim(),
          company: 'KFC',
          cost_code: getCostCode(site.trim()),
          session_date: dateOrInfo,
          session_start_time: new Date(startTime).toISOString(),
          session_end_time: new Date(endTime).toISOString(),
          operating_hours: operatingHours,
          opening_percentage: parseFloat(row['__EMPTY_2']?.replace('%', '').replace(',', '.')) || 0,
          opening_fuel: parseFloat(row['__EMPTY_3']?.replace(',', '.')) || 0,
          closing_percentage: parseFloat(row['__EMPTY_4']?.replace('%', '').replace(',', '.')) || 0,
          closing_fuel: parseFloat(row['__EMPTY_5']?.replace(',', '.')) || 0,
          total_usage: Math.abs(parseFloat(row['__EMPTY_6']?.replace(',', '.')) || 0),
          total_fill: parseFloat(row['__EMPTY_7']?.replace(',', '.')) || 0,
          liter_usage_per_hour: operatingHours > 0 ? Math.abs(parseFloat(row['__EMPTY_6']?.replace(',', '.')) || 0) / operatingHours : 0,
          cost_for_usage: parseCost(row['__EMPTY_9']),
          session_status: 'COMPLETED'
        };

        sessions.push(sessionData);
      }
    }

    console.log(`📊 Processing ${sessions.length} sessions...`);

    // Batch insert sessions (50 at a time)
    const batchSize = 50;
    for (let i = 0; i < sessions.length; i += batchSize) {
      const batch = sessions.slice(i, i + batchSize);
      
      try {
        const { data, error } = await supabase
          .from('energy_rite_operating_sessions')
          .insert(batch)
          .select('id');
        
        if (error) {
          console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
          errors += batch.length;
        } else {
          imported += data.length;
          console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}: ${data.length} sessions`);
        }
      } catch (batchError) {
        console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} error:`, batchError.message);
        errors += batch.length;
      }
      
      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    console.log(`\n✅ Import complete: ${imported} imported, ${errors} errors`);
  } catch (error) {
    console.error('❌ Import failed:', error.message);
  }
}

function parseTimeToHours(timeText) {
  if (!timeText || typeof timeText !== 'string') return 0;
  
  // Handle "X hours Y minutes" format
  const hoursMatch = timeText.match(/(\d+)\s*hours?/i);
  const minutesMatch = timeText.match(/(\d+)\s*minutes?/i);
  
  let totalHours = 0;
  if (hoursMatch) totalHours += parseInt(hoursMatch[1]);
  if (minutesMatch) totalHours += parseInt(minutesMatch[1]) / 60;
  
  // Handle "X minutes" only
  if (!hoursMatch && minutesMatch) {
    totalHours = parseInt(minutesMatch[1]) / 60;
  }
  
  return Math.round(totalHours * 100) / 100;
}

function parseCost(costText) {
  if (!costText) return 0;
  const match = costText.toString().match(/[\d,]+\.?\d*/);
  return match ? parseFloat(match[0].replace(',', '.')) : 0;
}

function getCostCode(site) {
  const codes = {
    'EBONY': 'KFC-0001-0001-0002-0005',
    'DURBANVILLE': 'KFC-0001-0001-0002-0002',
    'BLUEVALLEY': 'KFC-0001-0001-0002-0003'
  };
  return codes[site.toUpperCase()] || 'KFC-0001-0001-0003';
}

importMonthlyAccurate();