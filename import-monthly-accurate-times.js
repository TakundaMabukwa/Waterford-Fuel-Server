require('dotenv').config();
const XLSX = require('xlsx');
const { supabase } = require('./supabase-client');

async function importMonthlyData() {
  try {
    console.log('📊 Importing Monthly (7).xlsx with accurate times...');
    
    // Clear existing data
    await supabase.from('energy_rite_operating_sessions').delete().neq('id', 0);
    
    const workbook = XLSX.readFile('./historical-imports/Monthly (7).xlsx');
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
    
    let currentBranch = '';
    let runningTimes = [];
    let sessions = [];
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!row || !row[0]) continue;
      
      const text = row[0].toString().trim();
      
      // Extract branch name
      if (text.includes('Total Running Hours')) {
        currentBranch = text.split(' Total Running Hours')[0].trim();
        runningTimes = [];
        continue;
      }
      
      // Extract running times
      if (text.includes('Running Time From:')) {
        const match = text.match(/From: (\d{2}:\d{2}:\d{2}) To: (\d{2}:\d{2}:\d{2})/);
        if (match) {
          runningTimes.push({ start: match[1], end: match[2] });
        }
        continue;
      }
      
      // Extract daily session data
      const dateMatch = text.match(/(\d{4}-\d{2}-\d{2})/);
      if (dateMatch && currentBranch) {
        const sessionDate = dateMatch[1];
        const parts = text.split(' ');
        
        // Parse operating hours
        let operatingHours = 0;
        const hoursMatch = text.match(/(\d+) hours? (\d+) minutes?/);
        if (hoursMatch) {
          operatingHours = parseFloat(hoursMatch[1]) + parseFloat(hoursMatch[2]) / 60;
        } else {
          const minutesMatch = text.match(/(\d+) minutes?/);
          if (minutesMatch) {
            operatingHours = parseFloat(minutesMatch[1]) / 60;
          }
        }
        
        // Parse all data from the row
        const percentages = text.match(/(\d+),(\d+)%/g) || [];
        const volumes = text.match(/(\d+),(\d+)/g) || [];
        const costMatch = text.match(/@R([\d,\.]+) = ([\d,\.]+)/);
        
        let openingPercentage = null, closingPercentage = null;
        let openingFuel = null, closingFuel = null;
        let totalUsage = null, totalFill = null;
        let costForUsage = null, costPerLiter = null;
        let literUsagePerHour = null;
        
        // Extract percentages
        if (percentages.length >= 1) {
          openingPercentage = parseFloat(percentages[0].replace(',', '.').replace('%', ''));
        }
        if (percentages.length >= 2) {
          closingPercentage = parseFloat(percentages[1].replace(',', '.').replace('%', ''));
        }
        
        // Extract fuel volumes
        if (volumes.length >= 1) {
          openingFuel = parseFloat(volumes[0].replace(',', '.'));
        }
        if (volumes.length >= 2) {
          closingFuel = parseFloat(volumes[1].replace(',', '.'));
        }
        
        // Parse usage (negative values) and fills (positive)
        const usageMatch = text.match(/-(\d+),(\d+)/);
        if (usageMatch) {
          totalUsage = parseFloat(`${usageMatch[1]}.${usageMatch[2]}`);
        }
        
        const fillMatch = text.match(/\+(\d+),(\d+)/);
        if (fillMatch) {
          totalFill = parseFloat(`${fillMatch[1]}.${fillMatch[2]}`);
        }
        
        // Calculate usage per hour
        if (totalUsage && operatingHours > 0) {
          literUsagePerHour = totalUsage / operatingHours;
        }
        
        // Parse cost data
        if (costMatch) {
          costPerLiter = parseFloat(costMatch[1].replace(',', '.'));
          costForUsage = parseFloat(costMatch[2].replace(',', '.'));
        }
        
        // Calculate session times
        let sessionStartTime = `${sessionDate}T06:00:00+02:00`;
        let sessionEndTime = `${sessionDate}T18:00:00+02:00`;
        
        if (runningTimes.length > 0 && operatingHours > 0) {
          const firstTime = runningTimes[0];
          sessionStartTime = `${sessionDate}T${firstTime.start}+02:00`;
          sessionEndTime = `${sessionDate}T${firstTime.end}+02:00`;
        }
        
        sessions.push({
          branch: currentBranch,
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
          cost_per_liter: costPerLiter,
          cost_for_usage: costForUsage,
          session_status: 'COMPLETED'
        });
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

importMonthlyData();