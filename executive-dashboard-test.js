require('dotenv').config();
const { supabase } = require('./supabase-client');

class ExecutiveDashboardTest {
  
  async getMonthlyOverview() {
    try {
      const month = new Date().getMonth() + 1;
      const year = new Date().getFullYear();
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0);
      
      // Get all sessions for the month
      const { data: sessions, error } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('created_at', startDate.toISOString())
        .lte('created_at', endDate.toISOString())
        .order('created_at', { ascending: false });
      
      if (error) throw error;
      
      // Get fuel data to calculate usage
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('*')
        .gte('created_at', startDate.toISOString())
        .lte('created_at', endDate.toISOString())
        .order('plate')
        .order('created_at');
      
      if (fuelError) throw fuelError;
      
      // Get vehicle lookup for cost codes
      const { data: vehicleLookup, error: lookupError } = await supabase
        .from('energyrite_vehicle_lookup')
        .select('*');
      
      if (lookupError) console.log('No vehicle lookup data');
      
      // Calculate site usage and detect 24+ hour operations
      const siteStats = this.calculateSiteStats(sessions, fuelData, vehicleLookup || []);
      
      // Get top 10 sites by usage
      const top10Sites = siteStats
        .sort((a, b) => b.totalFuelUsage - a.totalFuelUsage)
        .slice(0, 10);
      
      // Find sites running over 24 hours
      const longRunningSites = siteStats.filter(site => site.maxContinuousHours >= 24);
      
      const overview = {
        period: `${month}/${year}`,
        totalSessions: sessions.length,
        totalSites: siteStats.length,
        activeSites: siteStats.filter(s => s.totalSessions > 0).length,
        totalFuelUsage: siteStats.reduce((sum, s) => sum + s.totalFuelUsage, 0),
        longRunningSites: longRunningSites.length,
        top10Sites: top10Sites,
        longRunningDetails: longRunningSites,
        timeBreakdown: this.calculateTimeBreakdown(sessions)
      };
      
      return overview;
      
    } catch (error) {
      console.error('❌ Dashboard error:', error);
      throw error;
    }
  }
  
  calculateSiteStats(sessions, fuelData, vehicleLookup) {
    const siteGroups = {};
    
    // Group fuel data by site to calculate usage
    const fuelBySite = {};
    fuelData.forEach(record => {
      if (!fuelBySite[record.plate]) {
        fuelBySite[record.plate] = [];
      }
      fuelBySite[record.plate].push(record);
    });
    
    // Calculate fuel usage for each site
    Object.keys(fuelBySite).forEach(plate => {
      const records = fuelBySite[plate];
      let totalUsage = 0;
      
      for (let i = 1; i < records.length; i++) {
        const current = parseFloat(records[i].fuel_probe_1_level || 0);
        const previous = parseFloat(records[i-1].fuel_probe_1_level || 0);
        const difference = previous - current; // Usage is decrease in fuel
        
        if (difference > 0) {
          totalUsage += difference;
        }
      }
      
      if (!siteGroups[plate]) {
        siteGroups[plate] = {
          siteName: plate,
          totalSessions: 0,
          totalFuelUsage: 0,
          maxContinuousHours: 0,
          costCode: 'N/A',
          sessions: []
        };
      }
      
      siteGroups[plate].totalFuelUsage = totalUsage;
    });
    
    // Add session data
    sessions.forEach(session => {
      const siteName = session.plate || 'Unknown';
      
      if (!siteGroups[siteName]) {
        siteGroups[siteName] = {
          siteName: siteName,
          totalSessions: 0,
          totalFuelUsage: 0,
          maxContinuousHours: 0,
          costCode: 'N/A',
          sessions: []
        };
      }
      
      siteGroups[siteName].totalSessions++;
      siteGroups[siteName].sessions.push(session);
    });
    
    // Add cost codes from vehicle lookup
    vehicleLookup.forEach(vehicle => {
      if (siteGroups[vehicle.plate]) {
        siteGroups[vehicle.plate].costCode = vehicle.cost_code || 'N/A';
      }
    });
    
    // Calculate continuous operation hours
    Object.values(siteGroups).forEach(site => {
      if (site.sessions.length > 0) {
        site.maxContinuousHours = this.calculateContinuousHours(site.sessions);
      }
    });
    
    return Object.values(siteGroups);
  }
  
  calculateContinuousHours(sessions) {
    if (sessions.length === 0) return 0;
    
    // Sort sessions by start time
    const sortedSessions = sessions.sort((a, b) => 
      new Date(a.created_at) - new Date(b.created_at)
    );
    
    let maxContinuous = 0;
    let currentContinuous = 0;
    let lastEndTime = null;
    
    sortedSessions.forEach(session => {
      const startTime = new Date(session.created_at);
      const endTime = session.session_end ? new Date(session.session_end) : new Date();
      
      if (lastEndTime && (startTime - lastEndTime) <= 60 * 60 * 1000) { // Within 1 hour
        currentContinuous += (endTime - startTime) / (1000 * 60 * 60); // Convert to hours
      } else {
        maxContinuous = Math.max(maxContinuous, currentContinuous);
        currentContinuous = (endTime - startTime) / (1000 * 60 * 60);
      }
      
      lastEndTime = endTime;
    });
    
    return Math.max(maxContinuous, currentContinuous);
  }
  
  calculateTimeBreakdown(sessions) {
    const morning = { sessions: 0 };
    const afternoon = { sessions: 0 };
    const evening = { sessions: 0 };
    
    sessions.forEach(session => {
      const hour = new Date(session.created_at).getHours();
      
      if (hour >= 6 && hour < 12) {
        morning.sessions++;
      } else if (hour >= 12 && hour < 18) {
        afternoon.sessions++;
      } else if (hour >= 18 || hour < 6) {
        evening.sessions++;
      }
    });
    
    return { morning, afternoon, evening };
  }
}

async function runExecutiveDashboardTest() {
  const dashboard = new ExecutiveDashboardTest();
  
  try {
    console.log('🔄 Running Executive Dashboard Test on Existing Data...\n');
    
    const overview = await dashboard.getMonthlyOverview();
    
    console.log('📊 EXECUTIVE DASHBOARD OVERVIEW');
    console.log('================================');
    console.log(`📅 Period: ${overview.period}`);
    console.log(`📈 Total Sessions: ${overview.totalSessions}`);
    console.log(`🏢 Total Sites: ${overview.totalSites}`);
    console.log(`✅ Active Sites: ${overview.activeSites}`);
    console.log(`⛽ Total Fuel Usage: ${overview.totalFuelUsage.toFixed(2)}L`);
    console.log(`⚠️  Sites Running 24+ Hours: ${overview.longRunningSites}`);
    
    console.log('\n🕐 TIME BREAKDOWN:');
    console.log(`   Morning (6AM-12PM): ${overview.timeBreakdown.morning.sessions} sessions`);
    console.log(`   Afternoon (12PM-6PM): ${overview.timeBreakdown.afternoon.sessions} sessions`);
    console.log(`   Evening (6PM-6AM): ${overview.timeBreakdown.evening.sessions} sessions`);
    
    console.log('\n🏆 TOP 10 SITES BY FUEL USAGE:');
    console.log('===============================');
    overview.top10Sites.forEach((site, index) => {
      console.log(`${index + 1}. ${site.siteName}`);
      console.log(`   Cost Code: ${site.costCode}`);
      console.log(`   Fuel Usage: ${site.totalFuelUsage.toFixed(2)}L`);
      console.log(`   Sessions: ${site.totalSessions}`);
      console.log('');
    });
    
    if (overview.longRunningDetails.length > 0) {
      console.log('⚠️  SITES RUNNING OVER 24 HOURS:');
      console.log('=================================');
      overview.longRunningDetails.forEach(site => {
        console.log(`🚨 ${site.siteName}`);
        console.log(`   Cost Code: ${site.costCode}`);
        console.log(`   Max Continuous Hours: ${site.maxContinuousHours.toFixed(1)}h`);
        console.log(`   Total Sessions: ${site.totalSessions}`);
        console.log('');
      });
    } else {
      console.log('✅ No sites running over 24 hours detected');
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

runExecutiveDashboardTest();