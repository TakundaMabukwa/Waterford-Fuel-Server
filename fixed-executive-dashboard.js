require('dotenv').config();
const { supabase } = require('./supabase-client');

class FixedExecutiveDashboard {
  
  async getMonthlyOverview() {
    try {
      const month = new Date().getMonth() + 1;
      const year = new Date().getFullYear();
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0);
      
      const { data: sessions, error } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('created_at', startDate.toISOString())
        .lte('created_at', endDate.toISOString())
        .order('created_at', { ascending: false });
      
      if (error) throw error;
      
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('*')
        .gte('created_at', startDate.toISOString())
        .lte('created_at', endDate.toISOString())
        .order('plate')
        .order('created_at');
      
      if (fuelError) throw fuelError;
      
      const { data: vehicleLookup, error: lookupError } = await supabase
        .from('energyrite_vehicle_lookup')
        .select('*');
      
      if (lookupError) console.log('No vehicle lookup data');
      
      const siteStats = this.calculateSiteStats(sessions, fuelData, vehicleLookup || []);
      const top10Sites = siteStats
        .sort((a, b) => b.totalFuelUsage - a.totalFuelUsage)
        .slice(0, 10);
      
      // Fixed: Only check for 24+ hours if we have actual session times
      const longRunningSites = siteStats.filter(site => 
        site.hasValidSessionTimes && site.maxContinuousHours >= 24
      );
      
      const overview = {
        period: `${month}/${year}`,
        totalSessions: sessions.length,
        totalSites: siteStats.length,
        activeSites: siteStats.filter(s => s.totalSessions > 0).length,
        totalFuelUsage: siteStats.reduce((sum, s) => sum + s.totalFuelUsage, 0),
        longRunningSites: longRunningSites.length,
        top10Sites: top10Sites,
        longRunningDetails: longRunningSites,
        timeBreakdown: this.calculateTimeBreakdown(sessions),
        dataQuality: this.assessDataQuality(sessions)
      };
      
      return overview;
      
    } catch (error) {
      console.error('❌ Dashboard error:', error);
      throw error;
    }
  }
  
  calculateSiteStats(sessions, fuelData, vehicleLookup) {
    const siteGroups = {};
    
    // Calculate fuel usage from fuel data
    const fuelBySite = {};
    fuelData.forEach(record => {
      if (!fuelBySite[record.plate]) {
        fuelBySite[record.plate] = [];
      }
      fuelBySite[record.plate].push(record);
    });
    
    Object.keys(fuelBySite).forEach(plate => {
      const records = fuelBySite[plate];
      let totalUsage = 0;
      
      for (let i = 1; i < records.length; i++) {
        const current = parseFloat(records[i].fuel_probe_1_level || 0);
        const previous = parseFloat(records[i-1].fuel_probe_1_level || 0);
        const difference = previous - current;
        
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
          sessions: [],
          hasValidSessionTimes: false
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
          sessions: [],
          hasValidSessionTimes: false
        };
      }
      
      siteGroups[siteName].totalSessions++;
      siteGroups[siteName].sessions.push(session);
    });
    
    // Add cost codes
    vehicleLookup.forEach(vehicle => {
      if (siteGroups[vehicle.plate]) {
        siteGroups[vehicle.plate].costCode = vehicle.cost_code || 'N/A';
      }
    });
    
    // Calculate continuous hours only if we have valid session times
    Object.values(siteGroups).forEach(site => {
      if (site.sessions.length > 0) {
        const result = this.calculateContinuousHours(site.sessions);
        site.maxContinuousHours = result.hours;
        site.hasValidSessionTimes = result.hasValidTimes;
      }
    });
    
    return Object.values(siteGroups);
  }
  
  calculateContinuousHours(sessions) {
    if (sessions.length === 0) return { hours: 0, hasValidTimes: false };
    
    // Check if we have valid session start/end times
    const validSessions = sessions.filter(s => s.session_start && s.session_end);
    
    if (validSessions.length === 0) {
      return { hours: 0, hasValidTimes: false };
    }
    
    const sortedSessions = validSessions.sort((a, b) => 
      new Date(a.session_start) - new Date(b.session_start)
    );
    
    let maxContinuous = 0;
    let currentContinuous = 0;
    let lastEndTime = null;
    
    sortedSessions.forEach(session => {
      const startTime = new Date(session.session_start);
      const endTime = new Date(session.session_end);
      const sessionDuration = (endTime - startTime) / (1000 * 60 * 60);
      
      if (lastEndTime && (startTime - lastEndTime) <= 60 * 60 * 1000) {
        currentContinuous += sessionDuration;
      } else {
        maxContinuous = Math.max(maxContinuous, currentContinuous);
        currentContinuous = sessionDuration;
      }
      
      lastEndTime = endTime;
    });
    
    return { 
      hours: Math.max(maxContinuous, currentContinuous), 
      hasValidTimes: true 
    };
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
  
  assessDataQuality(sessions) {
    const totalSessions = sessions.length;
    const sessionsWithStartEnd = sessions.filter(s => s.session_start && s.session_end).length;
    const sessionsWithDuration = sessions.filter(s => s.duration_minutes).length;
    
    return {
      totalSessions,
      sessionsWithStartEnd,
      sessionsWithDuration,
      dataCompleteness: totalSessions > 0 ? (sessionsWithStartEnd / totalSessions * 100).toFixed(1) : 0
    };
  }
}

async function testFixedDashboard() {
  const dashboard = new FixedExecutiveDashboard();
  
  try {
    console.log('🔧 Testing FIXED Executive Dashboard...\n');
    
    const overview = await dashboard.getMonthlyOverview();
    
    console.log('📊 FIXED EXECUTIVE DASHBOARD');
    console.log('============================');
    console.log(`📅 Period: ${overview.period}`);
    console.log(`📈 Total Sessions: ${overview.totalSessions}`);
    console.log(`🏢 Total Sites: ${overview.totalSites}`);
    console.log(`✅ Active Sites: ${overview.activeSites}`);
    console.log(`⛽ Total Fuel Usage: ${overview.totalFuelUsage.toFixed(2)}L`);
    console.log(`⚠️  Sites Running 24+ Hours: ${overview.longRunningSites}`);
    
    console.log('\n📊 DATA QUALITY ASSESSMENT:');
    console.log(`   Sessions with start/end times: ${overview.dataQuality.sessionsWithStartEnd}/${overview.dataQuality.totalSessions}`);
    console.log(`   Data completeness: ${overview.dataQuality.dataCompleteness}%`);
    
    console.log('\n🕐 TIME BREAKDOWN:');
    console.log(`   Morning: ${overview.timeBreakdown.morning.sessions} sessions`);
    console.log(`   Afternoon: ${overview.timeBreakdown.afternoon.sessions} sessions`);
    console.log(`   Evening: ${overview.timeBreakdown.evening.sessions} sessions`);
    
    console.log('\n🏆 TOP 10 SITES BY FUEL USAGE:');
    overview.top10Sites.slice(0, 5).forEach((site, index) => {
      console.log(`${index + 1}. ${site.siteName} - ${site.costCode} - ${site.totalFuelUsage.toFixed(2)}L`);
    });
    
    if (overview.longRunningDetails.length > 0) {
      console.log('\n⚠️  SITES RUNNING OVER 24 HOURS:');
      overview.longRunningDetails.forEach(site => {
        console.log(`🚨 ${site.siteName} - ${site.maxContinuousHours.toFixed(1)}h`);
      });
    } else {
      console.log('\n✅ No sites running over 24 hours (or no valid session timing data)');
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testFixedDashboard();