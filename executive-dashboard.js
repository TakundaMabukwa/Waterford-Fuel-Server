require('dotenv').config();
const { supabase } = require('./supabase-client');

class ExecutiveDashboard {
  
  async getMonthlyOverview(targetMonth = null, targetYear = null) {
    try {
      const now = new Date();
      const month = targetMonth || now.getMonth() + 1;
      const year = targetYear || now.getFullYear();
      
      // Get last month for comparison
      const lastMonth = month === 1 ? 12 : month - 1;
      const lastMonthYear = month === 1 ? year - 1 : year;
      
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0);
      const lastMonthStart = new Date(lastMonthYear, lastMonth - 1, 1);
      const lastMonthEnd = new Date(lastMonthYear, lastMonth, 0);
      
      // Get operating sessions for current and last month
      const [currentMonthSessions, lastMonthSessions] = await Promise.all([
        this.getSessionsForPeriod(startDate, endDate),
        this.getSessionsForPeriod(lastMonthStart, lastMonthEnd)
      ]);
      
      // Generate calendar data
      const calendarData = this.generateCalendarData(year, month, currentMonthSessions);
      
      // Calculate overview stats
      const overview = this.calculateOverview(currentMonthSessions, lastMonthSessions);
      
      // Get site breakdown with time periods
      const siteBreakdown = this.calculateSiteBreakdown(currentMonthSessions);
      
      return {
        success: true,
        period: `${month}/${year}`,
        lastMonth: `${lastMonth}/${lastMonthYear}`,
        calendar: calendarData,
        overview: overview,
        sites: siteBreakdown,
        timestamp: new Date().toISOString()
      };
      
    } catch (error) {
      console.error('❌ Executive dashboard error:', error);
      throw error;
    }
  }
  
  async getSessionsForPeriod(startDate, endDate) {
    const { data, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('created_at', startDate.toISOString())
      .lte('created_at', endDate.toISOString())
      .order('created_at', { ascending: false });
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    return data || [];
  }
  
  generateCalendarData(year, month, sessions) {
    const daysInMonth = new Date(year, month, 0).getDate();
    const calendar = [];
    
    for (let day = 1; day <= daysInMonth; day++) {
      const date = new Date(year, month - 1, day);
      const dateStr = date.toISOString().split('T')[0];
      
      // Filter sessions for this day
      const daySessions = sessions.filter(session => {
        const sessionDate = new Date(session.created_at).toISOString().split('T')[0];
        return sessionDate === dateStr;
      });
      
      // Calculate time period breakdowns
      const timeBreakdown = this.calculateTimeBreakdown(daySessions);
      
      calendar.push({
        day: day,
        date: dateStr,
        dayOfWeek: date.toLocaleDateString('en-US', { weekday: 'short' }),
        totalSessions: daySessions.length,
        totalOperatingHours: daySessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
        totalFuelUsage: daySessions.reduce((sum, s) => sum + parseFloat(s.fuel_consumed || 0), 0),
        morning: timeBreakdown.morning,
        afternoon: timeBreakdown.afternoon,
        evening: timeBreakdown.evening,
        sites: [...new Set(daySessions.map(s => s.plate))].length
      });
    }
    
    return calendar;
  }
  
  calculateTimeBreakdown(sessions) {
    const morning = { sessions: 0, hours: 0 }; // 6AM - 12PM
    const afternoon = { sessions: 0, hours: 0 }; // 12PM - 6PM
    const evening = { sessions: 0, hours: 0 }; // 6PM - 12AM
    
    sessions.forEach(session => {
      const hour = new Date(session.created_at).getHours();
      const operatingHours = parseFloat(session.operating_hours || 0);
      
      if (hour >= 6 && hour < 12) {
        morning.sessions++;
        morning.hours += operatingHours;
      } else if (hour >= 12 && hour < 18) {
        afternoon.sessions++;
        afternoon.hours += operatingHours;
      } else if (hour >= 18 || hour < 6) {
        evening.sessions++;
        evening.hours += operatingHours;
      }
    });
    
    return { morning, afternoon, evening };
  }
  
  calculateOverview(currentSessions, lastMonthSessions) {
    const current = {
      totalSessions: currentSessions.length,
      totalOperatingHours: currentSessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
      totalFuelUsage: currentSessions.reduce((sum, s) => sum + parseFloat(s.fuel_consumed || 0), 0),
      activeSites: [...new Set(currentSessions.map(s => s.plate))].length
    };
    
    const lastMonth = {
      totalSessions: lastMonthSessions.length,
      totalOperatingHours: lastMonthSessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
      totalFuelUsage: lastMonthSessions.reduce((sum, s) => sum + parseFloat(s.fuel_consumed || 0), 0),
      activeSites: [...new Set(lastMonthSessions.map(s => s.plate))].length
    };
    
    return {
      current: current,
      lastMonth: lastMonth,
      changes: {
        sessions: this.calculatePercentageChange(current.totalSessions, lastMonth.totalSessions),
        operatingHours: this.calculatePercentageChange(current.totalOperatingHours, lastMonth.totalOperatingHours),
        fuelUsage: this.calculatePercentageChange(current.totalFuelUsage, lastMonth.totalFuelUsage),
        activeSites: this.calculatePercentageChange(current.activeSites, lastMonth.activeSites)
      }
    };
  }
  
  calculateSiteBreakdown(sessions) {
    const siteGroups = {};
    
    sessions.forEach(session => {
      if (!siteGroups[session.plate]) {
        siteGroups[session.plate] = {
          siteName: session.plate,
          sessions: [],
          totalOperatingHours: 0,
          totalFuelUsage: 0,
          morning: { sessions: 0, hours: 0 },
          afternoon: { sessions: 0, hours: 0 },
          evening: { sessions: 0, hours: 0 }
        };
      }
      
      const site = siteGroups[session.plate];
      site.sessions.push(session);
      site.totalOperatingHours += parseFloat(session.operating_hours || 0);
      site.totalFuelUsage += parseFloat(session.fuel_consumed || 0);
      
      // Time period breakdown
      const hour = new Date(session.created_at).getHours();
      const operatingHours = parseFloat(session.operating_hours || 0);
      
      if (hour >= 6 && hour < 12) {
        site.morning.sessions++;
        site.morning.hours += operatingHours;
      } else if (hour >= 12 && hour < 18) {
        site.afternoon.sessions++;
        site.afternoon.hours += operatingHours;
      } else if (hour >= 18 || hour < 6) {
        site.evening.sessions++;
        site.evening.hours += operatingHours;
      }
    });
    
    return Object.values(siteGroups).map(site => ({
      siteName: site.siteName,
      totalSessions: site.sessions.length,
      totalOperatingHours: site.totalOperatingHours.toFixed(2),
      totalFuelUsage: site.totalFuelUsage.toFixed(2),
      morning: {
        sessions: site.morning.sessions,
        operatingHours: site.morning.hours.toFixed(2)
      },
      afternoon: {
        sessions: site.afternoon.sessions,
        operatingHours: site.afternoon.hours.toFixed(2)
      },
      evening: {
        sessions: site.evening.sessions,
        operatingHours: site.evening.hours.toFixed(2)
      }
    }));
  }
  
  calculatePercentageChange(current, previous) {
    if (previous === 0) return current > 0 ? 100 : 0;
    return ((current - previous) / previous * 100).toFixed(1);
  }
}

// Test executive dashboard
async function testExecutiveDashboard() {
  const dashboard = new ExecutiveDashboard();
  
  try {
    console.log('🔄 Generating executive dashboard...');
    
    const result = await dashboard.getMonthlyOverview();
    
    console.log('✅ Executive Dashboard Generated');
    console.log('📅 Period:', result.period);
    console.log('📊 Overview:');
    console.log('   Current Sessions:', result.overview.current.totalSessions);
    console.log('   Operating Hours:', result.overview.current.totalOperatingHours);
    console.log('   Active Sites:', result.overview.current.activeSites);
    console.log('📈 Changes from last month:');
    console.log('   Sessions:', result.overview.changes.sessions + '%');
    console.log('   Hours:', result.overview.changes.operatingHours + '%');
    console.log('📅 Calendar days with activity:', result.calendar.filter(d => d.totalSessions > 0).length);
    console.log('🏢 Site breakdown:', result.sites.length, 'sites');
    
    if (result.sites.length > 0) {
      console.log('\n🏢 Top Site:', result.sites[0].siteName);
      console.log('   Morning hours:', result.sites[0].morning.operatingHours);
      console.log('   Afternoon hours:', result.sites[0].afternoon.operatingHours);
      console.log('   Evening hours:', result.sites[0].evening.operatingHours);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testExecutiveDashboard();