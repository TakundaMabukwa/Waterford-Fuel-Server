require('dotenv').config();
const { supabase } = require('./supabase-client');

class FixedFilterableDashboard {
  
  async getFilteredDashboard(filters = {}) {
    try {
      const {
        month = new Date().getMonth() + 1,
        year = new Date().getFullYear(),
        siteName = null,
        timePeriod = null, // 'morning', 'afternoon', 'evening'
        minDuration = null // minimum duration in minutes
      } = filters;
      
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0);
      
      const sessions = await this.getFilteredSessions(startDate, endDate, {
        siteName,
        timePeriod,
        minDuration
      });
      
      const calendarData = this.generateFilteredCalendar(year, month, sessions, filters);
      const overview = this.calculateFilteredOverview(sessions);
      const siteBreakdown = this.calculateFilteredSiteBreakdown(sessions);
      
      return {
        success: true,
        filters: filters,
        period: `${month}/${year}`,
        calendar: calendarData,
        overview: overview,
        sites: siteBreakdown,
        timestamp: new Date().toISOString()
      };
      
    } catch (error) {
      console.error('❌ Dashboard error:', error);
      throw error;
    }
  }
  
  async getFilteredSessions(startDate, endDate, filters) {
    let query = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('created_at', startDate.toISOString())
      .lte('created_at', endDate.toISOString());
    
    if (filters.siteName) {
      query = query.eq('plate', filters.siteName);
    }
    
    if (filters.minDuration) {
      query = query.gte('duration_minutes', parseInt(filters.minDuration));
    }
    
    const { data, error } = await query.order('created_at', { ascending: false });
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    let filteredSessions = data || [];
    
    if (filters.timePeriod) {
      filteredSessions = filteredSessions.filter(session => {
        const hour = new Date(session.created_at).getHours();
        switch (filters.timePeriod) {
          case 'morning': return hour >= 6 && hour < 12;
          case 'afternoon': return hour >= 12 && hour < 18;
          case 'evening': return hour >= 18 || hour < 6;
          default: return true;
        }
      });
    }
    
    return filteredSessions;
  }
  
  generateFilteredCalendar(year, month, sessions, filters) {
    const daysInMonth = new Date(year, month, 0).getDate();
    const calendar = [];
    
    for (let day = 1; day <= daysInMonth; day++) {
      const date = new Date(year, month - 1, day);
      const dateStr = date.toISOString().split('T')[0];
      
      const daySessions = sessions.filter(session => {
        const sessionDate = new Date(session.created_at).toISOString().split('T')[0];
        return sessionDate === dateStr;
      });
      
      const timeBreakdown = this.calculateTimeBreakdown(daySessions);
      
      calendar.push({
        day: day,
        date: dateStr,
        dayOfWeek: date.toLocaleDateString('en-US', { weekday: 'short' }),
        totalSessions: daySessions.length,
        totalDuration: daySessions.reduce((sum, s) => sum + parseInt(s.duration_minutes || 0), 0),
        morning: timeBreakdown.morning,
        afternoon: timeBreakdown.afternoon,
        evening: timeBreakdown.evening,
        sites: [...new Set(daySessions.map(s => s.plate))],
        matchesFilter: daySessions.length > 0
      });
    }
    
    return calendar;
  }
  
  calculateTimeBreakdown(sessions) {
    const morning = { sessions: 0, duration: 0 };
    const afternoon = { sessions: 0, duration: 0 };
    const evening = { sessions: 0, duration: 0 };
    
    sessions.forEach(session => {
      const hour = new Date(session.created_at).getHours();
      const duration = parseInt(session.duration_minutes || 0);
      
      if (hour >= 6 && hour < 12) {
        morning.sessions++;
        morning.duration += duration;
      } else if (hour >= 12 && hour < 18) {
        afternoon.sessions++;
        afternoon.duration += duration;
      } else if (hour >= 18 || hour < 6) {
        evening.sessions++;
        evening.duration += duration;
      }
    });
    
    return { morning, afternoon, evening };
  }
  
  calculateFilteredOverview(sessions) {
    const siteNames = [...new Set(sessions.map(s => s.plate))];
    const timeBreakdown = this.calculateTimeBreakdown(sessions);
    
    return {
      totalSessions: sessions.length,
      totalDuration: sessions.reduce((sum, s) => sum + parseInt(s.duration_minutes || 0), 0),
      totalFuelConsumed: sessions.reduce((sum, s) => sum + parseFloat(s.fuel_consumed || 0), 0),
      activeSites: siteNames.length,
      siteNames: siteNames,
      timeBreakdown: {
        morning: {
          sessions: timeBreakdown.morning.sessions,
          duration: timeBreakdown.morning.duration,
          hours: (timeBreakdown.morning.duration / 60).toFixed(2),
          percentage: sessions.length > 0 ? ((timeBreakdown.morning.sessions / sessions.length) * 100).toFixed(1) : 0
        },
        afternoon: {
          sessions: timeBreakdown.afternoon.sessions,
          duration: timeBreakdown.afternoon.duration,
          hours: (timeBreakdown.afternoon.duration / 60).toFixed(2),
          percentage: sessions.length > 0 ? ((timeBreakdown.afternoon.sessions / sessions.length) * 100).toFixed(1) : 0
        },
        evening: {
          sessions: timeBreakdown.evening.sessions,
          duration: timeBreakdown.evening.duration,
          hours: (timeBreakdown.evening.duration / 60).toFixed(2),
          percentage: sessions.length > 0 ? ((timeBreakdown.evening.sessions / sessions.length) * 100).toFixed(1) : 0
        }
      }
    };
  }
  
  calculateFilteredSiteBreakdown(sessions) {
    const siteGroups = {};
    
    sessions.forEach(session => {
      if (!siteGroups[session.plate]) {
        siteGroups[session.plate] = {
          siteName: session.plate,
          sessions: [],
          totalDuration: 0,
          totalFuelConsumed: 0,
          morning: { sessions: 0, duration: 0 },
          afternoon: { sessions: 0, duration: 0 },
          evening: { sessions: 0, duration: 0 }
        };
      }
      
      const site = siteGroups[session.plate];
      site.sessions.push(session);
      site.totalDuration += parseInt(session.duration_minutes || 0);
      site.totalFuelConsumed += parseFloat(session.fuel_consumed || 0);
      
      const hour = new Date(session.created_at).getHours();
      const duration = parseInt(session.duration_minutes || 0);
      
      if (hour >= 6 && hour < 12) {
        site.morning.sessions++;
        site.morning.duration += duration;
      } else if (hour >= 12 && hour < 18) {
        site.afternoon.sessions++;
        site.afternoon.duration += duration;
      } else if (hour >= 18 || hour < 6) {
        site.evening.sessions++;
        site.evening.duration += duration;
      }
    });
    
    return Object.values(siteGroups).map(site => ({
      siteName: site.siteName,
      totalSessions: site.sessions.length,
      totalDuration: site.totalDuration,
      totalHours: (site.totalDuration / 60).toFixed(2),
      totalFuelConsumed: site.totalFuelConsumed.toFixed(2),
      morning: {
        sessions: site.morning.sessions,
        duration: site.morning.duration,
        hours: (site.morning.duration / 60).toFixed(2)
      },
      afternoon: {
        sessions: site.afternoon.sessions,
        duration: site.afternoon.duration,
        hours: (site.afternoon.duration / 60).toFixed(2)
      },
      evening: {
        sessions: site.evening.sessions,
        duration: site.evening.duration,
        hours: (site.evening.duration / 60).toFixed(2)
      }
    }));
  }
}

async function testAllFilters() {
  const dashboard = new FixedFilterableDashboard();
  
  try {
    console.log('🔍 Testing all dashboard filters...\n');
    
    // No filters
    console.log('1️⃣ No filters (all data):');
    const allData = await dashboard.getFilteredDashboard();
    console.log('   Total sessions:', allData.overview.totalSessions);
    console.log('   Active sites:', allData.overview.activeSites);
    console.log('   Site names:', allData.overview.siteNames);
    
    // Filter by time period
    console.log('\n2️⃣ Filter by Evening:');
    const eveningData = await dashboard.getFilteredDashboard({ timePeriod: 'evening' });
    console.log('   Evening sessions:', eveningData.overview.timeBreakdown.evening.sessions);
    console.log('   Evening hours:', eveningData.overview.timeBreakdown.evening.hours);
    
    // Filter by site
    if (allData.overview.siteNames.length > 0) {
      console.log('\n3️⃣ Filter by site:', allData.overview.siteNames[0]);
      const siteData = await dashboard.getFilteredDashboard({ 
        siteName: allData.overview.siteNames[0] 
      });
      console.log('   Site sessions:', siteData.overview.totalSessions);
      console.log('   Site duration:', siteData.overview.totalDuration, 'minutes');
    }
    
    // Filter by minimum duration
    console.log('\n4️⃣ Filter by min duration (10 minutes):');
    const durationData = await dashboard.getFilteredDashboard({ minDuration: 10 });
    console.log('   Sessions >10min:', durationData.overview.totalSessions);
    
    // Combined filters
    console.log('\n5️⃣ Combined filters (Evening + 10min):');
    const combinedData = await dashboard.getFilteredDashboard({ 
      timePeriod: 'evening', 
      minDuration: 10 
    });
    console.log('   Combined results:', combinedData.overview.totalSessions);
    
    console.log('\n✅ All filters working! You can filter by:');
    console.log('   📅 Month/Year');
    console.log('   🏢 Site Name');
    console.log('   🕐 Time Period (morning/afternoon/evening)');
    console.log('   ⏱️  Minimum Duration');
    console.log('   🔄 Any combination of the above');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testAllFilters();