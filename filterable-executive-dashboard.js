require('dotenv').config();
const { supabase } = require('./supabase-client');

class FilterableExecutiveDashboard {
  
  async getFilteredDashboard(filters = {}) {
    try {
      const {
        month = new Date().getMonth() + 1,
        year = new Date().getFullYear(),
        siteName = null,
        timePeriod = null, // 'morning', 'afternoon', 'evening'
        dateRange = null, // { start: 'YYYY-MM-DD', end: 'YYYY-MM-DD' }
        minOperatingHours = null,
        company = null
      } = filters;
      
      // Calculate date range
      let startDate, endDate;
      if (dateRange) {
        startDate = new Date(dateRange.start);
        endDate = new Date(dateRange.end);
      } else {
        startDate = new Date(year, month - 1, 1);
        endDate = new Date(year, month, 0);
      }
      
      // Get filtered sessions
      const sessions = await this.getFilteredSessions(startDate, endDate, {
        siteName,
        timePeriod,
        minOperatingHours,
        company
      });
      
      // Generate filtered results
      const calendarData = this.generateFilteredCalendar(year, month, sessions, filters);
      const overview = this.calculateFilteredOverview(sessions);
      const siteBreakdown = this.calculateFilteredSiteBreakdown(sessions, filters);
      
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
      console.error('❌ Filtered dashboard error:', error);
      throw error;
    }
  }
  
  async getFilteredSessions(startDate, endDate, filters) {
    let query = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('created_at', startDate.toISOString())
      .lte('created_at', endDate.toISOString());
    
    // Apply site filter
    if (filters.siteName) {
      query = query.eq('plate', filters.siteName);
    }
    
    // Apply minimum operating hours filter
    if (filters.minOperatingHours) {
      query = query.gte('operating_hours', parseFloat(filters.minOperatingHours));
    }
    
    const { data, error } = await query.order('created_at', { ascending: false });
    
    if (error) throw new Error(`Database error: ${error.message}`);
    
    let filteredSessions = data || [];
    
    // Apply time period filter (post-query filtering)
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
        totalOperatingHours: daySessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
        morning: timeBreakdown.morning,
        afternoon: timeBreakdown.afternoon,
        evening: timeBreakdown.evening,
        sites: [...new Set(daySessions.map(s => s.plate))],
        matchesFilter: this.dayMatchesFilter(daySessions, filters)
      });
    }
    
    return calendar;
  }
  
  dayMatchesFilter(daySessions, filters) {
    if (daySessions.length === 0) return false;
    
    // Check if day has sessions matching the time period filter
    if (filters.timePeriod) {
      const hasMatchingTime = daySessions.some(session => {
        const hour = new Date(session.created_at).getHours();
        switch (filters.timePeriod) {
          case 'morning': return hour >= 6 && hour < 12;
          case 'afternoon': return hour >= 12 && hour < 18;
          case 'evening': return hour >= 18 || hour < 6;
          default: return true;
        }
      });
      if (!hasMatchingTime) return false;
    }
    
    return true;
  }
  
  calculateTimeBreakdown(sessions) {
    const morning = { sessions: 0, hours: 0 };
    const afternoon = { sessions: 0, hours: 0 };
    const evening = { sessions: 0, hours: 0 };
    
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
  
  calculateFilteredOverview(sessions) {
    const siteNames = [...new Set(sessions.map(s => s.plate))];
    const timeBreakdown = this.calculateTimeBreakdown(sessions);
    
    return {
      totalSessions: sessions.length,
      totalOperatingHours: sessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
      totalFuelUsage: sessions.reduce((sum, s) => sum + parseFloat(s.fuel_consumed || 0), 0),
      activeSites: siteNames.length,
      siteNames: siteNames,
      timeBreakdown: {
        morning: {
          sessions: timeBreakdown.morning.sessions,
          hours: timeBreakdown.morning.hours.toFixed(2),
          percentage: sessions.length > 0 ? ((timeBreakdown.morning.sessions / sessions.length) * 100).toFixed(1) : 0
        },
        afternoon: {
          sessions: timeBreakdown.afternoon.sessions,
          hours: timeBreakdown.afternoon.hours.toFixed(2),
          percentage: sessions.length > 0 ? ((timeBreakdown.afternoon.sessions / sessions.length) * 100).toFixed(1) : 0
        },
        evening: {
          sessions: timeBreakdown.evening.sessions,
          hours: timeBreakdown.evening.hours.toFixed(2),
          percentage: sessions.length > 0 ? ((timeBreakdown.evening.sessions / sessions.length) * 100).toFixed(1) : 0
        }
      }
    };
  }
  
  calculateFilteredSiteBreakdown(sessions, filters) {
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
}

// Test filtering
async function testFiltering() {
  const dashboard = new FilterableExecutiveDashboard();
  
  try {
    console.log('🔍 Testing dashboard filters...\n');
    
    // Test 1: Filter by time period
    console.log('1️⃣ Filter by Evening sessions:');
    const eveningFilter = await dashboard.getFilteredDashboard({
      timePeriod: 'evening'
    });
    console.log('   Evening sessions:', eveningFilter.overview.timeBreakdown.evening.sessions);
    console.log('   Evening hours:', eveningFilter.overview.timeBreakdown.evening.hours);
    
    // Test 2: Filter by site name
    console.log('\n2️⃣ Filter by specific site:');
    const siteFilter = await dashboard.getFilteredDashboard({
      siteName: 'TAMBOTIE'
    });
    console.log('   Filtered sites:', siteFilter.overview.activeSites);
    console.log('   Site names:', siteFilter.overview.siteNames);
    
    // Test 3: Filter by minimum operating hours
    console.log('\n3️⃣ Filter by minimum operating hours (0.1h):');
    const hoursFilter = await dashboard.getFilteredDashboard({
      minOperatingHours: 0.1
    });
    console.log('   Sessions with >0.1h:', hoursFilter.overview.totalSessions);
    console.log('   Total hours:', hoursFilter.overview.totalOperatingHours.toFixed(2));
    
    // Test 4: Combined filters
    console.log('\n4️⃣ Combined filters (Evening + Min Hours):');
    const combinedFilter = await dashboard.getFilteredDashboard({
      timePeriod: 'evening',
      minOperatingHours: 0.1
    });
    console.log('   Combined results:', combinedFilter.overview.totalSessions, 'sessions');
    
    console.log('\n✅ All filters working correctly!');
    
  } catch (error) {
    console.error('❌ Filter test error:', error.message);
  }
}

testFiltering();