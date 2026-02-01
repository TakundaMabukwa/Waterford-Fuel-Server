require('dotenv').config();
const { supabase } = require('./supabase-client');

class WorkingDashboard {
  
  async getFilteredDashboard(filters = {}) {
    try {
      const {
        month = new Date().getMonth() + 1,
        year = new Date().getFullYear(),
        siteName = null,
        timePeriod = null
      } = filters;
      
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0);
      
      let query = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('created_at', startDate.toISOString())
        .lte('created_at', endDate.toISOString());
      
      if (siteName) {
        query = query.eq('plate', siteName);
      }
      
      const { data: sessions, error } = await query.order('created_at', { ascending: false });
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      let filteredSessions = sessions || [];
      
      if (timePeriod) {
        filteredSessions = filteredSessions.filter(session => {
          const hour = new Date(session.created_at).getHours();
          switch (timePeriod) {
            case 'morning': return hour >= 6 && hour < 12;
            case 'afternoon': return hour >= 12 && hour < 18;
            case 'evening': return hour >= 18 || hour < 6;
            default: return true;
          }
        });
      }
      
      const overview = this.calculateOverview(filteredSessions);
      const siteBreakdown = this.calculateSiteBreakdown(filteredSessions);
      
      return {
        success: true,
        filters: filters,
        period: `${month}/${year}`,
        overview: overview,
        sites: siteBreakdown,
        timestamp: new Date().toISOString()
      };
      
    } catch (error) {
      console.error('❌ Dashboard error:', error);
      throw error;
    }
  }
  
  calculateOverview(sessions) {
    const siteNames = [...new Set(sessions.map(s => s.plate).filter(p => p))];
    const timeBreakdown = this.calculateTimeBreakdown(sessions);
    
    return {
      totalSessions: sessions.length,
      activeSites: siteNames.length,
      siteNames: siteNames,
      timeBreakdown: {
        morning: {
          sessions: timeBreakdown.morning.sessions,
          percentage: sessions.length > 0 ? ((timeBreakdown.morning.sessions / sessions.length) * 100).toFixed(1) : 0
        },
        afternoon: {
          sessions: timeBreakdown.afternoon.sessions,
          percentage: sessions.length > 0 ? ((timeBreakdown.afternoon.sessions / sessions.length) * 100).toFixed(1) : 0
        },
        evening: {
          sessions: timeBreakdown.evening.sessions,
          percentage: sessions.length > 0 ? ((timeBreakdown.evening.sessions / sessions.length) * 100).toFixed(1) : 0
        }
      }
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
  
  calculateSiteBreakdown(sessions) {
    const siteGroups = {};
    
    sessions.forEach(session => {
      const siteName = session.plate || 'Unknown';
      
      if (!siteGroups[siteName]) {
        siteGroups[siteName] = {
          siteName: siteName,
          sessions: [],
          morning: { sessions: 0 },
          afternoon: { sessions: 0 },
          evening: { sessions: 0 }
        };
      }
      
      const site = siteGroups[siteName];
      site.sessions.push(session);
      
      const hour = new Date(session.created_at).getHours();
      
      if (hour >= 6 && hour < 12) {
        site.morning.sessions++;
      } else if (hour >= 12 && hour < 18) {
        site.afternoon.sessions++;
      } else if (hour >= 18 || hour < 6) {
        site.evening.sessions++;
      }
    });
    
    return Object.values(siteGroups).map(site => ({
      siteName: site.siteName,
      totalSessions: site.sessions.length,
      morning: { sessions: site.morning.sessions },
      afternoon: { sessions: site.afternoon.sessions },
      evening: { sessions: site.evening.sessions }
    }));
  }
}

async function testWorking() {
  const dashboard = new WorkingDashboard();
  
  try {
    console.log('🧪 Testing working dashboard...\n');
    
    const allData = await dashboard.getFilteredDashboard();
    console.log('✅ All data:', allData.overview.totalSessions, 'sessions');
    console.log('🏢 Sites:', allData.overview.siteNames);
    
    const eveningData = await dashboard.getFilteredDashboard({ timePeriod: 'evening' });
    console.log('🌙 Evening:', eveningData.overview.totalSessions, 'sessions');
    
    if (allData.overview.siteNames.length > 0) {
      const siteData = await dashboard.getFilteredDashboard({ 
        siteName: allData.overview.siteNames[0] 
      });
      console.log('🏢 Site filter:', siteData.overview.totalSessions, 'sessions');
    }
    
    console.log('\n✅ Dashboard is working! Filters available:');
    console.log('   📅 Month/Year');
    console.log('   🏢 Site Name');
    console.log('   🕐 Time Period (morning/afternoon/evening)');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testWorking();