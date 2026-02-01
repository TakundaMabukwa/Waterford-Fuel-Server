const { supabase } = require('../../supabase-client');

class EnergyRiteMonitoringController {

  /**
   * Get sites running longer than specified hours (default 24)
   */
  async getLongRunningSites(req, res) {
    try {
      const { hours = 24, cost_code } = req.query;
      const cutoffTime = new Date(Date.now() - parseInt(hours) * 60 * 60 * 1000);

      let query = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_status', 'ONGOING')
        .lt('session_start_time', cutoffTime.toISOString());
      
      if (cost_code) {
        query = query.eq('cost_code', cost_code);
      }
      
      const { data: longRunningSessions, error } = await query.order('session_start_time', { ascending: true });

      if (error) throw new Error(`Database error: ${error.message}`);

      // Calculate running duration for each session
      const longRunningSites = longRunningSessions.map(session => {
        const startTime = new Date(session.session_start_time);
        const hoursRunning = (Date.now() - startTime.getTime()) / (1000 * 60 * 60);
        
        return {
          ...session,
          hours_running: parseFloat(hoursRunning.toFixed(2)),
          alert_level: hoursRunning > 48 ? 'critical' : hoursRunning > 24 ? 'warning' : 'info'
        };
      });

      res.status(200).json({
        success: true,
        data: {
          threshold_hours: parseInt(hours),
          cost_code_filter: cost_code || 'all',
          long_running_sites: longRunningSites,
          count: longRunningSites.length,
          critical_sites: longRunningSites.filter(s => s.alert_level === 'critical').length
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error getting long running sites:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get top usage sites
   */
  async getTopUsageSites(req, res) {
    try {
      const { days = 30, limit = 10, cost_code } = req.query;
      const startDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000);

      let query = supabase
        .from('energy_rite_operating_sessions')
        .select('branch, cost_code, total_usage, operating_hours, cost_for_usage, session_date, company')
        .eq('session_status', 'COMPLETED')
        .gte('session_date', startDate.toISOString().split('T')[0])
        .not('total_usage', 'is', null);
      
      if (cost_code) {
        query = query.eq('cost_code', cost_code);
      }
      
      const { data: sessions, error } = await query;

      if (error) throw new Error(`Database error: ${error.message}`);

      // Group by site and calculate totals
      const siteUsage = {};
      
      sessions.forEach(session => {
        const site = session.branch;
        if (!siteUsage[site]) {
          siteUsage[site] = {
            branch: site,
            cost_code: session.cost_code,
            company: session.company,
            total_usage: 0,
            total_hours: 0,
            total_cost: 0,
            sessions: 0,
            avg_usage_per_hour: 0,
            efficiency_rank: 0
          };
        }
        
        siteUsage[site].total_usage += parseFloat(session.total_usage || 0);
        siteUsage[site].total_hours += parseFloat(session.operating_hours || 0);
        siteUsage[site].total_cost += parseFloat(session.cost_for_usage || 0);
        siteUsage[site].sessions++;
      });

      // Calculate averages and sort by usage
      const topUsageSites = Object.values(siteUsage)
        .map(site => ({
          ...site,
          avg_usage_per_hour: site.total_hours > 0 ? site.total_usage / site.total_hours : 0,
          avg_cost_per_hour: site.total_hours > 0 ? site.total_cost / site.total_hours : 0
        }))
        .sort((a, b) => b.total_usage - a.total_usage)
        .slice(0, parseInt(limit))
        .map((site, index) => ({ ...site, usage_rank: index + 1 }));

      res.status(200).json({
        success: true,
        data: {
          analysis_period: `${days} days`,
          cost_code_filter: cost_code || 'all',
          top_usage_sites: topUsageSites,
          summary: {
            total_sites_analyzed: Object.keys(siteUsage).length,
            total_usage: topUsageSites.reduce((sum, site) => sum + site.total_usage, 0),
            total_cost: topUsageSites.reduce((sum, site) => sum + site.total_cost, 0),
            highest_consumer: topUsageSites[0] || null
          }
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error getting top usage sites:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get current monitoring dashboard
   */
  async getMonitoringDashboard(req, res) {
    try {
      const { cost_code } = req.query;
      
      // Get ongoing sessions
      let ongoingQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_status', 'ONGOING');
      
      if (cost_code) {
        ongoingQuery = ongoingQuery.eq('cost_code', cost_code);
      }
      
      const { data: ongoingSessions, error: ongoingError } = await ongoingQuery.order('session_start_time', { ascending: true });

      if (ongoingError) throw new Error(`Ongoing sessions error: ${ongoingError.message}`);

      // Calculate running times and alerts
      const currentTime = Date.now();
      const activeSites = ongoingSessions.map(session => {
        const startTime = new Date(session.session_start_time);
        const hoursRunning = (currentTime - startTime.getTime()) / (1000 * 60 * 60);
        
        return {
          ...session,
          hours_running: parseFloat(hoursRunning.toFixed(2)),
          alert_level: hoursRunning > 48 ? 'critical' : hoursRunning > 24 ? 'warning' : 'normal'
        };
      });

      // Get recent activity (last 24 hours)
      const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
      let recentQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('total_usage, operating_hours, cost_for_usage')
        .eq('session_status', 'COMPLETED')
        .gte('session_start_time', twentyFourHoursAgo.toISOString());
      
      if (cost_code) {
        recentQuery = recentQuery.eq('cost_code', cost_code);
      }
      
      const { data: recentSessions, error: recentError } = await recentQuery;

      if (recentError) throw new Error(`Recent sessions error: ${recentError.message}`);

      const recentStats = recentSessions.reduce((acc, session) => ({
        usage: acc.usage + parseFloat(session.total_usage || 0),
        hours: acc.hours + parseFloat(session.operating_hours || 0),
        cost: acc.cost + parseFloat(session.cost_for_usage || 0)
      }), { usage: 0, hours: 0, cost: 0 });

      res.status(200).json({
        success: true,
        data: {
          cost_code_filter: cost_code || 'all',
          active_sites: {
            total: activeSites.length,
            normal: activeSites.filter(s => s.alert_level === 'normal').length,
            warning: activeSites.filter(s => s.alert_level === 'warning').length,
            critical: activeSites.filter(s => s.alert_level === 'critical').length,
            sites: activeSites
          },
          last_24_hours: {
            completed_sessions: recentSessions.length,
            total_usage: parseFloat(recentStats.usage.toFixed(2)),
            total_hours: parseFloat(recentStats.hours.toFixed(2)),
            total_cost: parseFloat(recentStats.cost.toFixed(2))
          },
          alerts: {
            long_running_sites: activeSites.filter(s => s.alert_level !== 'normal').length,
            sites_over_24h: activeSites.filter(s => s.hours_running > 24).length,
            sites_over_48h: activeSites.filter(s => s.hours_running > 48).length
          }
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error getting monitoring dashboard:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

module.exports = new EnergyRiteMonitoringController();