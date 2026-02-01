const { supabase } = require('../../supabase-client');
const activitySnapshots = require('../../helpers/activity-snapshots');

/**
 * Calculate fuel differences between two snapshots
 */
function calculateFuelDifferences(earlierSnapshot, laterSnapshot, siteReports, sitesToInclude, costCodeMap, periodName, timeSlotTotals) {
  const earlierVehicles = {};
  const laterVehicles = {};
  
  // Index vehicles by site name from earlier snapshot
  (earlierSnapshot.vehicles_data || []).forEach(vehicle => {
    const siteName = vehicle.branch || vehicle.plate;
    if (sitesToInclude.includes(siteName)) {
      earlierVehicles[siteName] = parseFloat(vehicle.fuel_level || 0);
    }
  });
  
  // Index vehicles by site name from later snapshot
  (laterSnapshot.vehicles_data || []).forEach(vehicle => {
    const siteName = vehicle.branch || vehicle.plate;
    if (sitesToInclude.includes(siteName)) {
      laterVehicles[siteName] = parseFloat(vehicle.fuel_level || 0);
    }
  });
  
  // Calculate differences for each site
  Object.keys(earlierVehicles).forEach(siteName => {
    if (laterVehicles[siteName] !== undefined) {
      const fuelDifference = earlierVehicles[siteName] - laterVehicles[siteName];
      const fuelUsage = fuelDifference > 0 ? fuelDifference : 0; // Only count positive differences as usage
      
      // Initialize site report if not exists
      if (!siteReports[siteName]) {
        siteReports[siteName] = {
          generator: siteName,
          cost_code: costCodeMap[siteName],
          company: 'KFC',
          morning_to_afternoon_usage: 0,
          afternoon_to_evening_usage: 0,
          morning_to_evening_usage: 0,
          peak_time_slot: null,
          peak_fuel_usage: 0,
          total_fuel_usage: 0,
          total_sessions: 0,
          total_operating_hours: 0
        };
      }
      
      // Add fuel usage to the appropriate period
      siteReports[siteName][`${periodName}_usage`] = (siteReports[siteName][`${periodName}_usage`] || 0) + fuelUsage;
      timeSlotTotals[periodName] += fuelUsage;
    }
  });
}

class EnergyRiteActivityReportController {

  /**
   * Get enhanced activity report with time slot fuel usage breakdown
   */
  async getActivityReport(req, res) {
    try {
      const { 
        site, 
        costCode,
        costCodes 
      } = req.query;

      // Month-to-date system: end date is always yesterday
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const end = yesterday.toISOString().split('T')[0];
      
      // Start date is the 1st of the current month
      const today = new Date();
      const startDate = new Date(today.getFullYear(), today.getMonth(), 1);
      const start = startDate.toISOString().split('T')[0];

      // Get all sites with cost codes
      const { data: allVehicles } = await supabase
        .from('energyrite_vehicle_lookup')
        .select('plate, cost_code');
      
      const costCodeMap = {};
      allVehicles?.forEach(v => {
        costCodeMap[v.plate] = v.cost_code;
      });

      // Filter sites by cost code with hierarchical access
      let sitesToInclude = Object.keys(costCodeMap);
      if (costCode || costCodes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        
        if (costCodes) {
          const codeArray = costCodes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (costCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(costCode);
        }
        
        console.log('Accessible cost codes:', accessibleCostCodes);
        console.log('Total vehicles:', allVehicles?.length);
        
        sitesToInclude = allVehicles
          .filter(v => v.cost_code && accessibleCostCodes.includes(v.cost_code))
          .map(v => v.plate);
        
        console.log('Sites to include after filtering:', sitesToInclude.length);
      }
      if (site) {
        sitesToInclude = sitesToInclude.filter(s => s === site);
      }

      // Get snapshots for time slot analysis
      const { data: snapshots } = await supabase
        .from('energy_rite_activity_snapshots')
        .select('*')
        .gte('snapshot_date', start)
        .lte('snapshot_date', end)
        .order('snapshot_time', { ascending: true });

      // Initialize site reports
      const siteReports = {};
      sitesToInclude.forEach(siteName => {
        siteReports[siteName] = {
          generator: siteName,
          cost_code: costCodeMap[siteName],
          company: 'KFC',
          morning_fuel: 0,
          afternoon_fuel: 0,
          evening_fuel: 0,
          peak_time_slot: null,
          peak_fuel_usage: 0,
          total_fuel_usage: 0,
          total_sessions: 0,
          total_operating_hours: 0
        };
      });

      // Group snapshots by date and time slot for fuel difference calculations
      const dailySnapshots = {};
      
      snapshots?.forEach(snapshot => {
        const date = snapshot.snapshot_date;
        const timeSlot = snapshot.time_slot;
        
        if (!dailySnapshots[date]) {
          dailySnapshots[date] = { morning: null, afternoon: null, evening: null };
        }
        
        dailySnapshots[date][timeSlot] = snapshot;
      });
      
      // Calculate fuel usage differences for all 3 periods
      const timeSlotTotals = { 
        morning_to_afternoon: 0, 
        afternoon_to_evening: 0,
        morning_to_evening: 0  // Full day comparison
      };
      
      Object.values(dailySnapshots).forEach(daySnapshots => {
        const { morning, afternoon, evening } = daySnapshots;
        
        // Process morning to afternoon differences (6AM to 12PM)
        if (morning && afternoon) {
          calculateFuelDifferences(morning, afternoon, siteReports, sitesToInclude, costCodeMap, 'morning_to_afternoon', timeSlotTotals);
        }
        
        // Process afternoon to evening differences (12PM to 6PM)
        if (afternoon && evening) {
          calculateFuelDifferences(afternoon, evening, siteReports, sitesToInclude, costCodeMap, 'afternoon_to_evening', timeSlotTotals);
        }
        
        // Process full day differences (6AM to 6PM)
        if (morning && evening) {
          calculateFuelDifferences(morning, evening, siteReports, sitesToInclude, costCodeMap, 'morning_to_evening', timeSlotTotals);
        }
      });

      // Get session data - filter by joining with lookup table for cost_code
      let sessions = [];
      
      if (costCode || costCodes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        
        if (costCodes) {
          const codeArray = costCodes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (costCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(costCode);
        }
        
        // Get vehicles with matching cost codes
        const vehiclesWithCostCode = allVehicles
          .filter(v => v.cost_code && accessibleCostCodes.includes(v.cost_code))
          .map(v => v.plate);
        
        if (vehiclesWithCostCode.length > 0) {
          const { data } = await supabase
            .from('energy_rite_operating_sessions')
            .select('*')
            .gte('session_date', start)
            .lte('session_date', end)
            .eq('session_status', 'COMPLETED')
            .in('branch', vehiclesWithCostCode);
          sessions = data || [];
        }
      } else {
        const { data } = await supabase
          .from('energy_rite_operating_sessions')
          .select('*')
          .gte('session_date', start)
          .lte('session_date', end)
          .eq('session_status', 'COMPLETED');
        sessions = data || [];
      }
      
      if (site) {
        sessions = sessions.filter(s => s.branch === site);
      }

      // Process sessions for additional metrics - create site reports dynamically
      sessions?.forEach(session => {
        const siteName = session.branch;
        
        // Create site report if it doesn't exist
        if (!siteReports[siteName]) {
          siteReports[siteName] = {
            generator: siteName,
            cost_code: costCodeMap[siteName] || session.cost_code,
            company: 'KFC',
            morning_fuel: 0,
            afternoon_fuel: 0,
            evening_fuel: 0,
            peak_time_slot: null,
            peak_fuel_usage: 0,
            total_fuel_usage: 0,
            total_sessions: 0,
            total_operating_hours: 0
          };
        }
        
        siteReports[siteName].total_sessions++;
        siteReports[siteName].total_operating_hours += parseFloat(session.operating_hours || 0);
        siteReports[siteName].total_fuel_usage += parseFloat(session.total_usage || 0);
      });

      // Calculate peak time slot and total fuel usage for each site
      Object.values(siteReports).forEach(report => {
        const periods = {
          morning_to_afternoon: report.morning_to_afternoon_usage || 0,
          afternoon_to_evening: report.afternoon_to_evening_usage || 0,
          morning_to_evening: report.morning_to_evening_usage || 0
        };
        
        // Find which 6-hour period had the most usage
        const peakPeriod = Object.entries(periods)
          .filter(([key]) => key !== 'morning_to_evening') // Exclude full day for peak comparison
          .sort(([,a], [,b]) => b - a)[0];
        
        report.peak_time_slot = peakPeriod[0];
        report.peak_fuel_usage = peakPeriod[1];
        report.total_fuel_usage = report.morning_to_evening_usage || 0; // Use full day total
        
        // Add period breakdown for detailed analysis
        report.period_breakdown = {
          morning_to_afternoon: report.morning_to_afternoon_usage || 0,
          afternoon_to_evening: report.afternoon_to_evening_usage || 0,
          full_day_total: report.morning_to_evening_usage || 0
        };
      });

      // Overall peak time slot (comparing 6-hour periods only)
      const periodComparison = {
        morning_to_afternoon: timeSlotTotals.morning_to_afternoon,
        afternoon_to_evening: timeSlotTotals.afternoon_to_evening
      };
      
      const overallPeakSlot = Object.entries(periodComparison)
        .sort(([,a], [,b]) => b - a)[0] || ['morning_to_afternoon', 0];

      const reportData = Object.values(siteReports)
        .filter(report => 
          (report.morning_to_afternoon_usage || 0) > 0 || 
          (report.afternoon_to_evening_usage || 0) > 0 ||
          (report.total_sessions || 0) > 0
        )
        .sort((a, b) => b.total_fuel_usage - a.total_fuel_usage);

      res.status(200).json({
        success: true,
        data: {
          period: { start_date: start, end_date: end },
          total_sites: reportData.length,
          overall_peak_time_slot: overallPeakSlot[0],
          overall_peak_usage: overallPeakSlot[1],
          time_slot_totals: timeSlotTotals,
          site_reports: reportData,
          summary: {
            total_morning_to_afternoon_usage: timeSlotTotals.morning_to_afternoon,
            total_afternoon_to_evening_usage: timeSlotTotals.afternoon_to_evening,
            total_full_day_usage: timeSlotTotals.morning_to_evening,
            period_comparison: {
              morning_period: timeSlotTotals.morning_to_afternoon,
              afternoon_period: timeSlotTotals.afternoon_to_evening,
              peak_period: overallPeakSlot[0],
              peak_usage: overallPeakSlot[1]
            },
            total_sessions: reportData.reduce((sum, r) => sum + r.total_sessions, 0),
            total_operating_hours: reportData.reduce((sum, r) => sum + r.total_operating_hours, 0)
          }
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error generating activity report:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get peak usage analysis
   */
  async getPeakUsageAnalysis(req, res) {
    try {
      const { days = 30 } = req.query;
      const startDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000);

      const { data: sessions, error } = await supabase
        .from('energy_rite_operating_sessions')
        .select('branch, session_date, total_usage, operating_hours, cost_code')
        .gte('session_date', startDate.toISOString().split('T')[0])
        .eq('session_status', 'COMPLETED')
        .order('total_usage', { ascending: false });

      if (error) throw new Error(`Database error: ${error.message}`);

      // Group by date and calculate daily totals
      const dailyUsage = {};
      sessions.forEach(session => {
        const date = session.session_date;
        if (!dailyUsage[date]) {
          dailyUsage[date] = { date, total_usage: 0, sites: new Set() };
        }
        dailyUsage[date].total_usage += parseFloat(session.total_usage || 0);
        dailyUsage[date].sites.add(session.branch);
      });

      // Convert to array and sort by usage
      const peakDays = Object.values(dailyUsage)
        .map(day => ({ ...day, sites: Array.from(day.sites) }))
        .sort((a, b) => b.total_usage - a.total_usage)
        .slice(0, 10);

      res.status(200).json({
        success: true,
        data: {
          analysis_period: `${days} days`,
          peak_usage_days: peakDays,
          highest_usage_day: peakDays[0] || null
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error generating peak usage analysis:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get comprehensive activity dashboard with snapshots and patterns
   */
  async getActivityDashboard(req, res) {
    try {
      const { startDate, endDate, days = 7 } = req.query;
      
      const defaultEndDate = new Date();
      const defaultStartDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000);
      
      const start = startDate || defaultStartDate.toISOString().split('T')[0];
      const end = endDate || defaultEndDate.toISOString().split('T')[0];
      
      // Get snapshots and patterns
      const snapshots = await activitySnapshots.getSnapshots(start, end);
      const patterns = await activitySnapshots.analyzeActivityPatterns(start, end);
      
      // Group snapshots by date
      const dailyAnalysis = {};
      const timeSlotTotals = { morning: 0, afternoon: 0, evening: 0 };
      
      snapshots.forEach(snapshot => {
        const date = snapshot.snapshot_date;
        if (!dailyAnalysis[date]) {
          dailyAnalysis[date] = {
            date,
            morning: null,
            afternoon: null,
            evening: null,
            peak_slot: null,
            peak_activity: 0
          };
        }
        
        dailyAnalysis[date][snapshot.time_slot] = {
          time_slot: snapshot.time_slot,
          time_slot_name: snapshot.time_slot_name,
          active_vehicles: snapshot.active_vehicles,
          total_vehicles: snapshot.total_vehicles,
          activity_percentage: (snapshot.active_vehicles / snapshot.total_vehicles) * 100,
          average_fuel_percentage: snapshot.average_fuel_percentage,
          snapshot_time: snapshot.snapshot_time
        };
        
        if (snapshot.active_vehicles > dailyAnalysis[date].peak_activity) {
          dailyAnalysis[date].peak_activity = snapshot.active_vehicles;
          dailyAnalysis[date].peak_slot = snapshot.time_slot;
        }
        
        timeSlotTotals[snapshot.time_slot] += snapshot.active_vehicles;
      });
      
      const overallPeakSlot = Object.entries(timeSlotTotals)
        .sort(([,a], [,b]) => b - a)[0] || ['morning', 0];
      
      const eveningPeakDays = Object.values(dailyAnalysis)
        .filter(day => day.peak_slot === 'evening').length;
      
      const dashboard = {
        period: { start_date: start, end_date: end },
        summary: {
          total_snapshots: snapshots.length,
          total_days: Object.keys(dailyAnalysis).length,
          overall_peak_time_slot: overallPeakSlot[0],
          evening_peak_days: eveningPeakDays,
          evening_peak_percentage: Object.keys(dailyAnalysis).length > 0 ? (eveningPeakDays / Object.keys(dailyAnalysis).length) * 100 : 0
        },
        daily_snapshots: Object.values(dailyAnalysis),
        time_slot_analysis: {
          morning: { total_activity: timeSlotTotals.morning, avg_activity: Object.keys(dailyAnalysis).length > 0 ? timeSlotTotals.morning / Object.keys(dailyAnalysis).length : 0 },
          afternoon: { total_activity: timeSlotTotals.afternoon, avg_activity: Object.keys(dailyAnalysis).length > 0 ? timeSlotTotals.afternoon / Object.keys(dailyAnalysis).length : 0 },
          evening: { total_activity: timeSlotTotals.evening, avg_activity: Object.keys(dailyAnalysis).length > 0 ? timeSlotTotals.evening / Object.keys(dailyAnalysis).length : 0 }
        },
        cost_code_patterns: patterns?.by_cost_code || {},
        site_utilization: patterns?.trends?.site_utilization || {},
        activity_trends: patterns?.trends || {}
      };
      
      res.status(200).json({
        success: true,
        data: dashboard,
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error generating activity dashboard:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Take manual snapshot
   */
  async takeSnapshot(req, res) {
    try {
      const snapshot = await activitySnapshots.takeSnapshot();
      
      res.status(200).json({
        success: true,
        data: snapshot,
        message: 'Activity snapshot taken successfully',
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error taking snapshot:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get activity patterns analysis
   */
  async getActivityPatterns(req, res) {
    try {
      const { startDate, endDate, days = 30 } = req.query;
      
      const defaultEndDate = new Date();
      const defaultStartDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000);
      
      const start = startDate || defaultStartDate.toISOString().split('T')[0];
      const end = endDate || defaultEndDate.toISOString().split('T')[0];
      
      const patterns = await activitySnapshots.analyzeActivityPatterns(start, end);
      
      res.status(200).json({
        success: true,
        data: {
          period: { start_date: start, end_date: end },
          patterns: patterns
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error analyzing activity patterns:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get site comparison report
   */
  async getSiteComparison(req, res) {
    try {
      const { days = 30 } = req.query;
      const startDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000);

      const { data: sessions, error } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('session_date', startDate.toISOString().split('T')[0])
        .eq('session_status', 'COMPLETED');

      if (error) throw new Error(`Database error: ${error.message}`);

      // Group by site and calculate metrics
      const siteMetrics = {};
      sessions.forEach(session => {
        const site = session.branch;
        if (!siteMetrics[site]) {
          siteMetrics[site] = {
            site,
            cost_code: session.cost_code,
            sessions: 0,
            total_hours: 0,
            total_usage: 0,
            total_cost: 0,
            efficiency_score: 0
          };
        }

        const metrics = siteMetrics[site];
        metrics.sessions++;
        metrics.total_hours += parseFloat(session.operating_hours || 0);
        metrics.total_usage += parseFloat(session.total_usage || 0);
        metrics.total_cost += parseFloat(session.cost_for_usage || 0);
      });

      // Calculate efficiency and sort
      const comparison = Object.values(siteMetrics)
        .map(site => ({
          ...site,
          avg_usage_per_hour: site.total_hours > 0 ? site.total_usage / site.total_hours : 0,
          avg_cost_per_hour: site.total_hours > 0 ? site.total_cost / site.total_hours : 0,
          efficiency_score: site.total_hours > 0 ? (site.total_usage / site.total_hours) : 0
        }))
        .sort((a, b) => b.total_usage - a.total_usage);

      res.status(200).json({
        success: true,
        data: {
          comparison_period: `${days} days`,
          site_comparison: comparison,
          top_consumer: comparison[0] || null,
          most_efficient: comparison.sort((a, b) => a.efficiency_score - b.efficiency_score)[0] || null
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error generating site comparison:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

module.exports = new EnergyRiteActivityReportController();