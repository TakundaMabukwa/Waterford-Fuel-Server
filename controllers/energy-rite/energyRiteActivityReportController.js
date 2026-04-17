const { supabase } = require('../../supabase-client');
const activitySnapshots = require('../../helpers/activity-snapshots');

function toPositiveNumber(value) {
  const parsed = parseFloat(value || 0);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function calculateProbeUsage(session) {
  const openingProbe1 = toPositiveNumber(session.opening_fuel_probe_1);
  const closingProbe1 = toPositiveNumber(session.closing_fuel_probe_1);
  const openingProbe2 = toPositiveNumber(session.opening_fuel_probe_2);
  const closingProbe2 = toPositiveNumber(session.closing_fuel_probe_2);

  const probe1Usage = Math.max(0, openingProbe1 - closingProbe1);
  const probe2Usage = Math.max(0, openingProbe2 - closingProbe2);
  const combinedUsage = probe1Usage + probe2Usage;
  const totalUsage = toPositiveNumber(session.total_usage || session.fuel_usage);

  return {
    fuel_usage_probe_1: probe1Usage,
    fuel_usage_probe_2: probe2Usage,
    total_fuel_usage: combinedUsage > 0 ? combinedUsage : totalUsage,
  };
}

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
        site_id,
        costCode,
        cost_code,
        costCodes,
        date,
        start_date,
        end_date
      } = req.query;

      const requestedSite = site_id || site || null;
      const requestedCostCode = cost_code || costCode || null;
      const start = start_date || date || new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().split('T')[0];
      const end = end_date || date || start;

      const costCenterAccess = require('../../helpers/cost-center-access');
      let allSites = [];
      const costCodeMap = {};

      if (requestedSite) {
        const { data: siteData } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .eq('plate', requestedSite)
          .single();

        if (siteData) {
          allSites = [siteData.plate];
          costCodeMap[siteData.plate] = siteData.cost_code;
        }
      }

      if ((!requestedSite || allSites.length === 0) && (requestedCostCode || costCodes)) {
        let accessibleCostCodes = [];

        if (costCodes) {
          const codeArray = String(costCodes).split(',').map(c => c.trim()).filter(Boolean);
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (requestedCostCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(requestedCostCode);
        }

        accessibleCostCodes = [...new Set(accessibleCostCodes)];

        const { data: vehicleLookup, error: lookupError } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .in('cost_code', accessibleCostCodes);

        if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);
        allSites = (vehicleLookup || []).map(v => v.plate);
        (vehicleLookup || []).forEach(v => {
          costCodeMap[v.plate] = v.cost_code;
        });
      } else if (!requestedSite && !requestedCostCode && !costCodes) {
        const { data: vehicleLookup, error: lookupError } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code');

        if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);
        allSites = (vehicleLookup || []).map(v => v.plate);
        (vehicleLookup || []).forEach(v => {
          costCodeMap[v.plate] = v.cost_code;
        });
      }

      const initializeSiteReport = (siteName, fallbackCostCode = null) => ({
        generator: siteName,
        branch: siteName,
        cost_code: costCodeMap[siteName] || fallbackCostCode || requestedCostCode || '',
        company: 'KFC',
        start_time: null,
        end_time: null,
        morning_to_afternoon_usage: 0,
        afternoon_to_evening_usage: 0,
        morning_to_evening_usage: 0,
        peak_time_slot: 'morning_to_afternoon',
        peak_fuel_usage: 0,
        total_fuel_usage: 0,
        total_fuel_usage_probe_1: 0,
        total_fuel_usage_probe_2: 0,
        total_sessions: 0,
        session_count: 0,
        total_operating_hours: 0,
        total_fuel_filled: 0,
        peak_usage_session: null,
        period_breakdown: {
          morning_to_afternoon: 0,
          afternoon_to_evening: 0,
          full_day_total: 0
        }
      });

      const siteReports = {};
      allSites.forEach(siteName => {
        siteReports[siteName] = initializeSiteReport(siteName);
      });

      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .in('session_status', ['COMPLETED', 'ONGOING'])
        .gte('session_date', start)
        .lte('session_date', end);

      let fillsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_status', 'FUEL_FILL_COMPLETED')
        .gte('session_date', start)
        .lte('session_date', end);

      if (allSites.length > 0 && (requestedSite || requestedCostCode || costCodes)) {
        sessionsQuery = sessionsQuery.in('branch', allSites);
        fillsQuery = fillsQuery.in('branch', allSites);
      }

      const [sessionsResult, fillsResult] = await Promise.all([
        sessionsQuery.order('session_start_time', { ascending: true }),
        fillsQuery.order('session_start_time', { ascending: true })
      ]);

      if (sessionsResult.error) throw new Error(`Sessions error: ${sessionsResult.error.message}`);
      if (fillsResult.error) throw new Error(`Fills error: ${fillsResult.error.message}`);

      const sessions = sessionsResult.data || [];
      const fills = fillsResult.data || [];

      const timeSlotTotals = {
        morning_to_afternoon: 0,
        afternoon_to_evening: 0,
        morning_to_evening: 0
      };

      const getTimeSlot = (timestamp) => {
        const match = String(timestamp || '').match(/T(\d{2}):/);
        const hour = match ? parseInt(match[1], 10) : 0;
        return hour < 12 ? 'morning_to_afternoon' : 'afternoon_to_evening';
      };

      sessions.forEach(session => {
        const siteName = session.branch;
        if (!siteReports[siteName]) {
          siteReports[siteName] = initializeSiteReport(siteName, session.cost_code);
        }

        const report = siteReports[siteName];
        const probeUsage = calculateProbeUsage(session);
        const totalUsage = probeUsage.total_fuel_usage;
        const slot = getTimeSlot(session.session_start_time);

        report.total_sessions += 1;
        report.session_count += 1;
        report.total_operating_hours += parseFloat(session.operating_hours || 0);
        report.total_fuel_usage += totalUsage;
        report.total_fuel_usage_probe_1 += probeUsage.fuel_usage_probe_1;
        report.total_fuel_usage_probe_2 += probeUsage.fuel_usage_probe_2;
        report[slot] += totalUsage;
        report.morning_to_evening_usage += totalUsage;
        report.period_breakdown.morning_to_afternoon = report.morning_to_afternoon_usage;
        report.period_breakdown.afternoon_to_evening = report.afternoon_to_evening_usage;
        report.period_breakdown.full_day_total = report.morning_to_evening_usage;

        timeSlotTotals[slot] += totalUsage;
        timeSlotTotals.morning_to_evening += totalUsage;

        if (session.session_start_time && (!report.start_time || new Date(session.session_start_time) < new Date(report.start_time))) {
          report.start_time = session.session_start_time;
        }

        if (session.session_status === 'ONGOING' || !session.session_end_time) {
          report.end_time = null;
        } else if (report.end_time !== null && (!report.end_time || new Date(session.session_end_time) > new Date(report.end_time))) {
          report.end_time = session.session_end_time;
        } else if (report.end_time === null && report.total_sessions === 1 && session.session_end_time) {
          report.end_time = session.session_end_time;
        }

        if (totalUsage > (report.peak_fuel_usage || 0)) {
          report.peak_fuel_usage = totalUsage;
          report.peak_usage_session = session.session_start_time;
          report.peak_time_slot = slot;
        }
      });

      fills.forEach(fill => {
        const siteName = fill.branch;
        if (!siteReports[siteName]) {
          siteReports[siteName] = initializeSiteReport(siteName, fill.cost_code);
        }
        siteReports[siteName].total_fuel_filled += parseFloat(fill.total_fill || 0);
      });

      Object.values(siteReports).forEach(report => {
        if (!report.end_time) {
          const hasOngoingSession = sessions.some(session => session.branch === report.branch && (session.session_status === 'ONGOING' || !session.session_end_time));
          if (!hasOngoingSession) {
            const completedEndTimes = sessions
              .filter(session => session.branch === report.branch && session.session_end_time)
              .map(session => session.session_end_time)
              .sort();
            if (completedEndTimes.length > 0) {
              report.end_time = completedEndTimes[completedEndTimes.length - 1];
            }
          }
        }
      });

      const overallPeakSlot = Object.entries({
        morning_to_afternoon: timeSlotTotals.morning_to_afternoon,
        afternoon_to_evening: timeSlotTotals.afternoon_to_evening
      }).sort(([, a], [, b]) => b - a)[0] || ['morning_to_afternoon', 0];

      const reportData = Object.values(siteReports)
        .filter(report => report.total_sessions > 0 || report.total_fuel_filled > 0 || report.total_operating_hours > 0)
        .sort((a, b) => (b.total_fuel_usage || 0) - (a.total_fuel_usage || 0));

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
            total_sites: reportData.length,
            total_sessions: reportData.reduce((sum, r) => sum + (r.total_sessions || 0), 0),
            total_operating_hours: reportData.reduce((sum, r) => sum + (r.total_operating_hours || 0), 0),
            total_fuel_usage: reportData.reduce((sum, r) => sum + (r.total_fuel_usage || 0), 0),
            total_fuel_usage_probe_1: reportData.reduce((sum, r) => sum + (r.total_fuel_usage_probe_1 || 0), 0),
            total_fuel_usage_probe_2: reportData.reduce((sum, r) => sum + (r.total_fuel_usage_probe_2 || 0), 0),
            total_fuel_filled: reportData.reduce((sum, r) => sum + (r.total_fuel_filled || 0), 0)
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
