const { supabase } = require('../../supabase-client');
const activitySnapshots = require('../../helpers/activity-snapshots');
const axios = require('axios');

class EnergyRiteExecutiveDashboardController {

  /**
   * Get executive dashboard with high-level KPIs and metrics
   */
  async getExecutiveDashboard(req, res) {
    try {
      const { days = 30, costCode, costCodes, month, site_id } = req.query;
      
      let startDate, endDate, start, end;
      
      if (month) {
        // Month format: YYYY-MM (e.g., "2025-01" or "2024-12")
        const [year, monthNum] = month.split('-');
        startDate = new Date(parseInt(year), parseInt(monthNum) - 1, 1);
        endDate = new Date(parseInt(year), parseInt(monthNum), 0); // Last day of month
        
        start = startDate.toISOString().split('T')[0];
        end = endDate.toISOString().split('T')[0];
      } else {
        // Default rolling period
        endDate = new Date();
        startDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000);
        
        start = startDate.toISOString().split('T')[0];
        end = endDate.toISOString().split('T')[0];
      }
      
      // Get current fleet status from API
      const vehicleResponse = await axios.get('http://64.227.138.235:3000/api/energy-rite/vehicles');
      let vehicles = vehicleResponse.data.data;
      
      // Filter vehicles by site_id or cost code with hierarchical access
      if (site_id) {
        vehicles = vehicles.filter(v => v.branch === site_id);
      } else if (costCode || costCodes) {
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
        
        const { data: vehicleLookup } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .in('cost_code', accessibleCostCodes);
          
        const costCodeMap = {};
        vehicleLookup?.forEach(v => {
          costCodeMap[v.plate] = v.cost_code;
        });
        
        vehicles = vehicles.filter(v => {
          const vCostCode = costCodeMap[v.branch] || v.cost_code;
          return accessibleCostCodes.includes(vCostCode);
        });
      }
      
      // Get recent snapshots for trends
      const snapshots = await activitySnapshots.getSnapshots(start, end);
      
      // Get operating sessions for cost analysis with optional cost code filtering
      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('session_date', start)
        .lte('session_date', end)
        .in('session_status', ['COMPLETED', 'FUEL_FILL']);
        
      // Apply filtering by site_id or hierarchical cost code filtering if provided
      if (site_id) {
        sessionsQuery = sessionsQuery.eq('branch', site_id);
      } else if (costCode || costCodes) {
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
        
        sessionsQuery = sessionsQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: sessions, error } = await sessionsQuery;
        
      if (error) throw error;
      
      // Calculate fleet metrics
      const totalVehicles = vehicles.length;
      const activeVehicles = vehicles.filter(v => v.drivername !== 'PTO OFF / ENGINE OFF').length;
      const fleetUtilization = totalVehicles > 0 ? (activeVehicles / totalVehicles) * 100 : 0;
      
      // Calculate fuel metrics
      const totalFuelLevel = vehicles.reduce((sum, v) => sum + (parseFloat(v.fuel_probe_1_level) || 0), 0);
      const avgFuelPercentage = vehicles.length > 0 
        ? vehicles.reduce((sum, v) => sum + (parseFloat(v.fuel_probe_1_level_percentage) || 0), 0) / vehicles.length 
        : 0;
      
      // Separate engine sessions and fuel fills
      const engineSessions = sessions.filter(s => s.session_status === 'COMPLETED');
      const fuelFillSessions = sessions.filter(s => s.session_status === 'FUEL_FILL');
      
      // Calculate operational metrics from engine sessions
      const totalOperatingHours = engineSessions.reduce((sum, s) => sum + (parseFloat(s.operating_hours) || 0), 0);
      const totalFuelUsage = engineSessions.reduce((sum, s) => sum + (parseFloat(s.total_usage) || 0), 0);
      const totalOperatingCost = engineSessions.reduce((sum, s) => sum + (parseFloat(s.cost_for_usage) || 0), 0);
      
      // Calculate fuel fill metrics
      const totalFuelFills = fuelFillSessions.length;
      const totalFuelFilled = fuelFillSessions.reduce((sum, s) => sum + (parseFloat(s.total_fill) || 0), 0);
      
      // Cost center breakdown
      const costCenterMetrics = {};
      sessions.forEach(session => {
        const costCode = session.cost_code || 'UNKNOWN';
        if (!costCenterMetrics[costCode]) {
          costCenterMetrics[costCode] = {
            cost_code: costCode,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            total_cost: 0,
            sites: new Set()
          };
        }
        
        costCenterMetrics[costCode].sessions++;
        costCenterMetrics[costCode].operating_hours += parseFloat(session.operating_hours || 0);
        costCenterMetrics[costCode].fuel_usage += parseFloat(session.total_usage || 0);
        costCenterMetrics[costCode].total_cost += parseFloat(session.cost_for_usage || 0);
        costCenterMetrics[costCode].sites.add(session.branch);
      });
      
      // Convert sets to arrays and calculate averages
      const costCenterSummary = Object.values(costCenterMetrics).map(cc => ({
        ...cc,
        sites: Array.from(cc.sites),
        site_count: cc.sites.size,
        avg_cost_per_hour: cc.operating_hours > 0 ? cc.total_cost / cc.operating_hours : 0,
        avg_fuel_per_hour: cc.operating_hours > 0 ? cc.fuel_usage / cc.operating_hours : 0
      })).sort((a, b) => b.total_cost - a.total_cost);
      
      // Activity trends from snapshots
      const dailyActivity = {};
      snapshots.forEach(snapshot => {
        const date = snapshot.snapshot_date;
        if (!dailyActivity[date]) {
          dailyActivity[date] = { date, total_activity: 0, snapshots: 0 };
        }
        dailyActivity[date].total_activity += snapshot.active_vehicles;
        dailyActivity[date].snapshots++;
      });
      
      const activityTrend = Object.values(dailyActivity)
        .map(day => ({ ...day, avg_activity: day.total_activity / day.snapshots }))
        .sort((a, b) => new Date(a.date) - new Date(b.date));
      
      // Top performing sites
      const siteMetrics = {};
      sessions.forEach(session => {
        const site = session.branch;
        if (!siteMetrics[site]) {
          siteMetrics[site] = {
            site,
            cost_code: session.cost_code,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            total_cost: 0
          };
        }
        
        siteMetrics[site].sessions++;
        siteMetrics[site].operating_hours += parseFloat(session.operating_hours || 0);
        siteMetrics[site].fuel_usage += parseFloat(session.total_usage || 0);
        siteMetrics[site].total_cost += parseFloat(session.cost_for_usage || 0);
      });
      
      const topSites = Object.values(siteMetrics)
        .map(site => ({
          ...site,
          efficiency: site.operating_hours > 0 ? site.fuel_usage / site.operating_hours : 0
        }))
        .sort((a, b) => b.operating_hours - a.operating_hours)
        .slice(0, 10);
      
      const dashboard = {
        period: { 
          start_date: start, 
          end_date: end, 
          days: month ? Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) : parseInt(days),
          month_filter: month || null,
          cost_code_filter: costCode || null,
          cost_codes_filter: costCodes || null,
          site_id_filter: site_id || null
        },
        fleet_overview: {
          total_vehicles: totalVehicles,
          active_vehicles: activeVehicles,
          fleet_utilization_percentage: Math.round(fleetUtilization * 100) / 100,
          total_fuel_level: Math.round(totalFuelLevel * 100) / 100,
          average_fuel_percentage: Math.round(avgFuelPercentage * 100) / 100
        },
        operational_metrics: {
          total_operating_hours: Math.round(totalOperatingHours * 100) / 100,
          total_fuel_usage_liters: Math.round(totalFuelUsage * 100) / 100,
          total_fuel_filled_liters: Math.round(totalFuelFilled * 100) / 100,
          net_fuel_consumption: Math.round((totalFuelUsage - totalFuelFilled) * 100) / 100,
          total_operating_cost: Math.round(totalOperatingCost * 100) / 100,
          average_cost_per_hour: totalOperatingHours > 0 ? Math.round((totalOperatingCost / totalOperatingHours) * 100) / 100 : 0,
          average_fuel_per_hour: totalOperatingHours > 0 ? Math.round((totalFuelUsage / totalOperatingHours) * 100) / 100 : 0,
          total_engine_sessions: engineSessions.length,
          total_fuel_fill_events: totalFuelFills,
          fuel_fill_frequency: totalFuelFills > 0 && totalOperatingHours > 0 ? Math.round((totalFuelFills / totalOperatingHours) * 100) / 100 : 0
        },
        cost_center_performance: costCenterSummary,
        activity_trends: activityTrend,
        top_performing_sites: topSites,
        key_insights: [
          `Fleet utilization: ${fleetUtilization.toFixed(1)}%`,
          `Total operational cost: R${totalOperatingCost.toFixed(2)}`,
          `Fuel used: ${totalFuelUsage.toFixed(1)}L, Filled: ${totalFuelFilled.toFixed(1)}L`,
          `Net consumption: ${(totalFuelUsage - totalFuelFilled).toFixed(1)}L`,
          `${totalFuelFills} fuel fill events recorded`,
          `Average fuel consumption: ${totalOperatingHours > 0 ? (totalFuelUsage / totalOperatingHours).toFixed(1) : 0}L/hour`,
          `Most active cost center: ${costCenterSummary[0]?.cost_code || 'N/A'}`,
          `Top performing site: ${topSites[0]?.site || 'N/A'}`
        ]
      };
      
      res.status(200).json({
        success: true,
        data: dashboard,
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error generating executive dashboard:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

module.exports = new EnergyRiteExecutiveDashboardController();