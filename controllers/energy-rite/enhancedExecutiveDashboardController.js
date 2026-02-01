const { supabase } = require('../../supabase-client');
const { combineFuelFills } = require('../../helpers/fuel-fill-combiner');
const axios = require('axios');

class EnhancedExecutiveDashboardController {

  /**
   * Get executive dashboard focused on cumulative operations and fuel tracking
   * - Total sites operating
   * - Litres used (cumulative)
   * - Litres filled (from fuel fill events)
   * - Total operational hours
   * - Sites running over 24 hours (continuous operations)
   */
  async getExecutiveDashboard(req, res) {
    try {
      const { 
        costCode, 
        cost_code, 
        costCodes, 
        cost_codes
      } = req.query;
      
      // Support both camelCase and snake_case parameter names
      const finalCostCode = costCode || cost_code;
      const finalCostCodes = costCodes || cost_codes;
      
      // Month-to-date system: end date is always yesterday
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const targetDate = yesterday.toISOString().split('T')[0];
      
      // Start date is the 1st of the current month
      const today = new Date();
      const startDate = new Date(today.getFullYear(), today.getMonth(), 1);
      const startDateStr = startDate.toISOString().split('T')[0];
      
      const period = Math.ceil((yesterday - startDate) / (24 * 60 * 60 * 1000));
      
      console.log(`🎯 Executive Dashboard: ${startDateStr} to ${targetDate} (${period} days cumulative)`);
      console.log(`🔍 Cost code filter: ${finalCostCode || finalCostCodes || 'None'}`);
      
      // Get current fleet status
      const vehicleResponse = await axios.get('http://64.227.138.235:3000/api/energy-rite/vehicles');
      let vehicles = vehicleResponse.data.data || [];
      
      // Apply cost code filtering if provided
      if (finalCostCode || finalCostCodes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        
        if (finalCostCodes) {
          const codeArray = finalCostCodes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (finalCostCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(finalCostCode);
        }
        
        console.log(`🔐 Accessible cost codes: ${accessibleCostCodes.join(', ')}`);
        
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
      
      // Get operating sessions for the cumulative period - fetch all first
      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select(`
          *,
          session_date,
          session_start_time,
          session_end_time,
          operating_hours,
          total_usage,
          cost_for_usage,
          branch,
          cost_code
        `)
        .gte('session_date', startDateStr)
        .lte('session_date', targetDate)
        .eq('session_status', 'COMPLETED');
      
      const { data: allSessions, error: sessionsError } = await sessionsQuery;
      if (sessionsError) throw sessionsError;
      
      console.log(`📊 Found ${allSessions?.length || 0} total sessions`);
      
      // Apply cost code filtering in memory (filter by cost_code OR branch)
      let sessions = allSessions || [];
      if (finalCostCode || finalCostCodes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        let vehiclePlatesForCostCode = [];
        
        if (finalCostCodes) {
          const codeArray = finalCostCodes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (finalCostCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(finalCostCode);
        }
        
        // Get vehicle plates for these cost codes
        const { data: vehicleLookup } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .in('cost_code', accessibleCostCodes);
        
        vehiclePlatesForCostCode = vehicleLookup?.map(v => v.plate) || [];
        
        console.log(`🔐 Accessible cost codes: ${accessibleCostCodes.join(', ')}`);
        console.log(`🚗 Vehicle plates for filtering: ${vehiclePlatesForCostCode.length}`);
        
        // Filter sessions by EITHER cost_code OR branch/plate
        const beforeFilter = sessions.length;
        sessions = sessions.filter(session => {
          const matchesCostCode = accessibleCostCodes.includes(session.cost_code);
          const matchesPlate = vehiclePlatesForCostCode.includes(session.branch);
          return matchesCostCode || matchesPlate;
        });
        
        console.log(`🔍 Filtered sessions: ${beforeFilter} → ${sessions.length}`);
      }
      
      console.log(`📊 Final session count: ${sessions?.length || 0}`);
      
      // Get fuel fill SESSIONS (not events) for the cumulative period - fetch all first, then combine
      let fuelFillSessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('session_date', startDateStr)
        .lte('session_date', targetDate)
        .eq('session_status', 'FUEL_FILL_COMPLETED')
        .order('branch')
        .order('session_start_time');
      
      const { data: allFuelFillSessions, error: fillsError } = await fuelFillSessionsQuery;
      if (fillsError) throw fillsError;
      
      // Apply cost code filtering in memory
      let fuelFillSessions = allFuelFillSessions || [];
      if (finalCostCode || finalCostCodes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        let vehiclePlatesForCostCode = [];
        
        if (finalCostCodes) {
          const codeArray = finalCostCodes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (finalCostCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(finalCostCode);
        }
        
        // Get vehicle plates for these cost codes
        const { data: vehicleLookup } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .in('cost_code', accessibleCostCodes);
        
        vehiclePlatesForCostCode = vehicleLookup?.map(v => v.plate) || [];
        
        // Filter fuel fill sessions by EITHER cost_code OR plate
        const beforeFilter = fuelFillSessions.length;
        fuelFillSessions = fuelFillSessions.filter(session => {
          const matchesCostCode = accessibleCostCodes.includes(session.cost_code);
          const matchesPlate = vehiclePlatesForCostCode.includes(session.branch);
          return matchesCostCode || matchesPlate;
        });
        
        console.log(`⛽ Filtered fuel fill sessions: ${beforeFilter} → ${fuelFillSessions.length}`);
      }
      
      // Group fill sessions by vehicle and combine consecutive fills (within 2 hours)
      const sessionsByVehicle = {};
      fuelFillSessions.forEach(session => {
        if (!sessionsByVehicle[session.branch]) {
          sessionsByVehicle[session.branch] = [];
        }
        sessionsByVehicle[session.branch].push(session);
      });
      
      // Combine fills for each vehicle
      const combinedFills = [];
      Object.keys(sessionsByVehicle).forEach(vehicle => {
        const combined = combineFuelFills(sessionsByVehicle[vehicle], 2);
        combinedFills.push(...combined);
      });
      
      console.log(`⛽ Found ${fuelFillSessions.length} fill sessions, combined into ${combinedFills.length} fills`);
      
      // Calculate cumulative operational metrics using combined fills
      const totalLitresUsed = sessions.reduce((sum, s) => sum + (parseFloat(s.total_usage) || 0), 0);
      const totalLitresFilled = combinedFills.reduce((sum, f) => sum + (parseFloat(f.total_fill) || 0), 0);
      const totalOperationalHours = sessions.reduce((sum, s) => sum + (parseFloat(s.operating_hours) || 0), 0);
      const totalOperationalCost = sessions.reduce((sum, s) => sum + (parseFloat(s.cost_for_usage) || 0), 0);
      
      // Get unique sites that operated during the period
      const sitesOperatedTotal = [...new Set(sessions.map(s => s.branch))];
      
      // Get ALL sites from lookup table (with or without cost code filtering)
      let allSitesForCostCode = sitesOperatedTotal;
      if (finalCostCode || finalCostCodes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        
        if (finalCostCodes) {
          const codeArray = finalCostCodes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (finalCostCode) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(finalCostCode);
        }
        
        // Get ALL sites from lookup table for these cost codes
        const { data: allVehiclesForCostCode } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate')
          .in('cost_code', accessibleCostCodes);
        
        allSitesForCostCode = allVehiclesForCostCode?.map(v => v.plate) || [];
      } else {
        // No cost code filter - get ALL sites from lookup table
        const { data: allVehicles } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate');
        
        allSitesForCostCode = allVehicles?.map(v => v.plate) || [];
      }
      
      const totalSites = allSitesForCostCode.length;
      
      // Get sites with combined fuel fills
      const sitesWithFills = [...new Set(combinedFills.map(f => f.branch))];
      
      // Detect continuous operations (sites running over 24 hours)
      const continuousOperationsSites = await detectContinuousOperations(targetDate, finalCostCode, finalCostCodes);
      
      // Site breakdown with cumulative metrics and fuel fills
      const siteMetrics = {};
      sessions.forEach(session => {
        const site = session.branch;
        if (!siteMetrics[site]) {
          siteMetrics[site] = {
            site_name: site,
            cost_code: session.cost_code,
            total_sessions: 0,
            operating_hours: 0,
            fuel_usage_liters: 0,
            operational_cost: 0,
            fuel_fills_count: 0,
            fuel_filled_liters: 0,
            is_continuous: continuousOperationsSites.some(cs => cs.site === site)
          };
        }
        
        siteMetrics[site].total_sessions++;
        siteMetrics[site].operating_hours += parseFloat(session.operating_hours || 0);
        siteMetrics[site].fuel_usage_liters += parseFloat(session.total_usage || 0);
        siteMetrics[site].operational_cost += parseFloat(session.cost_for_usage || 0);
      });
      
      // Add combined fuel fills data to site metrics
      combinedFills.forEach(fill => {
        const site = fill.branch;
        const fillAmount = parseFloat(fill.total_fill || 0);
        
        if (siteMetrics[site]) {
          siteMetrics[site].fuel_fills_count++;
          siteMetrics[site].fuel_filled_liters += fillAmount;
        } else {
          // Site had fuel fills but no operational sessions in this period
          siteMetrics[site] = {
            site_name: site,
            cost_code: fill.cost_code,
            total_sessions: 0,
            operating_hours: 0,
            fuel_usage_liters: 0,
            operational_cost: 0,
            fuel_fills_count: 1,
            fuel_filled_liters: fillAmount,
            is_continuous: false
          };
        }
      });
      
      // Convert to array and sort by operating hours
      const siteBreakdown = Object.values(siteMetrics)
        .map(site => ({
          ...site,
          efficiency_liters_per_hour: site.operating_hours > 0 ? 
            Math.round((site.fuel_usage_liters / site.operating_hours) * 100) / 100 : 0,
          cost_per_hour: site.operating_hours > 0 ? 
            Math.round((site.operational_cost / site.operating_hours) * 100) / 100 : 0,
          fuel_net_usage: site.fuel_usage_liters - site.fuel_filled_liters, // Net fuel consumption
          fuel_efficiency_with_fills: site.fuel_filled_liters > 0 ? 
            ((site.fuel_usage_liters / site.fuel_filled_liters) * 100).toFixed(2) + '%' : 'N/A'
        }))
        .sort((a, b) => b.operating_hours - a.operating_hours);
      
      // Cost center summary with fuel fills
      const costCenterMetrics = {};
      sessions.forEach(session => {
        const costCode = session.cost_code || 'UNKNOWN';
        if (!costCenterMetrics[costCode]) {
          costCenterMetrics[costCode] = {
            cost_code: costCode,
            sites: new Set(),
            operating_hours: 0,
            fuel_usage_liters: 0,
            operational_cost: 0,
            sessions: 0,
            fuel_fills_count: 0,
            fuel_filled_liters: 0
          };
        }
        
        costCenterMetrics[costCode].sites.add(session.branch);
        costCenterMetrics[costCode].operating_hours += parseFloat(session.operating_hours || 0);
        costCenterMetrics[costCode].fuel_usage_liters += parseFloat(session.total_usage || 0);
        costCenterMetrics[costCode].operational_cost += parseFloat(session.cost_for_usage || 0);
        costCenterMetrics[costCode].sessions++;
      });
      
      // Add combined fuel fills data to cost center metrics
      combinedFills.forEach(fill => {
        const costCode = fill.cost_code || 'UNKNOWN';
        if (!costCenterMetrics[costCode]) {
          costCenterMetrics[costCode] = {
            cost_code: costCode,
            sites: new Set(),
            operating_hours: 0,
            fuel_usage_liters: 0,
            operational_cost: 0,
            sessions: 0,
            fuel_fills_count: 0,
            fuel_filled_liters: 0
          };
        }
        
        costCenterMetrics[costCode].sites.add(fill.branch);
        costCenterMetrics[costCode].fuel_fills_count++;
        costCenterMetrics[costCode].fuel_filled_liters += parseFloat(fill.total_fill || 0);
      });
      
      const costCenterSummary = Object.values(costCenterMetrics)
        .map(cc => ({
          cost_code: cc.cost_code,
          sites_count: cc.sites.size,
          sites: Array.from(cc.sites),
          operating_hours: Math.round(cc.operating_hours * 100) / 100,
          fuel_usage_liters: Math.round(cc.fuel_usage_liters * 100) / 100,
          fuel_filled_liters: Math.round(cc.fuel_filled_liters * 100) / 100,
          operational_cost: Math.round(cc.operational_cost * 100) / 100,
          sessions: cc.sessions,
          fuel_fills_count: cc.fuel_fills_count,
          avg_fuel_per_hour: cc.operating_hours > 0 ? 
            Math.round((cc.fuel_usage_liters / cc.operating_hours) * 100) / 100 : 0,
          fuel_net_usage: Math.round((cc.fuel_usage_liters - cc.fuel_filled_liters) * 100) / 100
        }))
        .sort((a, b) => b.operating_hours - a.operating_hours);
      
      // Current fleet status
      const currentlyActive = vehicles.filter(v => v.drivername !== 'PTO OFF / ENGINE OFF').length;
      const totalFleetSize = vehicles.length;
      const fleetUtilization = totalFleetSize > 0 ? (currentlyActive / totalFleetSize) * 100 : 0;
      
      // Build response
      const dashboard = {
        period: {
          start_date: startDateStr,
          end_date: targetDate, 
          days: parseInt(period),
          is_cumulative: true
        },
        filters: {
          cost_code: costCode || null,
          cost_codes: costCodes || null
        },
        
        // Key metrics you requested (now cumulative)
        key_metrics: {
          total_sites_operated: totalSites,
          sites_list: allSitesForCostCode,
          total_litres_used: Math.round(totalLitresUsed * 100) / 100,
          total_litres_filled: Math.round(totalLitresFilled * 100) / 100,
          net_fuel_consumption: Math.round((totalLitresUsed - totalLitresFilled) * 100) / 100,
          total_operational_hours: Math.round(totalOperationalHours * 100) / 100,
          continuous_operations_count: continuousOperationsSites.length,
          total_operational_cost: Math.round(totalOperationalCost * 100) / 100,
          sites_with_fuel_fills: sitesWithFills.length,
          total_fuel_fill_events: combinedFills.length
        },
        
        // Current fleet status
        fleet_status: {
          total_fleet_size: totalFleetSize,
          currently_active: currentlyActive,
          fleet_utilization_percentage: Math.round(fleetUtilization * 100) / 100,
          inactive_vehicles: totalFleetSize - currentlyActive
        },
        
        // Continuous operations details
        continuous_operations: {
          sites_over_24_hours: continuousOperationsSites,
          count: continuousOperationsSites.length,
          total_hours: continuousOperationsSites.reduce((sum, co) => sum + co.total_hours, 0),
          total_fuel: continuousOperationsSites.reduce((sum, co) => sum + co.fuel_usage, 0)
        },
        
        // Fuel tracking analysis
        fuel_tracking: {
          fuel_fills_summary: {
            total_fill_events: combinedFills.length,
            total_litres_filled: Math.round(totalLitresFilled * 100) / 100,
            sites_with_fills: sitesWithFills.length,
            average_fill_amount: combinedFills.length > 0 ? 
              Math.round((totalLitresFilled / combinedFills.length) * 100) / 100 : 0
          },
          fuel_efficiency: {
            total_used: Math.round(totalLitresUsed * 100) / 100,
            total_filled: Math.round(totalLitresFilled * 100) / 100,
            net_consumption: Math.round((totalLitresUsed - totalLitresFilled) * 100) / 100,
            usage_to_fill_ratio: totalLitresFilled > 0 ? 
              Math.round((totalLitresUsed / totalLitresFilled) * 100) / 100 : 0,
            fill_frequency: totalSites > 0 ? 
              Math.round((combinedFills.length / totalSites) * 100) / 100 : 0
          }
        },
        
        // Site performance breakdown
        site_performance: siteBreakdown,
        
        // Top 10 sites by fuel usage
        top_performing_sites: siteBreakdown
          .filter(site => site.fuel_usage_liters > 0)
          .sort((a, b) => b.fuel_usage_liters - a.fuel_usage_liters)
          .slice(0, 10)
          .map(site => ({
            site: site.site_name,
            cost_code: site.cost_code,
            sessions: site.total_sessions,
            operating_hours: site.operating_hours,
            fuel_usage: site.fuel_usage_liters,
            fuel_filled: site.fuel_filled_liters,
            net_fuel_usage: site.fuel_net_usage,
            total_cost: site.operational_cost,
            efficiency: site.efficiency_liters_per_hour,
            cost_per_hour: site.cost_per_hour
          })),
        
        // Cost center analysis
        cost_center_analysis: costCenterSummary,
        
        // Efficiency metrics
        efficiency_metrics: {
          average_fuel_per_hour: totalOperationalHours > 0 ? 
            Math.round((totalLitresUsed / totalOperationalHours) * 100) / 100 : 0,
          average_cost_per_hour: totalOperationalHours > 0 ? 
            Math.round((totalOperationalCost / totalOperationalHours) * 100) / 100 : 0,
          average_hours_per_site: totalSites > 0 ? 
            Math.round((totalOperationalHours / totalSites) * 100) / 100 : 0,
          average_fuel_per_site: totalSites > 0 ? 
            Math.round((totalLitresUsed / totalSites) * 100) / 100 : 0
        },
        
        // Insights for executives
        executive_insights: [
          `${totalSites} sites operated over ${period} days with ${Math.round(totalOperationalHours)} total hours`,
          `${Math.round(totalLitresUsed)}L fuel consumed (cumulative over ${period} days)`,
          `${Math.round(totalLitresFilled)}L fuel filled across ${combinedFills.length} fill events`,
          `Net consumption: ${Math.round(totalLitresUsed - totalLitresFilled)}L (used minus filled)`,
          `${continuousOperationsSites.length} sites running continuous operations (24+ hours)`,
          `Fleet utilization: ${Math.round(fleetUtilization)}% (${currentlyActive}/${totalFleetSize} active)`,
          `Average efficiency: ${totalOperationalHours > 0 ? Math.round((totalLitresUsed / totalOperationalHours) * 10) / 10 : 0}L per hour`
        ]
      };
      
      res.status(200).json({
        success: true,
        data: dashboard,
        timestamp: new Date().toISOString(),
        note: `Metrics are cumulative over ${period} days (${startDateStr} to ${targetDate}). Includes fuel usage and fuel fill tracking.`
      });
      
    } catch (error) {
      console.error('Error generating enhanced executive dashboard:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

/**
 * Detect sites running continuous operations (over 24 hours)
 * This checks for operational patterns that suggest continuous running
 */
async function detectContinuousOperations(targetDate, costCode = null, costCodes = null) {
  try {
    // Month-to-date: start from 1st of current month
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const endDate = yesterday.toISOString().split('T')[0];
    
    const today = new Date();
    const startDate = new Date(today.getFullYear(), today.getMonth(), 1);
    const startDateStr = startDate.toISOString().split('T')[0];
    
    let sessionsQuery = supabase
      .from('energy_rite_operating_sessions')
      .select('branch, cost_code, session_date, operating_hours, total_usage')
      .gte('session_date', startDateStr)
      .lte('session_date', endDate)
      .eq('session_status', 'COMPLETED')
      .gt('operating_hours', 12);
      
    // Apply cost code filtering
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
      
      sessionsQuery = sessionsQuery.in('cost_code', accessibleCostCodes);
    }
    
    const { data: sessions, error } = await sessionsQuery;
    if (error) throw error;
    
    // Return individual sessions, not grouped
    const continuousOperationsSites = sessions.map(session => ({
      site: session.branch,
      cost_code: session.cost_code,
      session_date: session.session_date,
      total_hours: Math.round(parseFloat(session.operating_hours) * 100) / 100,
      fuel_usage: Math.round(parseFloat(session.total_usage || 0) * 100) / 100,
      max_continuous_streak: Math.round(parseFloat(session.operating_hours) * 100) / 100,
      sessions_today: 1,
      pattern: 'Long continuous run'
    })).sort((a, b) => b.total_hours - a.total_hours);
    
    return continuousOperationsSites;
    
  } catch (error) {
    console.error('Error detecting continuous operations:', error);
    return [];
  }
}

/**
 * Calculate time difference between two sessions in hours
 */
function calculateTimeDifference(session1, session2) {
  try {
    const end1 = new Date(`${session1.session_date}T${session1.session_end_time}`);
    const start2 = new Date(`${session2.session_date}T${session2.session_start_time}`);
    
    const diffMs = start2.getTime() - end1.getTime();
    return diffMs / (1000 * 60 * 60); // Convert to hours
  } catch (error) {
    return 24; // If error, assume they're far apart
  }
}

module.exports = new EnhancedExecutiveDashboardController();