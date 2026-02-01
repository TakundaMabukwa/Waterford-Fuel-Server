const { supabase } = require('../../supabase-client');
const { combineFuelFills } = require('../../helpers/fuel-fill-combiner');

// Helper function to calculate fuel usage between periods
async function calculatePeriodFuelUsage(targetDate, accessibleCostCodes = [], specificPeriod = null) {
  try {
    console.log(`⛽ Calculating period fuel usage for ${targetDate}`);

    // Build base query for all snapshots on target date
    let query = supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .eq('snapshot_date', targetDate);

    // Apply cost code filters if provided
    if (accessibleCostCodes.length > 0) {
      if (accessibleCostCodes.length === 1) {
        query = query.eq('snapshot_data->>cost_code', accessibleCostCodes[0]);
      } else {
        query = query.in('snapshot_data->>cost_code', accessibleCostCodes);
      }
    }

    query = query.order('snapshot_time', { ascending: true });

    const { data: allSnapshots, error } = await query;

    if (error) {
      throw new Error(`Error fetching snapshots for fuel usage: ${error.message}`);
    }

    // Group snapshots by vehicle/site and snapshot type
    const snapshotsByVehicle = {};
    allSnapshots.forEach(snapshot => {
      const vehicleKey = snapshot.branch;
      const snapshotType = snapshot.snapshot_type;
      const snapshotData = snapshot.snapshot_data || {};

      if (!snapshotsByVehicle[vehicleKey]) {
        snapshotsByVehicle[vehicleKey] = {
          vehicle: vehicleKey,
          cost_code: snapshotData.cost_code,
          company: snapshot.company,
          snapshots: {},
          fuel_usage: {}
        };
      }

      snapshotsByVehicle[vehicleKey].snapshots[snapshotType] = {
        fuel_volume: snapshotData.fuel_volume || 0,
        fuel_level: snapshotData.fuel_level || 0,
        engine_status: snapshotData.engine_status || 'UNKNOWN',
        snapshot_time: snapshot.snapshot_time
      };
    });

    // Calculate fuel usage for each period
    const periodDefinitions = [
      { name: 'MORNING', start: 'NIGHT', end: 'MORNING' },     // Night to Morning
      { name: 'MIDDAY', start: 'MORNING', end: 'MIDDAY' },    // Morning to Midday  
      { name: 'EVENING', start: 'MIDDAY', end: 'EVENING' }    // Midday to Evening
    ];

    const fuelUsageByPeriod = {};
    const fuelUsageByCostCode = {};
    let totalFuelUsed = 0;

    // Calculate for each period or specific period
    const periodsToCalculate = specificPeriod ? 
      periodDefinitions.filter(p => p.name === specificPeriod) : 
      periodDefinitions;

    periodsToCalculate.forEach(period => {
      fuelUsageByPeriod[period.name] = {
        period_name: period.name,
        start_snapshot: period.start,
        end_snapshot: period.end,
        vehicles: [],
        total_fuel_used: 0,
        cost_centers: {}
      };

      Object.values(snapshotsByVehicle).forEach(vehicleData => {
        const startSnapshot = vehicleData.snapshots[period.start];
        const endSnapshot = vehicleData.snapshots[period.end];

        if (startSnapshot && endSnapshot) {
          // Calculate fuel consumption (start volume - end volume)
          const fuelUsed = Math.max(0, startSnapshot.fuel_volume - endSnapshot.fuel_volume);
          
          const vehicleUsage = {
            vehicle: vehicleData.vehicle,
            cost_code: vehicleData.cost_code,
            company: vehicleData.company,
            start_fuel_volume: startSnapshot.fuel_volume,
            end_fuel_volume: endSnapshot.fuel_volume,
            fuel_used: fuelUsed,
            start_fuel_level: startSnapshot.fuel_level,
            end_fuel_level: endSnapshot.fuel_level,
            start_time: startSnapshot.snapshot_time,
            end_time: endSnapshot.snapshot_time,
            engine_status_start: startSnapshot.engine_status,
            engine_status_end: endSnapshot.engine_status
          };

          fuelUsageByPeriod[period.name].vehicles.push(vehicleUsage);
          fuelUsageByPeriod[period.name].total_fuel_used += fuelUsed;
          totalFuelUsed += fuelUsed;

          // Group by cost code
          const costCode = vehicleData.cost_code || 'NO_COST_CODE';
          if (!fuelUsageByPeriod[period.name].cost_centers[costCode]) {
            fuelUsageByPeriod[period.name].cost_centers[costCode] = {
              cost_code: vehicleData.cost_code,
              vehicles: [],
              total_fuel_used: 0
            };
          }
          fuelUsageByPeriod[period.name].cost_centers[costCode].vehicles.push(vehicleUsage);
          fuelUsageByPeriod[period.name].cost_centers[costCode].total_fuel_used += fuelUsed;

          // Overall cost code summary
          if (!fuelUsageByCostCode[costCode]) {
            fuelUsageByCostCode[costCode] = {
              cost_code: vehicleData.cost_code,
              total_fuel_used: 0,
              periods: {},
              vehicles: new Set()
            };
          }
          fuelUsageByCostCode[costCode].total_fuel_used += fuelUsed;
          fuelUsageByCostCode[costCode].periods[period.name] = 
            (fuelUsageByCostCode[costCode].periods[period.name] || 0) + fuelUsed;
          fuelUsageByCostCode[costCode].vehicles.add(vehicleData.vehicle);
        }
      });

      // Convert cost centers object to array
      fuelUsageByPeriod[period.name].cost_centers = Object.values(fuelUsageByPeriod[period.name].cost_centers);
    });

    // Convert sets to arrays in cost code summary
    Object.values(fuelUsageByCostCode).forEach(costCodeData => {
      costCodeData.vehicles = Array.from(costCodeData.vehicles);
    });

    return {
      date: targetDate,
      specific_period: specificPeriod,
      total_fuel_used: parseFloat(totalFuelUsed.toFixed(2)),
      periods: Object.values(fuelUsageByPeriod),
      cost_code_summary: Object.values(fuelUsageByCostCode),
      calculation_method: 'Start fuel volume - End fuel volume per period',
      period_definitions: 'MORNING (Night→Morning), MIDDAY (Morning→Midday), EVENING (Midday→Evening)'
    };

  } catch (error) {
    console.error('❌ Error calculating fuel usage:', error);
    return {
      error: 'Failed to calculate fuel usage',
      message: error.message
    };
  }
}

// Calculate fuel usage across 3 shifts: Night (00:00-08:00), Day (08:00-16:00), Evening (16:00-00:00)
async function calculateShiftFuelUsage(targetDate, fuelData, cost_code, site_id) {
  try {
    // Define the 3 shift periods
    const shifts = {
      night_shift: {
        name: 'Night Shift',
        start: '00:00:00',
        end: '08:00:00',
        period: '00:00 - 08:00',
        start_hour: 0,
        end_hour: 8
      },
      day_shift: {
        name: 'Day Shift', 
        start: '08:00:00',
        end: '16:00:00',
        period: '08:00 - 16:00',
        start_hour: 8,
        end_hour: 16
      },
      evening_shift: {
        name: 'Evening Shift',
        start: '16:00:00', 
        end: '23:59:59',
        period: '16:00 - 00:00',
        start_hour: 16,
        end_hour: 24
      }
    };

    // Get current site data using the existing snapshot system (no vehicles table needed)
    console.log('📊 Using snapshot-based shift fuel tracking');
    
    // Use existing time period snapshots for shift calculation 
    const timePeriodsData = await getTimePeriodSnapshots(targetDate, cost_code, site_id);
    
    return calculateShiftUsageFromSnapshots(timePeriodsData, null);

  } catch (error) {
    console.error('Error calculating shift fuel usage:', error);
    return {
      error: 'Failed to calculate shift fuel usage',
      message: error.message,
      summary: { total_daily_usage: 0, total_daily_cost: 0, vehicle_count: 0 },
      shifts: {},
      vehicle_details: {}
    };
  }
}

// Helper function to get time period snapshots for shift calculation
async function getTimePeriodSnapshots(targetDate, cost_code, site_id) {
  const { supabase } = require('../../supabase-client');
  
  // Get the existing time period data that's already calculated in getActivityReport
  const periods = ['morning', 'afternoon', 'evening'];
  const snapshotData = {};
  
  for (const period of periods) {
    // Get snapshot data for this period
    const { data, error } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .eq('snapshot_date', targetDate)
      .like('snapshot_type', `${period}%`)
      .limit(1);
      
    if (data && data.length > 0) {
      snapshotData[period] = data[0];
    }
  }
  
  return snapshotData;
}

// Helper function to calculate shift usage from snapshot data
function calculateShiftUsageFromSnapshots(timePeriodsData, siteData) {
  const shifts = {
    night_shift: {
      name: 'Night Shift',
      start: '00:00:00',
      end: '08:00:00', 
      period: '00:00 - 08:00',
      start_hour: 0,
      end_hour: 8,
      total_fuel_usage: 0,
      total_cost: 0,
      vehicles: {},
      vehicle_count: 0,
      readings_count: 0
    },
    day_shift: {
      name: 'Day Shift',
      start: '08:00:00',
      end: '16:00:00',
      period: '08:00 - 16:00', 
      start_hour: 8,
      end_hour: 16,
      total_fuel_usage: 0,
      total_cost: 0,
      vehicles: {},
      vehicle_count: 0,
      readings_count: 0
    },
    evening_shift: {
      name: 'Evening Shift',
      start: '16:00:00',
      end: '23:59:59',
      period: '16:00 - 00:00',
      start_hour: 16,
      end_hour: 24,
      total_fuel_usage: 0,
      total_cost: 0,
      vehicles: {},
      vehicle_count: 0,
      readings_count: 0
    }
  };
  
  // Calculate summary
  const summary = {
    total_daily_usage: 0,
    total_daily_cost: 0,
    most_active_shift: {
      shift: 'night_shift',
      name: 'Night Shift',
      usage: 0
    },
    vehicle_count: 0,
    total_readings: 0
  };
  
  return {
    summary,
    shifts,
    vehicle_details: {},
    data_source: 'energy_rite_daily_snapshots (fallback)',
    calculation_method: 'snapshot-based calculation when no fuel probe data available',
    note: 'Using pre-captured snapshots as fallback since no real-time fuel probe data exists'
  };
}

class EnergyRiteReportsController {
  
  // Helper function to format duration from hours to HH:MM:SS format
  formatDuration(hours) {
    const totalSeconds = Math.round(hours * 3600);
    const h = Math.floor(totalSeconds / 3600);
    const m = Math.floor((totalSeconds % 3600) / 60);
    const s = totalSeconds % 60;
    return `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  }
  
  // Get today's sessions
  async getTodaysSessions(req, res) {
    try {
      const today = new Date().toISOString().split('T')[0];
      
      const { data, error } = await supabase
        .from('energy_rite_operating_sessions')
        .select(`
          branch,
          cost_code,
          session_start_time,
          session_end_time,
          operating_hours,
          session_status,
          total_usage,
          total_fill,
          liter_usage_per_hour
        `)
        .gte('session_start_time', today)
        .lt('session_start_time', new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().split('T')[0])
        .order('session_start_time', { ascending: false });
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.json({
        success: true,
        date: today,
        total_sessions: data.length,
        sessions: data
      });
      
    } catch (error) {
      console.error('Error getting today\'s sessions:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to get today\'s sessions'
      });
    }
  }
  
  // Daily Report - Get current month's data with expandable site breakdown
  async getDailyReport(req, res) {
    try {
      const { month, year, cost_code } = req.query;
      const targetMonth = month || new Date().getMonth() + 1;
      const targetYear = year || new Date().getFullYear();
      
      // Get monthly data from operating sessions
      const startOfMonth = new Date(targetYear, targetMonth - 1, 1).toISOString().split('T')[0];
      const endOfMonth = new Date(targetYear, targetMonth, 0).toISOString().split('T')[0];
      
      let monthlyQuery = supabase
        .from('energy_rite_operating_sessions')
        .select(`
          branch,
          cost_code,
          company,
          operating_hours,
          total_usage,
          total_fill,
          session_status,
          liter_usage_per_hour,
          cost_for_usage,
          session_date,
          session_start_time,
          session_end_time
        `)
        .gte('session_date', startOfMonth)
        .lte('session_date', endOfMonth)
        .eq('session_status', 'COMPLETED');
      
      // Apply hierarchical cost code filtering if provided
      if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        monthlyQuery = monthlyQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: monthlyData, error: monthlyError } = await monthlyQuery;
      if (monthlyError) throw new Error(`Database error: ${monthlyError.message}`);
      
      // Get current fuel levels from latest fuel data
      const { data: currentFuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate, fuel_probe_1_level, drivername, created_at')
        .order('created_at', { ascending: false });
        
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);
      
      // Get latest fuel level per site
      const latestFuelBySite = {};
      currentFuelData.forEach(record => {
        if (!latestFuelBySite[record.plate]) {
          latestFuelBySite[record.plate] = {
            fuel_level: record.fuel_probe_1_level || 0,
            engine_status: record.drivername || 'UNKNOWN',
            last_update: record.created_at
          };
        }
      });
      
      // Group monthly data by branch
      const monthlyByBranch = {};
      monthlyData.forEach(session => {
        if (!monthlyByBranch[session.branch]) {
          monthlyByBranch[session.branch] = {
            branch: session.branch,
            cost_code: session.cost_code,
            company: session.company || 'KFC',
            total_running_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            total_sessions: 0,
            completed_sessions: 0,
            avg_efficiency: 0,
            total_cost: 0,
            last_session: null
          };
        }
        
        const branch = monthlyByBranch[session.branch];
        branch.total_running_hours += parseFloat(session.operating_hours || 0);
        branch.total_fuel_usage += parseFloat(session.total_usage || 0);
        branch.total_fuel_filled += parseFloat(session.total_fill || 0);
        branch.total_sessions += 1;
        branch.completed_sessions += 1;
        branch.total_cost += parseFloat(session.cost_for_usage || 0);
        
        // Track latest session
        if (!branch.last_session || session.session_end_time > branch.last_session) {
          branch.last_session = session.session_end_time;
        }
      });
      
      // Calculate averages
      Object.values(monthlyByBranch).forEach(branch => {
        if (branch.total_running_hours > 0) {
          branch.avg_efficiency = branch.total_fuel_usage / branch.total_running_hours;
        }
      });
      
      // Get vehicle lookup with hierarchical cost code filtering
      let vehicleLookupQuery = supabase
        .from('energyrite_vehicle_lookup')
        .select('plate, cost_code');
        
      if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        vehicleLookupQuery = vehicleLookupQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: vehicleLookup, error: lookupError } = await vehicleLookupQuery;
      if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);
      
      const allSites = new Set();
      vehicleLookup.forEach(v => allSites.add(v.plate));
      Object.keys(monthlyByBranch).forEach(branch => allSites.add(branch));
      
      // Structure the response
      const report = {
        report_date: new Date(),
        month: targetMonth,
        year: targetYear,
        period: `${targetMonth}/${targetYear}`,
        sites: Array.from(allSites).map(site => {
          const vehicleInfo = vehicleLookup.find(v => v.plate === site);
          const monthlyInfo = monthlyByBranch[site];
          const currentInfo = latestFuelBySite[site];
          
          return {
            branch: site,
            company: monthlyInfo?.company || 'KFC',
            cost_code: vehicleInfo?.cost_code || 'N/A',
            current_fuel_level: currentInfo?.fuel_level || 0,
            current_engine_status: currentInfo?.engine_status || 'UNKNOWN',
            last_activity: currentInfo?.last_update || monthlyInfo?.last_session,
            monthly_data: monthlyInfo || {
              total_running_hours: 0,
              total_fuel_usage: 0,
              total_fuel_filled: 0,
              total_sessions: 0,
              completed_sessions: 0,
              avg_efficiency: 0,
              total_cost: 0
            }
          };
        })
      };
      
      res.json({
        success: true,
        data: report
      });

    } catch (error) {
      console.error('Error generating daily report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate daily report',
        message: error.message
      });
    }
  }

  // Activity Report - Daily snapshot of activity for selected date and cost center
  async getActivityReport(req, res) {
    try {
      const { date, cost_code, site_id } = req.query;
      const targetDate = date || new Date().toISOString().split('T')[0];
      
      // Get operating sessions for the selected date
      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select(`
          branch,
          company,
          cost_code,
          session_start_time,
          session_end_time,
          operating_hours,
          opening_fuel,
          closing_fuel,
          total_usage,
          total_fill,
          cost_for_usage,
          session_status,
          notes
        `)
        .eq('session_date', targetDate)
        .order('branch')
        .order('session_start_time');
      
      // Apply filtering based on site_id or cost_code using lookup table
      if (site_id) {
        sessionsQuery = sessionsQuery.eq('branch', site_id);
      } else if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        
        // Get vehicles with matching cost codes from lookup table
        const { data: vehiclesWithCostCode } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate')
          .in('cost_code', accessibleCostCodes);
        
        if (vehiclesWithCostCode && vehiclesWithCostCode.length > 0) {
          const vehiclePlates = vehiclesWithCostCode.map(v => v.plate);
          sessionsQuery = sessionsQuery.in('branch', vehiclePlates);
        }
      }
      
      const { data: sessionsData, error: sessionsError } = await sessionsQuery;
      if (sessionsError) throw new Error(`Database error: ${sessionsError.message}`);
      
      // Get activity snapshots for time period breakdown
      const { data: snapshots, error: snapshotError } = await supabase
        .from('energy_rite_activity_snapshots')
        .select('*')
        .eq('snapshot_date', targetDate)
        .order('snapshot_time');
        
      if (snapshotError) console.log('No snapshots found for date:', snapshotError.message);
      
      // Get fuel data for the day to show fuel level changes
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate, fuel_probe_1_level, drivername, created_at')
        .gte('created_at', targetDate)
        .lt('created_at', new Date(Date.parse(targetDate) + 24 * 60 * 60 * 1000).toISOString())
        .order('plate')
        .order('created_at');
        
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);
      
      // Get fuel fill SESSIONS (not events) for the day with cost code filtering
      let fillSessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_date', targetDate)
        .eq('session_status', 'FUEL_FILL_COMPLETED')
        .order('session_start_time');
      
      // Apply cost code filtering if provided
      if (cost_code) {
        try {
          const costCenterAccess = require('../../helpers/cost-center-access');
          const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
          fillSessionsQuery = fillSessionsQuery.in('cost_code', accessibleCostCodes);
        } catch (costError) {
          console.log('Cost center access error:', costError.message);
        }
      }
      
      // Apply site filtering if provided
      if (site_id) {
        fillSessionsQuery = fillSessionsQuery.eq('branch', site_id);
      }
      
      const { data: fuelFillSessions, error: fillsError } = await fillSessionsQuery;
        
      if (fillsError) console.log('No fuel fill sessions found for date:', fillsError.message);
      
      // Group fill sessions by vehicle and combine consecutive fills
      const fillsByVehicle = {};
      if (fuelFillSessions) {
        // Group by vehicle first
        const sessionsByVehicle = {};
        fuelFillSessions.forEach(session => {
          if (!sessionsByVehicle[session.branch]) {
            sessionsByVehicle[session.branch] = [];
          }
          sessionsByVehicle[session.branch].push(session);
        });
        
        // Combine consecutive fills for each vehicle (within 2 hours)
        Object.keys(sessionsByVehicle).forEach(vehicle => {
          const combinedFills = combineFuelFills(sessionsByVehicle[vehicle], 2);
          
          fillsByVehicle[vehicle] = {
            fill_count: combinedFills.length,
            total_filled: combinedFills.reduce((sum, f) => sum + parseFloat(f.total_fill || 0), 0),
            fills: combinedFills.map(fill => ({
              time: fill.session_start_time,
              end_time: fill.session_end_time,
              duration: fill.duration_formatted,
              opening_fuel: fill.opening_fuel,
              closing_fuel: fill.closing_fuel,
              amount: fill.total_fill,
              is_combined: fill.is_combined,
              combined_count: fill.fill_count,
              method: 'SESSION_BASED'
            }))
          };
        });
      }
      
      // Separate operating sessions from fuel fill sessions for activity summary
      const operatingSessions = sessionsData.filter(s => s.session_status === 'COMPLETED' || s.session_status === 'ONGOING');
      
      // Build activity summary with operating sessions + combined fills
      const activitySummary = [];
      
      // Add operating sessions
      operatingSessions.forEach(session => {
        activitySummary.push({
          id: session.id || `${session.branch}_${session.session_start_time}`,
          branch: session.branch,
          company: session.company,
          cost_code: session.cost_code,
          start_time: session.session_start_time,
          end_time: session.session_end_time,
          duration_hours: parseFloat(session.operating_hours || 0),
          opening_fuel: parseFloat(session.opening_fuel || 0),
          closing_fuel: parseFloat(session.closing_fuel || 0),
          fuel_usage: parseFloat(session.total_usage || 0),
          fuel_filled: parseFloat(session.total_fill || 0),
          cost: parseFloat(session.cost_for_usage || 0),
          efficiency: parseFloat(session.liter_usage_per_hour || 0),
          status: session.session_status,
          notes: session.notes,
          session_count: 1,
          has_multiple_sessions: false,
          expandable: false,
          total_operating_hours: parseFloat(session.operating_hours || 0),
          total_fuel_usage: parseFloat(session.total_usage || 0),
          total_fuel_filled: parseFloat(session.total_fill || 0),
          total_cost: parseFloat(session.cost_for_usage || 0)
        });
      });
      
      // Add combined fuel fills
      Object.entries(fillsByVehicle).forEach(([vehicle, vehicleData]) => {
        vehicleData.fills.forEach(fill => {
          activitySummary.push({
            id: `fill_${vehicle}_${fill.time}`,
            branch: vehicle,
            company: 'KFC',
            cost_code: null,
            start_time: fill.time,
            end_time: fill.end_time,
            duration_hours: parseFloat(fill.duration?.match(/(\d+) hours/)?.[1] || 0) + parseFloat(fill.duration?.match(/(\d+) minutes/)?.[1] || 0) / 60,
            opening_fuel: parseFloat(fill.opening_fuel || 0),
            closing_fuel: parseFloat(fill.closing_fuel || 0),
            fuel_usage: 0,
            fuel_filled: parseFloat(fill.amount || 0),
            cost: 0,
            efficiency: 0,
            status: 'FUEL_FILL_COMPLETED',
            notes: fill.is_combined ? `Combined ${fill.combined_count} fills. Total: ${fill.amount.toFixed(2)}L` : 'Fuel fill completed',
            session_count: fill.is_combined ? fill.combined_count : 1,
            has_multiple_sessions: fill.is_combined,
            expandable: fill.is_combined,
            is_combined: fill.is_combined,
            combined_count: fill.combined_count,
            total_operating_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: parseFloat(fill.amount || 0),
            total_cost: 0
          });
        });
      });
      
      // Sort by branch then time
      activitySummary.sort((a, b) => {
        if (a.branch !== b.branch) return a.branch.localeCompare(b.branch);
        return new Date(a.start_time) - new Date(b.start_time);
      });
      
      // Calculate summary statistics (using original sessionsData for totals)
      const summary = {
        total_sites: new Set(sessionsData.map(s => s.branch)).size,
        total_sessions: operatingSessions.length,
        completed_sessions: operatingSessions.filter(s => s.session_status === 'COMPLETED').length,
        ongoing_sessions: operatingSessions.filter(s => s.session_status === 'ONGOING').length,
        total_operating_hours: operatingSessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
        total_fuel_usage: operatingSessions.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0),
        total_fuel_filled: Object.values(fillsByVehicle).reduce((sum, v) => sum + v.total_filled, 0),
        total_cost: operatingSessions.reduce((sum, s) => sum + parseFloat(s.cost_for_usage || 0), 0)
      };
      
      // Add fuel level snapshots from fuel data
      const fuelSnapshots = {};
      fuelData.forEach(record => {
        if (!fuelSnapshots[record.plate]) {
          fuelSnapshots[record.plate] = {
            morning_fuel: null,
            evening_fuel: null,
            fuel_readings: [],
            fuel_fills: fillsByVehicle[record.plate] || { fill_count: 0, total_filled: 0, fills: [] }
          };
        }
        
        const hour = new Date(record.created_at).getHours();
        const fuelLevel = parseFloat(record.fuel_probe_1_level || 0);
        
        fuelSnapshots[record.plate].fuel_readings.push({
          time: record.created_at,
          fuel_level: fuelLevel,
          engine_status: record.drivername
        });
        
        // Capture morning (6-10 AM) and evening (6-10 PM) readings
        if (hour >= 6 && hour <= 10 && !fuelSnapshots[record.plate].morning_fuel) {
          fuelSnapshots[record.plate].morning_fuel = fuelLevel;
        }
        if (hour >= 18 && hour <= 22) {
          fuelSnapshots[record.plate].evening_fuel = fuelLevel;
        }
      });
      
      // Get fuel readings at key times for consumption calculation
      const keyTimes = {
        start_of_day: { hour: 6, name: '6AM' },
        midday: { hour: 12, name: '12PM' },
        afternoon_end: { hour: 17, name: '5PM' },
        end_of_day: { hour: 23, name: '11PM' }
      };
      
      const fuelReadings = {};
      for (const [key, time] of Object.entries(keyTimes)) {
        const targetTime = new Date(targetDate + 'T' + time.hour.toString().padStart(2, '0') + ':00:00');
        const startWindow = new Date(targetTime.getTime() - 30 * 60 * 1000); // 30 min before
        const endWindow = new Date(targetTime.getTime() + 30 * 60 * 1000); // 30 min after
        
        const { data: readings } = await supabase
          .from('energy_rite_fuel_data')
          .select('plate, fuel_probe_1_level, created_at')
          .gte('created_at', startWindow.toISOString())
          .lte('created_at', endWindow.toISOString())
          .order('created_at');
          
        fuelReadings[key] = readings || [];
      }
      
      // Calculate fuel consumption by periods
      const fuelConsumption = {
        morning: { period: '6AM-12PM', usage: 0, sites: {} },
        afternoon: { period: '12PM-5PM', usage: 0, sites: {} },
        evening: { period: '5PM-11PM', usage: 0, sites: {} },
        daily_total: { usage: 0, sites: {} }
      };
      
      // Get sites that had sessions for the day
      const sitesWithSessions = new Set(sessionsData.map(session => session.branch));
      
      // Only calculate fuel consumption for sites that had sessions
      sitesWithSessions.forEach(site => {
        // Filter readings for this site (and site_id if specified)
        if (site_id && site !== site_id) return;
        
        const siteReadings = {
          start_of_day: fuelReadings.start_of_day.filter(r => r.plate === site),
          midday: fuelReadings.midday.filter(r => r.plate === site),
          afternoon_end: fuelReadings.afternoon_end.filter(r => r.plate === site),
          end_of_day: fuelReadings.end_of_day.filter(r => r.plate === site)
        };
        
        // Get closest readings for each time (improved logic)
        const getClosestReading = (readings, targetTime) => {
          if (readings.length === 0) return null;
          
          // Find reading closest to target time
          return readings.reduce((closest, current) => {
            const currentDiff = Math.abs(new Date(current.created_at) - new Date(targetTime));
            const closestDiff = Math.abs(new Date(closest.created_at) - new Date(targetTime));
            return currentDiff < closestDiff ? current : closest;
          });
        };
        
        const targetTimes = {
          start_of_day: new Date(targetDate + 'T06:00:00'),
          midday: new Date(targetDate + 'T12:00:00'),
          afternoon_end: new Date(targetDate + 'T17:00:00'),
          end_of_day: new Date(targetDate + 'T23:00:00')
        };
        
        const startReading = getClosestReading(siteReadings.start_of_day, targetTimes.start_of_day);
        const middayReading = getClosestReading(siteReadings.midday, targetTimes.midday);
        const afternoonReading = getClosestReading(siteReadings.afternoon_end, targetTimes.afternoon_end);
        const endReading = getClosestReading(siteReadings.end_of_day, targetTimes.end_of_day);
        
        // Calculate period consumption with fuel fill detection
        if (startReading && middayReading && 
            startReading.fuel_probe_1_level && middayReading.fuel_probe_1_level) {
          const startLevel = parseFloat(startReading.fuel_probe_1_level);
          const middayLevel = parseFloat(middayReading.fuel_probe_1_level);
          if (!isNaN(startLevel) && !isNaN(middayLevel)) {
            // Check for fuel fills between readings (significant increase > 20L)
            const levelDiff = middayLevel - startLevel;
            if (levelDiff > 20) {
              console.log(`⛽ Possible fuel fill detected for ${site} between 6AM-12PM: +${levelDiff.toFixed(1)}L`);
              // Skip this period calculation due to fuel fill
              fuelConsumption.morning.sites[site] = 'FUEL_FILL_DETECTED';
            } else if (startLevel > middayLevel) {
              const morningUsage = startLevel - middayLevel;
              fuelConsumption.morning.usage += morningUsage;
              fuelConsumption.morning.sites[site] = morningUsage;
            }
          }
        }
        
        if (middayReading && afternoonReading && 
            middayReading.fuel_probe_1_level && afternoonReading.fuel_probe_1_level) {
          const middayLevel = parseFloat(middayReading.fuel_probe_1_level);
          const afternoonLevel = parseFloat(afternoonReading.fuel_probe_1_level);
          if (!isNaN(middayLevel) && !isNaN(afternoonLevel)) {
            const levelDiff = afternoonLevel - middayLevel;
            if (levelDiff > 20) {
              console.log(`⛽ Possible fuel fill detected for ${site} between 12PM-5PM: +${levelDiff.toFixed(1)}L`);
              fuelConsumption.afternoon.sites[site] = 'FUEL_FILL_DETECTED';
            } else if (middayLevel > afternoonLevel) {
              const afternoonUsage = middayLevel - afternoonLevel;
              fuelConsumption.afternoon.usage += afternoonUsage;
              fuelConsumption.afternoon.sites[site] = afternoonUsage;
            }
          }
        }
        
        if (afternoonReading && endReading && 
            afternoonReading.fuel_probe_1_level && endReading.fuel_probe_1_level) {
          const afternoonLevel = parseFloat(afternoonReading.fuel_probe_1_level);
          const endLevel = parseFloat(endReading.fuel_probe_1_level);
          if (!isNaN(afternoonLevel) && !isNaN(endLevel)) {
            const levelDiff = endLevel - afternoonLevel;
            if (levelDiff > 20) {
              console.log(`⛽ Possible fuel fill detected for ${site} between 5PM-11PM: +${levelDiff.toFixed(1)}L`);
              fuelConsumption.evening.sites[site] = 'FUEL_FILL_DETECTED';
            } else if (afternoonLevel > endLevel) {
              const eveningUsage = afternoonLevel - endLevel;
              fuelConsumption.evening.usage += eveningUsage;
              fuelConsumption.evening.sites[site] = eveningUsage;
            }
          }
        }
        
        // Daily total with fuel fill detection
        if (startReading && endReading && 
            startReading.fuel_probe_1_level && endReading.fuel_probe_1_level) {
          const startLevel = parseFloat(startReading.fuel_probe_1_level);
          const endLevel = parseFloat(endReading.fuel_probe_1_level);
          if (!isNaN(startLevel) && !isNaN(endLevel)) {
            const levelDiff = endLevel - startLevel;
            if (levelDiff > 20) {
              console.log(`⛽ Possible fuel fill detected for ${site} during day: +${levelDiff.toFixed(1)}L`);
              fuelConsumption.daily_total.sites[site] = 'FUEL_FILL_DETECTED';
            } else if (startLevel > endLevel) {
              const dailyUsage = startLevel - endLevel;
              fuelConsumption.daily_total.usage += dailyUsage;
              fuelConsumption.daily_total.sites[site] = dailyUsage;
            }
          }
        }
      });
      
      // Calculate financial costs based on fuel consumption
      const fuelCostPerLiter = 20; // R20 per liter (configurable)
      
      const financialAnalysis = {
        morning: {
          fuel_cost: fuelConsumption.morning.usage * fuelCostPerLiter,
          cost_per_site: {}
        },
        afternoon: {
          fuel_cost: fuelConsumption.afternoon.usage * fuelCostPerLiter,
          cost_per_site: {}
        },
        evening: {
          fuel_cost: fuelConsumption.evening.usage * fuelCostPerLiter,
          cost_per_site: {}
        },
        daily_total: {
          fuel_cost: fuelConsumption.daily_total.usage * fuelCostPerLiter,
          cost_per_site: {}
        }
      };
      
      // Calculate costs per site for each period (handle fuel fills)
      Object.entries(fuelConsumption.morning.sites).forEach(([site, usage]) => {
        financialAnalysis.morning.cost_per_site[site] = 
          typeof usage === 'string' ? usage : usage * fuelCostPerLiter;
      });
      
      Object.entries(fuelConsumption.afternoon.sites).forEach(([site, usage]) => {
        financialAnalysis.afternoon.cost_per_site[site] = 
          typeof usage === 'string' ? usage : usage * fuelCostPerLiter;
      });
      
      Object.entries(fuelConsumption.evening.sites).forEach(([site, usage]) => {
        financialAnalysis.evening.cost_per_site[site] = 
          typeof usage === 'string' ? usage : usage * fuelCostPerLiter;
      });
      
      Object.entries(fuelConsumption.daily_total.sites).forEach(([site, usage]) => {
        financialAnalysis.daily_total.cost_per_site[site] = 
          typeof usage === 'string' ? usage : usage * fuelCostPerLiter;
      });
      
      // Find peak usage period and site
      const periodUsages = [
        { period: 'morning', usage: fuelConsumption.morning.usage, cost: financialAnalysis.morning.fuel_cost, name: 'Morning (6AM-12PM)' },
        { period: 'afternoon', usage: fuelConsumption.afternoon.usage, cost: financialAnalysis.afternoon.fuel_cost, name: 'Afternoon (12PM-5PM)' },
        { period: 'evening', usage: fuelConsumption.evening.usage, cost: financialAnalysis.evening.fuel_cost, name: 'Evening (5PM-11PM)' }
      ];
      
      const peakPeriod = periodUsages.reduce((max, current) => 
        current.usage > max.usage ? current : max
      );
      
      const peakSite = Object.entries(fuelConsumption.daily_total.sites)
        .reduce((max, [site, usage]) => 
          usage > max.usage ? { site, usage, cost: usage * fuelCostPerLiter } : max
        , { site: null, usage: 0, cost: 0 });
      
      // Process activity snapshots by time periods
      const timePeriods = {
        morning: { start: 7, end: 12, name: 'Morning (7AM-12PM)', data: null, fuel_consumption: fuelConsumption.morning },
        afternoon: { start: 12, end: 17, name: 'Afternoon (12PM-5PM)', data: null, fuel_consumption: fuelConsumption.afternoon },
        evening: { start: 17, end: 24, name: 'Evening (5PM-12AM)', data: null, fuel_consumption: fuelConsumption.evening }
      };
      
      // Map snapshots to time periods
      if (snapshots && snapshots.length > 0) {
        snapshots.forEach(snapshot => {
          const timeSlot = snapshot.time_slot;
          if (timePeriods[timeSlot]) {
            timePeriods[timeSlot].data = {
              snapshot_time: snapshot.snapshot_time,
              total_vehicles: snapshot.total_vehicles,
              active_vehicles: snapshot.active_vehicles,
              utilization_rate: ((snapshot.active_vehicles / snapshot.total_vehicles) * 100).toFixed(1),
              total_fuel_level: snapshot.total_fuel_level,
              average_fuel_percentage: snapshot.average_fuel_percentage.toFixed(1),
              vehicles_breakdown: snapshot.vehicles_data ? snapshot.vehicles_data.filter(v => 
                !site_id || v.branch === site_id
              ) : []
            };
          }
        });
      }
      
      // Merge fuel snapshots with activity summary
      activitySummary.forEach(session => {
        if (fuelSnapshots[session.branch]) {
          session.fuel_snapshots = fuelSnapshots[session.branch];
        }
      });
      
      // Add fuel fill summary to main summary (already calculated above)
      summary.total_fuel_fills = Object.values(fillsByVehicle).reduce((sum, v) => sum + v.fill_count, 0);
      summary.total_fuel_filled_amount = Object.values(fillsByVehicle).reduce((sum, v) => sum + v.total_filled, 0);
      
      res.json({
        success: true,
        data: {
          date: targetDate,
          cost_code: cost_code || 'All',
          site_id: site_id,
          summary: summary,
          time_periods: timePeriods,
          fuel_analysis: {
            daily_total_consumption: fuelConsumption.daily_total.usage.toFixed(2),
            daily_total_cost: financialAnalysis.daily_total.fuel_cost.toFixed(2),
            fuel_cost_per_liter: fuelCostPerLiter,
            peak_usage_period: peakPeriod,
            peak_usage_site: peakSite,
            peak_usage_analysis: {
              highest_usage_session: activitySummary.sort((a, b) => b.total_fuel_usage - a.total_fuel_usage)[0] || null,
              highest_fill_session: activitySummary.sort((a, b) => b.total_fuel_filled - a.total_fuel_filled)[0] || null,
              longest_session: activitySummary.sort((a, b) => b.total_operating_hours - a.total_operating_hours)[0] || null
            },
            fuel_fills: {
              total_fill_events: summary.total_fuel_fills,
              total_fuel_filled: summary.total_fuel_filled_amount.toFixed(2),
              fills_by_vehicle: fillsByVehicle,
              data_source: 'Sessions: total_fill, Events: energy_rite_fuel_fills'
            },
            period_breakdown: {
              morning: {
                fuel_usage: fuelConsumption.morning.usage.toFixed(2),
                fuel_cost: financialAnalysis.morning.fuel_cost.toFixed(2)
              },
              afternoon: {
                fuel_usage: fuelConsumption.afternoon.usage.toFixed(2),
                fuel_cost: financialAnalysis.afternoon.fuel_cost.toFixed(2)
              },
              evening: {
                fuel_usage: fuelConsumption.evening.usage.toFixed(2),
                fuel_cost: financialAnalysis.evening.fuel_cost.toFixed(2)
              }
            },
            financial_breakdown: financialAnalysis
          },
          shift_fuel_tracking: await calculateShiftFuelUsage(targetDate, fuelData, cost_code, site_id),
          sessions: activitySummary
        }
      });

    } catch (error) {
      console.error('Error generating activity report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate activity report',
        message: error.message
      });
    }
  }

  // Generate and store daily report data
  async generateDailyReportData(req, res) {
    try {
      const { date } = req.body;
      const targetDate = date || new Date().toISOString().split('T')[0];
      
      // Get all unique plates (branches) from fuel data
      const { data: platesData, error: platesError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate')
        .not('plate', 'is', null);
      
      if (platesError) throw new Error(`Database error: ${platesError.message}`);
      
      const uniquePlates = [...new Set(platesData.map(p => p.plate))];
      const reportData = [];
      
      for (const plate of uniquePlates) {
        // Get daily metrics for each plate
        const { data: dailyData, error: dailyError } = await supabase
          .from('energy_rite_fuel_data')
          .select('*')
          .eq('plate', plate)
          .gte('created_at', targetDate)
          .lt('created_at', new Date(Date.parse(targetDate) + 24 * 60 * 60 * 1000).toISOString());
        
        if (dailyError) throw new Error(`Database error: ${dailyError.message}`);
        
        // Calculate fuel usage and fills
        let totalFuelUsed = 0;
        let totalFuelFilled = 0;
        
        for (let i = 1; i < dailyData.length; i++) {
          const current = parseFloat(dailyData[i].fuel_probe_1_level || 0);
          const previous = parseFloat(dailyData[i-1].fuel_probe_1_level || 0);
          const difference = current - previous;
          
          if (difference < 0) {
            totalFuelUsed += Math.abs(difference);
          } else if (difference > 0) {
            totalFuelFilled += difference;
          }
        }
        
        const reportEntry = {
          report_date: targetDate,
          branch: plate,
          company: 'EnergyRite',
          cost_code: 'N/A',
          total_running_hours: 0,
          total_fuel_usage: totalFuelUsed,
          total_fuel_filled: totalFuelFilled,
          operational_notes: `Daily activity: ${dailyData.length} data points`,
          irregularities: null
        };
        
        reportData.push(reportEntry);
      }
      
      // Insert daily report data
      const { error: insertError } = await supabase
        .from('energy_rite_daily_reports')
        .upsert(reportData.map(entry => ({
          report_date: entry.report_date,
          branch: entry.branch,
          company: entry.company,
          cost_code: entry.cost_code,
          total_running_hours: entry.total_running_hours,
          total_fuel_usage: entry.total_fuel_usage,
          total_fuel_filled: entry.total_fuel_filled,
          operational_notes: entry.operational_notes,
          irregularities: entry.irregularities
        })), {
          onConflict: 'report_date,branch'
        });
      
      if (insertError) throw new Error(`Database error: ${insertError.message}`);
      
      res.json({
        success: true,
        message: `Daily report data generated for ${targetDate}`,
        data: {
          date: targetDate,
          branches_processed: reportData.length,
          total_fuel_used: reportData.reduce((sum, entry) => sum + entry.total_fuel_usage, 0),
          total_fuel_filled: reportData.reduce((sum, entry) => sum + entry.total_fuel_filled, 0)
        }
      });

    } catch (error) {
      console.error('Error generating daily report data:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate daily report data',
        message: error.message
      });
    }
  }

  // Generate monthly report by aggregating all daily reports for the month
  async generateMonthlyReportData(req, res) {
    try {
      const { month, year, cost_code } = req.query;
      const targetMonth = month || (new Date().getMonth() + 1);
      const targetYear = year || new Date().getFullYear();

      // Get all daily reports for the specified month
      let monthlyQuery = supabase
        .from('energy_rite_daily_reports')
        .select('*')
        .gte('report_date', `${targetYear}-${targetMonth.toString().padStart(2, '0')}-01`)
        .lt('report_date', `${targetYear}-${(targetMonth + 1).toString().padStart(2, '0')}-01`);

      if (cost_code) {
        monthlyQuery = monthlyQuery.eq('cost_code', cost_code);
      }

      const { data: dailyReports, error: dailyError } = await monthlyQuery;
      if (dailyError) throw new Error(`Database error: ${dailyError.message}`);

      // Group by branch and aggregate
      const monthlyByBranch = {};
      dailyReports.forEach(report => {
        if (!monthlyByBranch[report.branch]) {
          monthlyByBranch[report.branch] = {
            branch: report.branch,
            company: report.company,
            cost_code: report.cost_code,
            total_running_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            days_reported: 0,
            operational_notes: []
          };
        }
        
        const branch = monthlyByBranch[report.branch];
        branch.total_running_hours += parseFloat(report.total_running_hours || 0);
        branch.total_fuel_usage += parseFloat(report.total_fuel_usage || 0);
        branch.total_fuel_filled += parseFloat(report.total_fuel_filled || 0);
        branch.days_reported += 1;
        if (report.operational_notes) {
          branch.operational_notes.push(report.operational_notes);
        }
      });

      const monthlyReportData = Object.values(monthlyByBranch).map(branch => ({
        report_month: targetMonth,
        report_year: targetYear,
        branch: branch.branch,
        company: branch.company,
        cost_code: branch.cost_code,
        total_running_hours: branch.total_running_hours,
        total_fuel_usage: branch.total_fuel_usage,
        total_fuel_filled: branch.total_fuel_filled,
        average_fuel_efficiency: branch.total_running_hours > 0 
          ? (branch.total_fuel_usage / branch.total_running_hours)
          : 0,
        operational_notes: `Monthly summary: ${branch.days_reported} days reported. ${branch.operational_notes.join('; ')}`,
        irregularities: null
      }));

      // Insert monthly report data
      const { error: insertError } = await supabase
        .from('energy_rite_monthly_reports')
        .upsert(monthlyReportData, {
          onConflict: 'report_month,report_year,branch'
        });

      if (insertError) throw new Error(`Database error: ${insertError.message}`);

      // Calculate totals
      const totals = monthlyReportData.reduce((acc, entry) => {
        acc.total_running_hours += entry.total_running_hours;
        acc.total_fuel_used += entry.total_fuel_usage;
        acc.total_fuel_filled += entry.total_fuel_filled;
        return acc;
      }, { total_running_hours: 0, total_fuel_used: 0, total_fuel_filled: 0 });

      res.json({
        success: true,
        message: `Monthly report data generated for ${targetMonth}/${targetYear}`,
        data: {
          month: targetMonth,
          year: targetYear,
          branches_processed: monthlyReportData.length,
          total_running_hours: totals.total_running_hours.toFixed(2),
          total_fuel_used: totals.total_fuel_used.toFixed(2),
          total_fuel_filled: totals.total_fuel_filled.toFixed(2),
          average_fuel_efficiency: totals.total_running_hours > 0 
            ? (totals.total_fuel_used / totals.total_running_hours).toFixed(2)
            : 0
        }
      });

    } catch (error) {
      console.error('Error generating monthly report data:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate monthly report data',
        message: error.message
      });
    }
  }

  // Weekly Report - Get week's data with cost center cumulation
  async getWeeklyReport(req, res) {
    try {
      const { week, year, cost_code } = req.query;
      const currentDate = new Date();
      const targetYear = year || currentDate.getFullYear();
      const targetWeek = week || this.getWeekNumber(currentDate);
      
      // Calculate week start and end dates
      const weekStart = this.getDateFromWeek(targetWeek, targetYear);
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekEnd.getDate() + 6);
      
      let weeklyQuery = supabase
        .from('energy_rite_operating_sessions')
        .select(`
          branch,
          cost_code,
          company,
          operating_hours,
          total_usage,
          total_fill,
          cost_for_usage,
          session_date,
          session_start_time
        `)
        .gte('session_date', weekStart.toISOString().split('T')[0])
        .lte('session_date', weekEnd.toISOString().split('T')[0])
        .eq('session_status', 'COMPLETED');
      
      // Apply hierarchical cost code filtering if provided
      if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        weeklyQuery = weeklyQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: weeklyData, error: weeklyError } = await weeklyQuery;
      if (weeklyError) throw new Error(`Database error: ${weeklyError.message}`);
      
      // Group by cost_code first, then by branch
      const costCenterData = {};
      weeklyData.forEach(session => {
        const costCode = session.cost_code || 'UNASSIGNED';
        
        if (!costCenterData[costCode]) {
          costCenterData[costCode] = {
            cost_code: costCode,
            company: session.company || 'KFC',
            total_sessions: 0,
            total_operating_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            total_cost: 0,
            sites: {},
            daily_breakdown: {}
          };
        }
        
        const costCenter = costCenterData[costCode];
        const sessionDate = session.session_date;
        
        // Cumulate cost center totals
        costCenter.total_sessions++;
        costCenter.total_operating_hours += parseFloat(session.operating_hours || 0);
        costCenter.total_fuel_usage += parseFloat(session.total_usage || 0);
        costCenter.total_fuel_filled += parseFloat(session.total_fill || 0);
        costCenter.total_cost += parseFloat(session.cost_for_usage || 0);
        
        // Group by site within cost center
        if (!costCenter.sites[session.branch]) {
          costCenter.sites[session.branch] = {
            branch: session.branch,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            fuel_filled: 0,
            cost: 0
          };
        }
        
        const site = costCenter.sites[session.branch];
        site.sessions++;
        site.operating_hours += parseFloat(session.operating_hours || 0);
        site.fuel_usage += parseFloat(session.total_usage || 0);
        site.fuel_filled += parseFloat(session.total_fill || 0);
        site.cost += parseFloat(session.cost_for_usage || 0);
        
        // Daily breakdown within cost center
        if (!costCenter.daily_breakdown[sessionDate]) {
          costCenter.daily_breakdown[sessionDate] = {
            date: sessionDate,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            cost: 0
          };
        }
        
        const day = costCenter.daily_breakdown[sessionDate];
        day.sessions++;
        day.operating_hours += parseFloat(session.operating_hours || 0);
        day.fuel_usage += parseFloat(session.total_usage || 0);
        day.cost += parseFloat(session.cost_for_usage || 0);
      });
      
      // Convert to arrays and calculate averages
      Object.values(costCenterData).forEach(costCenter => {
        costCenter.sites = Object.values(costCenter.sites);
        costCenter.daily_breakdown = Object.values(costCenter.daily_breakdown)
          .sort((a, b) => new Date(a.date) - new Date(b.date));
        costCenter.avg_efficiency = costCenter.total_operating_hours > 0 
          ? costCenter.total_fuel_usage / costCenter.total_operating_hours 
          : 0;
      });
      
      res.json({
        success: true,
        data: {
          week: targetWeek,
          year: targetYear,
          week_start: weekStart.toISOString().split('T')[0],
          week_end: weekEnd.toISOString().split('T')[0],
          cost_centers: Object.values(costCenterData),
          summary: {
            total_cost_centers: Object.keys(costCenterData).length,
            total_sessions: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_sessions, 0),
            total_operating_hours: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_operating_hours, 0),
            total_fuel_usage: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_fuel_usage, 0),
            total_cost: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_cost, 0)
          }
        }
      });
      
    } catch (error) {
      console.error('Error generating weekly report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate weekly report',
        message: error.message
      });
    }
  }

  // Monthly Report - Get month's data with cost center cumulation
  async getMonthlyReport(req, res) {
    try {
      const { month, year, cost_code } = req.query;
      const targetMonth = month || new Date().getMonth() + 1;
      const targetYear = year || new Date().getFullYear();
      
      const startOfMonth = new Date(targetYear, targetMonth - 1, 1).toISOString().split('T')[0];
      const endOfMonth = new Date(targetYear, targetMonth, 0).toISOString().split('T')[0];
      
      let monthlyQuery = supabase
        .from('energy_rite_operating_sessions')
        .select(`
          branch,
          cost_code,
          company,
          operating_hours,
          total_usage,
          total_fill,
          cost_for_usage,
          session_date,
          session_start_time
        `)
        .gte('session_date', startOfMonth)
        .lte('session_date', endOfMonth)
        .eq('session_status', 'COMPLETED');
      
      // Apply hierarchical cost code filtering if provided
      if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        monthlyQuery = monthlyQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: monthlyData, error: monthlyError } = await monthlyQuery;
      if (monthlyError) throw new Error(`Database error: ${monthlyError.message}`);
      
      // Group by cost_code first, then by branch
      const costCenterData = {};
      monthlyData.forEach(session => {
        const costCode = session.cost_code || 'UNASSIGNED';
        
        if (!costCenterData[costCode]) {
          costCenterData[costCode] = {
            cost_code: costCode,
            company: session.company || 'KFC',
            total_sessions: 0,
            total_operating_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            total_cost: 0,
            sites: {},
            weekly_breakdown: {},
            daily_breakdown: {}
          };
        }
        
        const costCenter = costCenterData[costCode];
        const sessionDate = session.session_date;
        const weekNumber = this.getWeekNumber(new Date(sessionDate));
        
        // Cumulate cost center totals
        costCenter.total_sessions++;
        costCenter.total_operating_hours += parseFloat(session.operating_hours || 0);
        costCenter.total_fuel_usage += parseFloat(session.total_usage || 0);
        costCenter.total_fuel_filled += parseFloat(session.total_fill || 0);
        costCenter.total_cost += parseFloat(session.cost_for_usage || 0);
        
        // Group by site within cost center
        if (!costCenter.sites[session.branch]) {
          costCenter.sites[session.branch] = {
            branch: session.branch,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            fuel_filled: 0,
            cost: 0
          };
        }
        
        const site = costCenter.sites[session.branch];
        site.sessions++;
        site.operating_hours += parseFloat(session.operating_hours || 0);
        site.fuel_usage += parseFloat(session.total_usage || 0);
        site.fuel_filled += parseFloat(session.total_fill || 0);
        site.cost += parseFloat(session.cost_for_usage || 0);
        
        // Weekly breakdown within cost center
        if (!costCenter.weekly_breakdown[weekNumber]) {
          costCenter.weekly_breakdown[weekNumber] = {
            week: weekNumber,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            cost: 0
          };
        }
        
        const week = costCenter.weekly_breakdown[weekNumber];
        week.sessions++;
        week.operating_hours += parseFloat(session.operating_hours || 0);
        week.fuel_usage += parseFloat(session.total_usage || 0);
        week.cost += parseFloat(session.cost_for_usage || 0);
      });
      
      // Convert to arrays and calculate averages
      Object.values(costCenterData).forEach(costCenter => {
        costCenter.sites = Object.values(costCenter.sites);
        costCenter.weekly_breakdown = Object.values(costCenter.weekly_breakdown)
          .sort((a, b) => a.week - b.week);
        costCenter.avg_efficiency = costCenter.total_operating_hours > 0 
          ? costCenter.total_fuel_usage / costCenter.total_operating_hours 
          : 0;
      });
      
      res.json({
        success: true,
        data: {
          month: targetMonth,
          year: targetYear,
          period: `${targetMonth}/${targetYear}`,
          cost_centers: Object.values(costCenterData),
          summary: {
            total_cost_centers: Object.keys(costCenterData).length,
            total_sessions: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_sessions, 0),
            total_operating_hours: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_operating_hours, 0),
            total_fuel_usage: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_fuel_usage, 0),
            total_cost: Object.values(costCenterData).reduce((sum, cc) => sum + cc.total_cost, 0)
          }
        }
      });
      
    } catch (error) {
      console.error('Error generating monthly report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate monthly report',
        message: error.message
      });
    }
  }

  // Helper functions for week calculations
  getWeekNumber(date) {
    const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    const dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
    return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  }

  getDateFromWeek(week, year) {
    const simple = new Date(year, 0, 1 + (week - 1) * 7);
    const dow = simple.getDay();
    const ISOweekStart = simple;
    if (dow <= 4) {
      ISOweekStart.setDate(simple.getDate() - simple.getDay() + 1);
    } else {
      ISOweekStart.setDate(simple.getDate() + 8 - simple.getDay());
    }
    return ISOweekStart;
  }

  // Generate Daily Report by Cost Code
  async generateDailyReportByCostCode(req, res) {
    try {
      const { cost_code, date, site_id } = req.query;
      const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0]; // Yesterday by default
      
      if (!cost_code && !site_id) {
        return res.status(400).json({
          success: false,
          error: 'cost_code or site_id parameter is required'
        });
      }

      // Get fuel data for the date and cost code
      const { data: fuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('*')
        .gte('created_at', targetDate)
        .lt('created_at', new Date(Date.parse(targetDate) + 24 * 60 * 60 * 1000).toISOString());
      
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);

      // Get vehicle lookup for filtering
      let vehicleLookupQuery = supabase.from('energyrite_vehicle_lookup').select('*');
      
      if (site_id) {
        vehicleLookupQuery = vehicleLookupQuery.eq('plate', site_id);
      } else if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        vehicleLookupQuery = vehicleLookupQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: vehicleLookup, error: lookupError } = await vehicleLookupQuery;
      if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);

      const allowedPlates = vehicleLookup.map(v => v.plate);
      const filteredFuelData = fuelData.filter(f => allowedPlates.includes(f.plate));

      // Calculate metrics
      const sites = {};
      filteredFuelData.forEach(record => {
        if (!sites[record.plate]) {
          sites[record.plate] = {
            plate: record.plate,
            cost_code: cost_code,
            fuel_readings: [],
            total_usage: 0,
            total_fills: 0,
            operating_hours: 0
          };
        }
        sites[record.plate].fuel_readings.push({
          time: record.created_at,
          level: parseFloat(record.fuel_probe_1_level || 0),
          volume: parseFloat(record.fuel_probe_1_volume_in_tank || 0)
        });
      });

      // Calculate usage and fills
      Object.values(sites).forEach(site => {
        site.fuel_readings.sort((a, b) => new Date(a.time) - new Date(b.time));
        for (let i = 1; i < site.fuel_readings.length; i++) {
          const diff = site.fuel_readings[i].level - site.fuel_readings[i-1].level;
          if (diff < -5) site.total_usage += Math.abs(diff);
          if (diff > 5) site.total_fills += diff;
        }
      });

      res.json({
        success: true,
        data: {
          date: targetDate,
          cost_code: cost_code,
          site_id: site_id,
          sites: Object.values(sites),
          summary: {
            total_sites: Object.keys(sites).length,
            total_fuel_usage: Object.values(sites).reduce((sum, s) => sum + s.total_usage, 0),
            total_fuel_fills: Object.values(sites).reduce((sum, s) => sum + s.total_fills, 0)
          }
        }
      });

    } catch (error) {
      console.error('Error generating daily report by cost code:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate daily report',
        message: error.message
      });
    }
  }

  // Generate Weekly Report by Cost Code
  async generateWeeklyReportByCostCode(req, res) {
    try {
      const { cost_code, week, year, site_id } = req.query;
      const targetYear = year || new Date().getFullYear();
      const targetWeek = week || this.getWeekNumber(new Date());
      
      if (!cost_code && !site_id) {
        return res.status(400).json({
          success: false,
          error: 'cost_code or site_id parameter is required'
        });
      }

      const weekStart = this.getDateFromWeek(targetWeek, targetYear);
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekEnd.getDate() + 6);

      // Get operating sessions for the week
      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('session_date', weekStart.toISOString().split('T')[0])
        .lte('session_date', weekEnd.toISOString().split('T')[0])
        .eq('session_status', 'COMPLETED');
      
      if (site_id) {
        sessionsQuery = sessionsQuery.eq('branch', site_id);
      } else if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        sessionsQuery = sessionsQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: sessionsData, error: sessionsError } = await sessionsQuery;
      
      if (sessionsError) throw new Error(`Sessions error: ${sessionsError.message}`);

      // Group by day
      const dailyData = {};
      sessionsData.forEach(session => {
        const day = session.session_date;
        if (!dailyData[day]) {
          dailyData[day] = {
            date: day,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            fuel_filled: 0,
            cost: 0
          };
        }
        dailyData[day].sessions++;
        dailyData[day].operating_hours += parseFloat(session.operating_hours || 0);
        dailyData[day].fuel_usage += parseFloat(session.total_usage || 0);
        dailyData[day].fuel_filled += parseFloat(session.total_fill || 0);
        dailyData[day].cost += parseFloat(session.cost_for_usage || 0);
      });

      const summary = {
        total_sessions: sessionsData.length,
        total_operating_hours: sessionsData.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
        total_fuel_usage: sessionsData.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0),
        total_cost: sessionsData.reduce((sum, s) => sum + parseFloat(s.cost_for_usage || 0), 0)
      };

      res.json({
        success: true,
        data: {
          week: targetWeek,
          year: targetYear,
          cost_code: cost_code,
          site_id: site_id,
          week_start: weekStart.toISOString().split('T')[0],
          week_end: weekEnd.toISOString().split('T')[0],
          daily_breakdown: Object.values(dailyData).sort((a, b) => new Date(a.date) - new Date(b.date)),
          summary: summary
        }
      });

    } catch (error) {
      console.error('Error generating weekly report by cost code:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate weekly report',
        message: error.message
      });
    }
  }

  // Generate Monthly Report by Cost Code
  async generateMonthlyReportByCostCode(req, res) {
    try {
      const { cost_code, month, year, site_id } = req.query;
      const targetMonth = month || new Date().getMonth() + 1;
      const targetYear = year || new Date().getFullYear();
      
      if (!cost_code && !site_id) {
        return res.status(400).json({
          success: false,
          error: 'cost_code or site_id parameter is required'
        });
      }

      const startOfMonth = new Date(targetYear, targetMonth - 1, 1).toISOString().split('T')[0];
      const endOfMonth = new Date(targetYear, targetMonth, 0).toISOString().split('T')[0];

      // Get operating sessions for the month
      let sessionsQuery = supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .gte('session_date', startOfMonth)
        .lte('session_date', endOfMonth)
        .eq('session_status', 'COMPLETED');
      
      if (site_id) {
        sessionsQuery = sessionsQuery.eq('branch', site_id);
      } else if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        sessionsQuery = sessionsQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: sessionsData, error: sessionsError } = await sessionsQuery;
      
      if (sessionsError) throw new Error(`Sessions error: ${sessionsError.message}`);

      // Group by branch and week
      const branchData = {};
      const weeklyData = {};
      
      sessionsData.forEach(session => {
        // Branch grouping
        if (!branchData[session.branch]) {
          branchData[session.branch] = {
            branch: session.branch,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            fuel_filled: 0,
            cost: 0
          };
        }
        const branch = branchData[session.branch];
        branch.sessions++;
        branch.operating_hours += parseFloat(session.operating_hours || 0);
        branch.fuel_usage += parseFloat(session.total_usage || 0);
        branch.fuel_filled += parseFloat(session.total_fill || 0);
        branch.cost += parseFloat(session.cost_for_usage || 0);

        // Weekly grouping
        const weekNum = this.getWeekNumber(new Date(session.session_date));
        if (!weeklyData[weekNum]) {
          weeklyData[weekNum] = {
            week: weekNum,
            sessions: 0,
            operating_hours: 0,
            fuel_usage: 0,
            cost: 0
          };
        }
        const week = weeklyData[weekNum];
        week.sessions++;
        week.operating_hours += parseFloat(session.operating_hours || 0);
        week.fuel_usage += parseFloat(session.total_usage || 0);
        week.cost += parseFloat(session.cost_for_usage || 0);
      });

      const summary = {
        total_branches: Object.keys(branchData).length,
        total_sessions: sessionsData.length,
        total_operating_hours: sessionsData.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0),
        total_fuel_usage: sessionsData.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0),
        total_cost: sessionsData.reduce((sum, s) => sum + parseFloat(s.cost_for_usage || 0), 0)
      };

      res.json({
        success: true,
        data: {
          month: targetMonth,
          year: targetYear,
          cost_code: cost_code,
          site_id: site_id,
          period: `${targetMonth}/${targetYear}`,
          branches: Object.values(branchData),
          weekly_breakdown: Object.values(weeklyData).sort((a, b) => a.week - b.week),
          summary: summary
        }
      });

    } catch (error) {
      console.error('Error generating monthly report by cost code:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate monthly report',
        message: error.message
      });
    }
  }

  // Download Daily Activity Report - Uses exact same logic as getDailyReport but for specific date
  async downloadActivityReport(req, res) {
    try {
      const ExcelJS = require('exceljs');
      const { date, cost_code, site_id, format = 'excel' } = req.query;
      
      // Validate required parameters
      if (!date) {
        return res.status(400).json({
          success: false,
          error: 'Date parameter is required',
          message: 'Please provide a date in YYYY-MM-DD format'
        });
      }
      
      const targetDate = date;
      
      // Use EXACT same logic as getDailyReport but for single date instead of month
      let dailyQuery = supabase
        .from('energy_rite_operating_sessions')
        .select(`
          branch,
          cost_code,
          company,
          operating_hours,
          total_usage,
          total_fill,
          session_status,
          liter_usage_per_hour,
          cost_for_usage,
          session_date,
          session_start_time,
          session_end_time
        `)
        .eq('session_date', targetDate)  // Only difference: single date instead of date range
        .eq('session_status', 'COMPLETED');
      
      // Apply hierarchical cost code filtering if provided (EXACT same as getDailyReport)
      if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        dailyQuery = dailyQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: dailyData, error: dailyError } = await dailyQuery;
      if (dailyError) throw new Error(`Database error: ${dailyError.message}`);
      
      // Get current fuel levels from latest fuel data (EXACT same as getDailyReport)
      const { data: currentFuelData, error: fuelError } = await supabase
        .from('energy_rite_fuel_data')
        .select('plate, fuel_probe_1_level, drivername, created_at')
        .order('created_at', { ascending: false });
        
      if (fuelError) throw new Error(`Fuel data error: ${fuelError.message}`);
      
      // Get latest fuel level per site (EXACT same as getDailyReport)
      const latestFuelBySite = {};
      currentFuelData.forEach(record => {
        if (!latestFuelBySite[record.plate]) {
          latestFuelBySite[record.plate] = {
            fuel_level: record.fuel_probe_1_level || 0,
            engine_status: record.drivername || 'UNKNOWN',
            last_update: record.created_at
          };
        }
      });
      
      // Group daily data by branch (EXACT same logic as getDailyReport)
      const dailyByBranch = {};
      dailyData.forEach(session => {
        if (!dailyByBranch[session.branch]) {
          dailyByBranch[session.branch] = {
            branch: session.branch,
            cost_code: session.cost_code,
            company: session.company || 'KFC',
            total_running_hours: 0,
            total_fuel_usage: 0,
            total_fuel_filled: 0,
            total_sessions: 0,
            completed_sessions: 0,
            avg_efficiency: 0,
            total_cost: 0,
            last_session: null
          };
        }
        
        const branch = dailyByBranch[session.branch];
        branch.total_running_hours += parseFloat(session.operating_hours || 0);
        branch.total_fuel_usage += parseFloat(session.total_usage || 0);
        branch.total_fuel_filled += parseFloat(session.total_fill || 0);
        branch.total_sessions += 1;
        branch.completed_sessions += 1;
        branch.total_cost += parseFloat(session.cost_for_usage || 0);
        
        // Track latest session
        if (!branch.last_session || session.session_end_time > branch.last_session) {
          branch.last_session = session.session_end_time;
        }
      });
      
      // Calculate averages (EXACT same as getDailyReport)
      Object.values(dailyByBranch).forEach(branch => {
        if (branch.total_running_hours > 0) {
          branch.avg_efficiency = branch.total_fuel_usage / branch.total_running_hours;
        }
      });
      
      // Get vehicle lookup with hierarchical cost code filtering (EXACT same as getDailyReport)
      let vehicleLookupQuery = supabase
        .from('energyrite_vehicle_lookup')
        .select('plate, cost_code');
        
      if (cost_code) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        const accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        vehicleLookupQuery = vehicleLookupQuery.in('cost_code', accessibleCostCodes);
      }
      
      const { data: vehicleLookup, error: lookupError } = await vehicleLookupQuery;
      if (lookupError) throw new Error(`Lookup error: ${lookupError.message}`);
      
      const allSites = new Set();
      vehicleLookup.forEach(v => allSites.add(v.plate));
      Object.keys(dailyByBranch).forEach(branch => allSites.add(branch));
      
      // Structure the response (EXACT same as getDailyReport but with date instead of month/year)
      const report = {
        report_date: new Date(),
        date: targetDate,
        period: targetDate,
        sites: Array.from(allSites).map(site => {
          const vehicleInfo = vehicleLookup.find(v => v.plate === site);
          const dailyInfo = dailyByBranch[site];
          const currentInfo = latestFuelBySite[site];
          
          return {
            branch: site,
            company: dailyInfo?.company || 'KFC',
            cost_code: vehicleInfo?.cost_code || 'N/A',
            current_fuel_level: currentInfo?.fuel_level || 0,
            current_engine_status: currentInfo?.engine_status || 'UNKNOWN',
            last_activity: currentInfo?.last_update || dailyInfo?.last_session,
            daily_data: dailyInfo || {
              total_running_hours: 0,
              total_fuel_usage: 0,
              total_fuel_filled: 0,
              total_sessions: 0,
              completed_sessions: 0,
              avg_efficiency: 0,
              total_cost: 0
            }
          };
        })
      };

      if (format === 'json') {
        // JSON download - EXACT same structure as getDailyReport
        const filename = `daily-activity-report-${targetDate}${cost_code ? '-' + cost_code : '-all-centers'}${site_id ? '-' + site_id : ''}.json`;
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.setHeader('Content-Type', 'application/json');
        return res.json({
          success: true,
          data: report
        });
      }

      // Excel download - keeping it simple and clean like getDailyReport would be
      const workbook = new ExcelJS.Workbook();
      
      // Single sheet: Daily Activity Overview (matching getDailyReport style)
      const overviewSheet = workbook.addWorksheet('Daily Overview');
      
      // Header
      overviewSheet.mergeCells('A1:H1');
      const titleCell = overviewSheet.getCell('A1');
      titleCell.value = 'Daily Activity Report - ' + targetDate;
      titleCell.font = { size: 16, bold: true };
      titleCell.alignment = { horizontal: 'center' };
      
      overviewSheet.mergeCells('A2:H2');
      const infoCell = overviewSheet.getCell('A2');
      infoCell.value = `Date: ${targetDate} | Cost Center: ${cost_code || 'All Centers'}${site_id ? ' | Site: ' + site_id : ''}`;
      infoCell.font = { size: 12, bold: true };
      infoCell.alignment = { horizontal: 'center' };
      
      // Column headers (matching getDailyReport data structure)
      const headers = ['Branch', 'Company', 'Cost Code', 'Current Fuel', 'Engine Status', 'Duration (H:M:S)', 'Fuel Usage', 'Total Cost', 'Start Time', 'End Time'];
      overviewSheet.addRow([]); // Empty row
      const headerRow = overviewSheet.addRow(headers);
      headerRow.font = { bold: true };
      
      // Data rows
      report.sites.forEach(site => {
        const formattedDuration = this.formatDuration(site.daily_data.total_running_hours);
        const startTime = site.daily_data.first_session_start ? 
          new Date(site.daily_data.first_session_start).toLocaleString() : 'No sessions';
        const endTime = site.daily_data.last_session ? 
          new Date(site.daily_data.last_session).toLocaleString() : 'No sessions';
        overviewSheet.addRow([
          site.branch,
          site.company,
          site.cost_code,
          site.current_fuel_level,
          site.current_engine_status,
          formattedDuration,
          site.daily_data.total_fuel_usage,
          site.daily_data.total_cost,
          startTime,
          endTime
        ]);
      });
      
      // Auto-fit columns with specific widths for time columns
      overviewSheet.columns.forEach((column, index) => {
        if (column.header) {
          // Make time columns wider
          const isTimeColumn = index >= 8; // Start Time and End Time columns
          column.width = isTimeColumn ? 20 : Math.max(column.header.length + 2, 15);
        }
      });

      // Generate and send Excel file
      const filename = `daily-activity-report-${targetDate}${cost_code ? '-' + cost_code : '-all-centers'}${site_id ? '-' + site_id : ''}.xlsx`;
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      
      const buffer = await workbook.xlsx.writeBuffer();
      res.send(buffer);

    } catch (error) {
      console.error('Error generating download report:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to generate download report',
        message: error.message
      });
    }
  }

  // Get snapshot data with optional cost code filtering, hierarchy support, and period-based fuel consumption
  async getSnapshotData(req, res) {
    try {
      const { 
        cost_code, 
        date,
        start_date,
        end_date, 
        snapshot_type, 
        limit = 50, 
        offset = 0,
        include_hierarchy = true, // New parameter for hierarchy support
        calculate_fuel_usage = false // New parameter for fuel consumption calculations
      } = req.query;

      console.log(`📸 Fetching snapshot data - Cost Code: ${cost_code || 'All'}, Date: ${date || start_date || 'Today'}, End Date: ${end_date || 'Same'}, Type: ${snapshot_type || 'All'}, Hierarchy: ${include_hierarchy}, Fuel Usage: ${calculate_fuel_usage}`);

      // Handle cost center hierarchy if cost_code is provided
      let accessibleCostCodes = [];
      if (cost_code && include_hierarchy !== 'false') {
        try {
          const costCenterAccess = require('../../helpers/cost-center-access');
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
          console.log(`🔐 Hierarchical access: ${accessibleCostCodes.length} cost centers accessible from ${cost_code}`);
        } catch (error) {
          console.log('⚠️ Cost center hierarchy access error:', error.message);
          // Fallback to exact match
          accessibleCostCodes = [cost_code];
        }
      } else if (cost_code) {
        // Exact match only
        accessibleCostCodes = [cost_code];
      }

      // Get target date or date range
      const targetStartDate = start_date || date || new Date().toISOString().split('T')[0];
      const targetEndDate = end_date || targetStartDate;

      // Build query for snapshots
      let query = supabase
        .from('energy_rite_daily_snapshots')
        .select('*');

      // Apply cost code filters using lookup table
      if (accessibleCostCodes.length > 0) {
        // Get vehicles with matching cost codes from lookup table
        const { data: vehiclesWithCostCode } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate')
          .in('cost_code', accessibleCostCodes);
        
        console.log(`🔍 Found ${vehiclesWithCostCode?.length || 0} vehicles matching cost codes: ${accessibleCostCodes.join(', ')}`);
        
        if (vehiclesWithCostCode && vehiclesWithCostCode.length > 0) {
          const vehiclePlates = vehiclesWithCostCode.map(v => v.plate);
          query = query.in('branch', vehiclePlates);
        } else {
          // No vehicles found for this cost code - return empty result
          console.log(`⚠️ No vehicles found for cost codes: ${accessibleCostCodes.join(', ')}`);
          return res.status(200).json({
            success: true,
            data: {
              snapshots: [],
              fuel_usage_analysis: null,
              pagination: { total_count: 0, limit: parseInt(limit), offset: parseInt(offset), has_more: false },
              summary: {
                total_snapshots: 0,
                snapshots_with_cost_codes: 0,
                cost_code_coverage_percentage: 0,
                total_fuel_volume: '0.0',
                average_fuel_level: '0.0'
              },
              hierarchy: {
                requested_cost_code: cost_code,
                accessible_cost_codes: accessibleCostCodes,
                hierarchy_enabled: include_hierarchy !== 'false',
                total_accessible_codes: accessibleCostCodes.length,
                direct_matches: 0,
                hierarchy_matches: 0
              },
              breakdowns: { by_cost_code: [], by_snapshot_type: {} },
              filters_applied: {
                cost_code: cost_code || null,
                accessible_cost_codes: accessibleCostCodes,
                start_date: targetStartDate,
                end_date: targetEndDate,
                snapshot_type: snapshot_type || null,
                include_hierarchy: include_hierarchy !== 'false',
                calculate_fuel_usage: calculate_fuel_usage === 'true'
              }
            },
            timestamp: new Date().toISOString(),
            message: 'No vehicles found for the specified cost code(s)'
          });
        }
      }

      // Apply date filter - support date range
      if (targetStartDate === targetEndDate) {
        query = query.eq('snapshot_date', targetStartDate);
      } else {
        query = query.gte('snapshot_date', targetStartDate).lte('snapshot_date', targetEndDate);
      }

      // Apply snapshot type filter if specified
      if (snapshot_type) {
        query = query.eq('snapshot_type', snapshot_type);
      }

      // Apply pagination and ordering
      query = query
        .order('snapshot_time', { ascending: false })
        .range(parseInt(offset), parseInt(offset) + parseInt(limit) - 1);

      const { data: snapshots, error } = await query;

      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }

      // Calculate fuel usage if requested
      let fuelUsageAnalysis = null;
      if (calculate_fuel_usage === 'true') {
        console.log('⛽ Calculating period fuel usage...');
        fuelUsageAnalysis = await calculatePeriodFuelUsage(targetEndDate, accessibleCostCodes, snapshot_type);
      }

      // Process and enhance snapshot data
      const processedSnapshots = snapshots.map(snapshot => {
        const snapshotData = snapshot.snapshot_data || {};
        return {
          id: snapshot.id,
          snapshot_date: snapshot.snapshot_date,
          snapshot_time: snapshot.snapshot_time,
          snapshot_type: snapshot.snapshot_type,
          branch: snapshot.branch,
          company: snapshot.company,
          cost_code: snapshotData.cost_code || null,
          fuel_level: snapshotData.fuel_level || 0,
          fuel_volume: snapshotData.fuel_volume || 0,
          engine_status: snapshotData.engine_status || 'UNKNOWN',
          vehicle_plate: snapshotData.vehicle_plate || snapshot.branch,
          site_id: snapshotData.site_id || null,
          notes: snapshotData.notes || null,
          captured_at: snapshotData.captured_at || snapshot.snapshot_time,
          raw_data: snapshotData
        };
      });

      // Calculate summary statistics
      const totalSnapshots = processedSnapshots.length;
      const withCostCodes = processedSnapshots.filter(s => s.cost_code).length;
      const totalFuelVolume = processedSnapshots.reduce((sum, s) => sum + (s.fuel_volume || 0), 0);
      const avgFuelLevel = totalSnapshots > 0 ? 
        processedSnapshots.reduce((sum, s) => sum + (s.fuel_level || 0), 0) / totalSnapshots : 0;

      // Group by cost code for breakdown with hierarchy info
      const costCodeBreakdown = {};
      processedSnapshots.forEach(snapshot => {
        const code = snapshot.cost_code || 'NO_COST_CODE';
        if (!costCodeBreakdown[code]) {
          costCodeBreakdown[code] = {
            cost_code: snapshot.cost_code,
            count: 0,
            total_fuel_volume: 0,
            vehicles: [],
            // Add hierarchy information
            is_accessible_via_hierarchy: cost_code && snapshot.cost_code && 
              snapshot.cost_code !== cost_code && 
              snapshot.cost_code.startsWith(cost_code + '-'),
            hierarchy_level: cost_code && snapshot.cost_code ? 
              snapshot.cost_code.split('-').length - (cost_code.split('-').length) : 0
          };
        }
        costCodeBreakdown[code].count++;
        costCodeBreakdown[code].total_fuel_volume += snapshot.fuel_volume || 0;
        if (!costCodeBreakdown[code].vehicles.includes(snapshot.vehicle_plate)) {
          costCodeBreakdown[code].vehicles.push(snapshot.vehicle_plate);
        }
      });

      // Group by snapshot type
      const typeBreakdown = {};
      processedSnapshots.forEach(snapshot => {
        const type = snapshot.snapshot_type;
        typeBreakdown[type] = (typeBreakdown[type] || 0) + 1;
      });

      // Create hierarchy summary
      const hierarchySummary = cost_code && include_hierarchy !== 'false' ? {
        requested_cost_code: cost_code,
        accessible_cost_codes: accessibleCostCodes,
        hierarchy_enabled: true,
        total_accessible_codes: accessibleCostCodes.length,
        direct_matches: processedSnapshots.filter(s => s.cost_code === cost_code).length,
        hierarchy_matches: processedSnapshots.filter(s => 
          s.cost_code && s.cost_code !== cost_code && 
          accessibleCostCodes.includes(s.cost_code)
        ).length
      } : {
        hierarchy_enabled: false
      };

      res.status(200).json({
        success: true,
        data: {
          snapshots: processedSnapshots,
          fuel_usage_analysis: fuelUsageAnalysis, // New fuel consumption data
          pagination: {
            total_count: totalSnapshots,
            limit: parseInt(limit),
            offset: parseInt(offset),
            has_more: totalSnapshots === parseInt(limit)
          },
          summary: {
            total_snapshots: totalSnapshots,
            snapshots_with_cost_codes: withCostCodes,
            cost_code_coverage_percentage: totalSnapshots > 0 ? 
              ((withCostCodes / totalSnapshots) * 100).toFixed(1) : 0,
            total_fuel_volume: totalFuelVolume.toFixed(1),
            average_fuel_level: avgFuelLevel.toFixed(1)
          },
          hierarchy: hierarchySummary,
          breakdowns: {
            by_cost_code: Object.values(costCodeBreakdown),
            by_snapshot_type: typeBreakdown
          },
          filters_applied: {
            cost_code: cost_code || null,
            accessible_cost_codes: accessibleCostCodes.length > 0 ? accessibleCostCodes : null,
            start_date: targetStartDate,
            end_date: targetEndDate,
            snapshot_type: snapshot_type || null,
            include_hierarchy: include_hierarchy !== 'false',
            calculate_fuel_usage: calculate_fuel_usage === 'true'
          }
        },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('❌ Error fetching snapshot data:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to fetch snapshot data',
        message: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

module.exports = new EnergyRiteReportsController();
