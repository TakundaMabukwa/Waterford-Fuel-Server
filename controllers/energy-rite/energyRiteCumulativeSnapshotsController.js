const { supabase } = require('../../supabase-client');

/**
 * Get cumulative snapshots for a specific month
 */
const getCumulativeMonthlySnapshots = async (req, res) => {
  try {
    const { year, month } = req.params;
    const { cost_code } = req.query;
    
    if (!year || !month) {
      return res.status(400).json({
        success: false,
        error: 'Year and month parameters are required'
      });
    }

    // Month-to-date system: end date is always yesterday
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const endDate = yesterday.toISOString().split('T')[0];
    
    // Start date is the 1st of the requested month
    const startDate = `${year}-${month.padStart(2, '0')}-01`;

    console.log(`📅 Cumulative snapshots (month-to-date): ${startDate} to ${endDate} (yesterday), Cost Code: ${cost_code || 'All'}`);

    // Get accessible cost codes if cost_code provided
    let accessibleCostCodes = [];
    let vehiclePlatesForCostCode = [];
    
    if (cost_code) {
      try {
        const costCenterAccess = require('../../helpers/cost-center-access');
        accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        console.log(`🔐 Accessible cost codes: ${accessibleCostCodes.join(', ')}`);
        
        // Get vehicle plates for these cost codes
        const { data: vehicleLookup, error: lookupError } = await supabase
          .from('energyrite_vehicle_lookup')
          .select('plate, cost_code')
          .in('cost_code', accessibleCostCodes);
        
        if (lookupError) {
          console.error('❌ Error fetching vehicle lookup:', lookupError);
        }
        
        vehiclePlatesForCostCode = vehicleLookup?.map(v => v.plate) || [];
        console.log(`🚗 Found ${vehiclePlatesForCostCode.length} vehicles for cost codes`);
        console.log(`🚗 Vehicle plates: ${vehiclePlatesForCostCode.join(', ')}`);
        
      } catch (error) {
        console.log('⚠️ Cost center access error:', error.message);
        accessibleCostCodes = [cost_code];
      }
    }

    // Get all snapshots for the month
    let query = supabase
      .from('energy_rite_activity_snapshots')
      .select('*')
      .gte('snapshot_date', startDate)
      .lte('snapshot_date', endDate)
      .order('snapshot_time', { ascending: true });

    const { data: snapshots, error } = await query;

    if (error) throw error;

    if (!snapshots || snapshots.length === 0) {
      console.log(`⚠️ No snapshots found for period ${startDate} to ${endDate}`);
      return res.json({
        success: true,
        data: {
          period: `${year}-${month}`,
          cost_code: cost_code || 'All',
          accessible_cost_codes: accessibleCostCodes.length > 0 ? accessibleCostCodes : null,
          filtered_vehicles: vehiclePlatesForCostCode.length > 0 ? vehiclePlatesForCostCode.length : null,
          total_snapshots: 0,
          cumulative_metrics: {},
          monthly_fuel_usage: {
            morning_7_12: 0,
            afternoon_12_17: 0,
            evening_17_24: 0,
            total_monthly: 0
          },
          daily_summaries: []
        },
        message: 'No snapshot data found for this period'
      });
    }

    console.log(`📸 Found ${snapshots.length} snapshots for period`);

    // Filter snapshots by cost code if provided - filter at snapshot level for activity_snapshots
    let filteredSnapshots = snapshots;
    if (vehiclePlatesForCostCode.length > 0) {
      filteredSnapshots = snapshots.filter(snapshot => {
        if (!snapshot.vehicles_data || !Array.isArray(snapshot.vehicles_data)) return false;
        
        // Check if any vehicle in this snapshot matches our cost code plates
        return snapshot.vehicles_data.some(vehicle => 
          vehiclePlatesForCostCode.includes(vehicle.branch || vehicle.plate)
        );
      });
      
      console.log(`✅ Filtered snapshots: ${filteredSnapshots.length} of ${snapshots.length} match cost code criteria`);
    }

    // Group by date and calculate cumulative metrics
    const dailySummaries = {};
    let cumulativeActiveHours = 0;
    let cumulativeFuelUsage = 0;
    let totalVehicleDays = 0;

    filteredSnapshots.forEach(snapshot => {
      const date = snapshot.snapshot_date;
      
      // Filter vehicles within snapshot by cost code plates
      let snapshotVehicles = snapshot.vehicles_data || [];
      if (vehiclePlatesForCostCode.length > 0) {
        snapshotVehicles = snapshotVehicles.filter(vehicle => 
          vehiclePlatesForCostCode.includes(vehicle.branch || vehicle.plate)
        );
      }
      
      const activeVehicles = snapshotVehicles.filter(v => v.is_active).length;
      const totalVehicles = snapshotVehicles.length;
      
      if (!dailySummaries[date]) {
        dailySummaries[date] = {
          date,
          snapshots: [],
          peak_active_vehicles: 0,
          total_fuel_level: 0,
          average_utilization: 0
        };
      }

      dailySummaries[date].snapshots.push({
        time_slot: snapshot.time_slot,
        active_vehicles: activeVehicles,
        total_vehicles: totalVehicles,
        utilization_rate: totalVehicles > 0 ? (activeVehicles / totalVehicles) * 100 : 0
      });

      if (activeVehicles > dailySummaries[date].peak_active_vehicles) {
        dailySummaries[date].peak_active_vehicles = activeVehicles;
      }

      const fuelLevel = snapshotVehicles.reduce((sum, v) => sum + (v.fuel_level || 0), 0);
      dailySummaries[date].total_fuel_level += fuelLevel;
      totalVehicleDays += totalVehicles;
      cumulativeActiveHours += activeVehicles;
    });

    // Calculate daily averages
    Object.values(dailySummaries).forEach(day => {
      day.average_utilization = day.snapshots.reduce((sum, s) => sum + s.utilization_rate, 0) / day.snapshots.length;
    });

    // Calculate monthly fuel usage from operating sessions
    console.log(`🔍 Querying sessions from ${startDate} to ${endDate} (yesterday)`);
    
    // Query all sessions for the period without cost code filter initially
    let sessionsQuery = supabase
      .from('energy_rite_operating_sessions')
      .select('total_usage, session_date, session_start_time, cost_code, branch')
      .gte('session_date', startDate)
      .lte('session_date', endDate)
      .eq('session_status', 'COMPLETED');
    
    const { data: allSessions, error: sessionsError } = await sessionsQuery;
    
    if (sessionsError) {
      console.error('❌ Error fetching sessions:', sessionsError);
    }
    
    console.log(`📊 Found ${allSessions?.length || 0} total completed sessions for the period`);
    
    // Filter sessions based on cost code criteria
    let filteredSessions = allSessions || [];
    
    if (cost_code && (accessibleCostCodes.length > 0 || vehiclePlatesForCostCode.length > 0)) {
      const beforeFilter = filteredSessions.length;
      
      // Filter by EITHER cost_code match OR branch/plate match
      filteredSessions = filteredSessions.filter(session => {
        const matchesCostCode = accessibleCostCodes.includes(session.cost_code);
        const matchesPlate = vehiclePlatesForCostCode.includes(session.branch);
        return matchesCostCode || matchesPlate;
      });
      
      console.log(`🔍 Filtered sessions: ${beforeFilter} → ${filteredSessions.length}`);
      console.log(`   - By cost_code: ${filteredSessions.filter(s => accessibleCostCodes.includes(s.cost_code)).length}`);
      console.log(`   - By branch/plate: ${filteredSessions.filter(s => vehiclePlatesForCostCode.includes(s.branch)).length}`);
    }
    
    // Log sample session for debugging
    if (filteredSessions.length > 0) {
      console.log(`📋 Sample session:`, JSON.stringify(filteredSessions[0]));
    } else if (allSessions && allSessions.length > 0) {
      console.log(`📋 Sample of unfiltered session:`, JSON.stringify(allSessions[0]));
    }
    
    // Group fuel usage by time periods
    const monthlyFuelUsage = {
      morning: 0, // 7-12
      afternoon: 0, // 12-17 
      evening: 0 // 17-24
    };
    
    filteredSessions?.forEach(session => {
      const hour = new Date(session.session_start_time).getHours();
      const usage = parseFloat(session.total_usage || 0);
      
      if (hour >= 7 && hour < 12) {
        monthlyFuelUsage.morning += usage;
        console.log(`  ☀️ Morning session: ${usage}L at ${hour}:00`);
      } else if (hour >= 12 && hour < 17) {
        monthlyFuelUsage.afternoon += usage;
        console.log(`  🌤️ Afternoon session: ${usage}L at ${hour}:00`);
      } else if (hour >= 17 && hour < 24) {
        monthlyFuelUsage.evening += usage;
        console.log(`  🌙 Evening session: ${usage}L at ${hour}:00`);
      }
    });
    
    console.log(`⛽ Monthly fuel usage: Morning: ${monthlyFuelUsage.morning.toFixed(2)}L, Afternoon: ${monthlyFuelUsage.afternoon.toFixed(2)}L, Evening: ${monthlyFuelUsage.evening.toFixed(2)}L`);

    const totalDays = Object.keys(dailySummaries).length;
    const avgVehiclesPerDay = filteredSnapshots.length > 0 ? totalVehicleDays / filteredSnapshots.length : 0;

    const result = {
      success: true,
      data: {
        period: `${year}-${month}`,
        cost_code: cost_code || 'All',
        accessible_cost_codes: accessibleCostCodes.length > 0 ? accessibleCostCodes : null,
        filtered_vehicles: vehiclePlatesForCostCode.length > 0 ? vehiclePlatesForCostCode.length : null,
        total_snapshots: filteredSnapshots.length,
        total_days_with_data: totalDays,
        cumulative_metrics: {
          total_active_vehicle_hours: cumulativeActiveHours,
          average_vehicles_per_day: Math.round(avgVehiclesPerDay),
          average_utilization_rate: totalVehicleDays > 0 ? ((cumulativeActiveHours / totalVehicleDays) * 100).toFixed(2) : 0,
          peak_single_day_active: Math.max(...Object.values(dailySummaries).map(d => d.peak_active_vehicles), 0)
        },
        monthly_fuel_usage: {
          morning_7_12: parseFloat(monthlyFuelUsage.morning.toFixed(2)),
          afternoon_12_17: parseFloat(monthlyFuelUsage.afternoon.toFixed(2)),
          evening_17_24: parseFloat(monthlyFuelUsage.evening.toFixed(2)),
          total_monthly: parseFloat((monthlyFuelUsage.morning + monthlyFuelUsage.afternoon + monthlyFuelUsage.evening).toFixed(2))
        },
        daily_summaries: Object.values(dailySummaries).sort((a, b) => a.date.localeCompare(b.date))
      }
    };

    res.json(result);

  } catch (error) {
    console.error('Error getting cumulative monthly snapshots:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

module.exports = {
  getCumulativeMonthlySnapshots
};