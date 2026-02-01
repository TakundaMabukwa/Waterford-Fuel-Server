const fs = require('fs');
const path = require('path');

// Add the monthly snapshots method to the controller
const methodToAdd = `
  /**
   * Get cumulative monthly snapshots
   */
  async getMonthlyCumulativeSnapshots(req, res) {
    try {
      const { 
        start_date, 
        end_date, 
        month,
        cost_code, 
        cost_codes,
        include_hierarchy = false 
      } = req.query;

      let startDate, endDate;

      if (month) {
        // Month format: YYYY-MM
        const [year, monthNum] = month.split('-');
        startDate = new Date(parseInt(year), parseInt(monthNum) - 1, 1);
        endDate = new Date(parseInt(year), parseInt(monthNum), 0);
      } else if (start_date && end_date) {
        startDate = new Date(start_date);
        endDate = new Date(end_date);
      } else {
        // Default to current month
        const now = new Date();
        startDate = new Date(now.getFullYear(), now.getMonth(), 1);
        endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);
      }

      const start = startDate.toISOString().split('T')[0];
      const end = endDate.toISOString().split('T')[0];

      console.log(\`📊 Monthly Snapshots: \${start} to \${end}\`);

      // Get all snapshots for the period
      let snapshotsQuery = supabase
        .from('energy_rite_activity_snapshots')
        .select('*')
        .gte('snapshot_date', start)
        .lte('snapshot_date', end)
        .order('snapshot_date, snapshot_time');

      const { data: snapshots, error } = await snapshotsQuery;
      if (error) throw error;

      // Apply cost code filtering if provided
      let filteredSnapshots = snapshots;
      if (cost_code || cost_codes) {
        const costCenterAccess = require('../../helpers/cost-center-access');
        let accessibleCostCodes = [];
        
        if (cost_codes) {
          const codeArray = cost_codes.split(',').map(c => c.trim());
          for (const code of codeArray) {
            const accessible = await costCenterAccess.getAccessibleCostCenters(code);
            accessibleCostCodes.push(...accessible);
          }
        } else if (cost_code) {
          accessibleCostCodes = await costCenterAccess.getAccessibleCostCenters(cost_code);
        }

        // Filter snapshots by cost codes
        filteredSnapshots = snapshots.filter(snapshot => {
          if (!snapshot.vehicles_data) return false;
          return snapshot.vehicles_data.some(vehicle => 
            accessibleCostCodes.includes(vehicle.cost_code)
          );
        });
      }

      // Group snapshots by date and time slot
      const dailySnapshots = {};
      const cumulativeMetrics = {
        total_snapshots: filteredSnapshots.length,
        total_days: 0,
        total_active_vehicles: 0,
        total_fuel_consumption: 0,
        average_utilization: 0,
        peak_activity_day: null,
        peak_activity_count: 0
      };

      const timeSlotTotals = {
        morning: { total_active: 0, total_fuel: 0, count: 0 },
        afternoon: { total_active: 0, total_fuel: 0, count: 0 },
        evening: { total_active: 0, total_fuel: 0, count: 0 }
      };

      filteredSnapshots.forEach(snapshot => {
        const date = snapshot.snapshot_date;
        const timeSlot = snapshot.time_slot;

        if (!dailySnapshots[date]) {
          dailySnapshots[date] = {
            date,
            morning: null,
            afternoon: null,
            evening: null,
            daily_peak_activity: 0,
            daily_total_fuel: 0
          };
        }

        const snapshotData = {
          time_slot: timeSlot,
          snapshot_time: snapshot.snapshot_time,
          active_vehicles: snapshot.active_vehicles || 0,
          total_vehicles: snapshot.total_vehicles || 0,
          utilization_rate: snapshot.total_vehicles > 0 ? 
            ((snapshot.active_vehicles / snapshot.total_vehicles) * 100).toFixed(1) : '0',
          average_fuel_percentage: snapshot.average_fuel_percentage || 0,
          vehicles_breakdown: snapshot.vehicles_data || []
        };

        dailySnapshots[date][timeSlot] = snapshotData;

        // Update daily metrics
        if (snapshot.active_vehicles > dailySnapshots[date].daily_peak_activity) {
          dailySnapshots[date].daily_peak_activity = snapshot.active_vehicles;
        }

        // Update time slot totals
        if (timeSlotTotals[timeSlot]) {
          timeSlotTotals[timeSlot].total_active += snapshot.active_vehicles || 0;
          timeSlotTotals[timeSlot].count++;
        }

        // Update cumulative metrics
        cumulativeMetrics.total_active_vehicles += snapshot.active_vehicles || 0;
        
        if (snapshot.active_vehicles > cumulativeMetrics.peak_activity_count) {
          cumulativeMetrics.peak_activity_count = snapshot.active_vehicles;
          cumulativeMetrics.peak_activity_day = date;
        }
      });

      // Calculate averages
      const totalDays = Object.keys(dailySnapshots).length;
      cumulativeMetrics.total_days = totalDays;
      cumulativeMetrics.average_utilization = filteredSnapshots.length > 0 ? 
        (cumulativeMetrics.total_active_vehicles / filteredSnapshots.length).toFixed(1) : 0;

      // Calculate time slot averages
      Object.keys(timeSlotTotals).forEach(slot => {
        const slotData = timeSlotTotals[slot];
        slotData.average_active = slotData.count > 0 ? 
          (slotData.total_active / slotData.count).toFixed(1) : 0;
      });

      // Determine overall peak time slot
      const peakTimeSlot = Object.entries(timeSlotTotals)
        .sort(([,a], [,b]) => b.total_active - a.total_active)[0];

      const response = {
        period: {
          start_date: start,
          end_date: end,
          total_days: totalDays,
          month_filter: month || null,
          cost_code_filter: cost_code || null,
          cost_codes_filter: cost_codes || null
        },
        cumulative_summary: cumulativeMetrics,
        time_slot_analysis: {
          overall_peak_slot: peakTimeSlot[0],
          overall_peak_activity: peakTimeSlot[1].total_active,
          morning: {
            total_snapshots: timeSlotTotals.morning.count,
            total_active_vehicles: timeSlotTotals.morning.total_active,
            average_active_vehicles: timeSlotTotals.morning.average_active
          },
          afternoon: {
            total_snapshots: timeSlotTotals.afternoon.count,
            total_active_vehicles: timeSlotTotals.afternoon.total_active,
            average_active_vehicles: timeSlotTotals.afternoon.average_active
          },
          evening: {
            total_snapshots: timeSlotTotals.evening.count,
            total_active_vehicles: timeSlotTotals.evening.total_active,
            average_active_vehicles: timeSlotTotals.evening.average_active
          }
        },
        daily_breakdown: Object.values(dailySnapshots).sort((a, b) => 
          new Date(a.date) - new Date(b.date)
        ),
        monthly_insights: [
          \`\${totalDays} days of snapshot data collected\`,
          \`\${cumulativeMetrics.total_snapshots} total snapshots taken\`,
          \`Peak activity: \${cumulativeMetrics.peak_activity_count} vehicles on \${cumulativeMetrics.peak_activity_day}\`,
          \`Average utilization: \${cumulativeMetrics.average_utilization}% across all snapshots\`,
          \`Most active time slot: \${peakTimeSlot[0]} (\${peakTimeSlot[1].total_active} total active vehicles)\`
        ]
      };

      res.status(200).json({
        success: true,
        data: response,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Error generating monthly cumulative snapshots:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
`;

console.log('Method to add to energyRiteReportsController.js:');
console.log(methodToAdd);
console.log('\nAdd this method before the closing bracket of the class.');