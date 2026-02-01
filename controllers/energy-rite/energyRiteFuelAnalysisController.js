const { supabase } = require('../../supabase-client');

class EnergyRiteFuelAnalysisController {
  
  // Detect fuel anomalies and irregularities
  async detectFuelAnomalies(req, res) {
    try {
      const { branch, startDate, endDate, cost_code } = req.query;
      
      let query = supabase
        .from('energy_rite_fuel_data')
        .select('*')
        .not('fuel_probe_1_level', 'is', null);
      
      // Add filters
      if (branch) {
        query = query.eq('plate', branch); // Using plate as branch identifier
      }
      
      if (startDate) {
        query = query.gte('created_at', startDate);
      }
      
      if (endDate) {
        query = query.lte('created_at', endDate);
      }
      
      query = query.order('plate').order('created_at');
      
      const { data, error } = await query;
      if (error) throw new Error(`Database error: ${error.message}`);
      
      const anomalies = [];
      
      // Group by plate to analyze fuel changes
      const plateGroups = {};
      data.forEach(row => {
        if (!plateGroups[row.plate]) {
          plateGroups[row.plate] = [];
        }
        plateGroups[row.plate].push(row);
      });
      
      // Analyze each plate's fuel data
      Object.entries(plateGroups).forEach(([plate, records]) => {
        for (let i = 1; i < records.length; i++) {
          const current = records[i];
          const previous = records[i - 1];
          
          const currentFuel = parseFloat(current.fuel_probe_1_level);
          const previousFuel = parseFloat(previous.fuel_probe_1_level);
          const fuelDifference = currentFuel - previousFuel;
          const engineStatus = current.status || 'UNKNOWN';
          
          // Check for various anomaly types
          if (previousFuel && currentFuel) {
            // 1. Filled while generator ON (10-15 litres)
            if (fuelDifference > 10 && fuelDifference < 15 && engineStatus === 'ON') {
              anomalies.push({
                plate: plate,
                anomaly_type: 'FILLED_WHILE_ON',
                anomaly_date: current.created_at,
                fuel_before: previousFuel,
                fuel_after: currentFuel,
                difference: fuelDifference,
                severity: 'MEDIUM',
                status: 'pending',
                anomaly_data: {
                  engine_status: engineStatus,
                  notes: 'Filled while generator ON; calculation affected.'
                }
              });
            }
            
            // 2. Possible fuel theft (sudden large decrease)
            if (fuelDifference < -50 && engineStatus === 'OFF') {
              anomalies.push({
                plate: plate,
                anomaly_type: 'POSSIBLE_THEFT',
                anomaly_date: current.created_at,
                fuel_before: previousFuel,
                fuel_after: currentFuel,
                difference: fuelDifference,
                severity: 'HIGH',
                status: 'pending',
                anomaly_data: {
                  engine_status: engineStatus,
                  notes: 'Large fuel decrease detected while generator OFF - possible theft'
                }
              });
            }
            
            // 3. Possible fuel spillage (sudden large decrease while ON)
            if (fuelDifference < -30 && engineStatus === 'ON') {
              anomalies.push({
                plate: plate,
                anomaly_type: 'POSSIBLE_SPILLAGE',
                anomaly_date: current.created_at,
                fuel_before: previousFuel,
                fuel_after: currentFuel,
                difference: fuelDifference,
                severity: 'HIGH',
                status: 'pending',
                anomaly_data: {
                  engine_status: engineStatus,
                  notes: 'Large fuel decrease detected while generator ON - possible spillage'
                }
              });
            }
            
            // 4. Unusual consumption pattern
            if (fuelDifference < -100) {
              anomalies.push({
                plate: plate,
                anomaly_type: 'UNUSUAL_CONSUMPTION',
                anomaly_date: current.created_at,
                fuel_before: previousFuel,
                fuel_after: currentFuel,
                difference: fuelDifference,
                severity: 'MEDIUM',
                status: 'pending',
                anomaly_data: {
                  engine_status: engineStatus,
                  notes: 'Unusually high fuel consumption detected'
                }
              });
            }
          }
        }
      });
      
      // Store detected anomalies in database
      if (anomalies.length > 0) {
        const { error: insertError } = await supabase
          .from('energy_rite_fuel_anomalies')
          .upsert(anomalies, {
            onConflict: 'plate,anomaly_date,anomaly_type'
          });
        
        if (insertError) throw new Error(`Database error: ${insertError.message}`);
      }
      
      res.json({
        success: true,
        data: {
          anomalies_detected: anomalies.length,
          anomalies: anomalies
        }
      });
      
    } catch (error) {
      console.error('Error detecting fuel anomalies:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to detect fuel anomalies',
        message: error.message
      });
    }
  }
  
  // Get fuel anomalies for a specific period
  async getFuelAnomalies(req, res) {
    try {
      const { branch, startDate, endDate, severity, resolved, cost_code } = req.query;
      
      let query = supabase
        .from('energy_rite_fuel_anomalies')
        .select('*');
      
      // Add filters
      if (branch) {
        query = query.eq('plate', branch);
      }
      
      if (startDate) {
        query = query.gte('anomaly_date', startDate);
      }
      
      if (endDate) {
        query = query.lte('anomaly_date', endDate);
      }
      
      if (severity) {
        query = query.eq('severity', severity);
      }
      
      if (resolved !== undefined) {
        query = query.eq('status', resolved === 'true' ? 'resolved' : 'pending');
      }
      
      query = query.order('anomaly_date', { ascending: false });
      
      const { data, error } = await query;
      if (error) throw new Error(`Database error: ${error.message}`);
      
      res.json({
        success: true,
        data: {
          anomalies: data.map(row => ({
            id: row.id,
            plate: row.plate,
            anomaly_type: row.anomaly_type,
            anomaly_date: row.anomaly_date,
            fuel_before: parseFloat(row.fuel_before),
            fuel_after: parseFloat(row.fuel_after),
            difference: parseFloat(row.difference),
            severity: row.severity,
            status: row.status,
            anomaly_data: row.anomaly_data,
            created_at: row.created_at,
            updated_at: row.updated_at
          }))
        }
      });
      
    } catch (error) {
      console.error('Error fetching fuel anomalies:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to fetch fuel anomalies',
        message: error.message
      });
    }
  }
  
  // Mark anomaly as resolved
  async resolveAnomaly(req, res) {
    try {
      const { id } = req.params;
      const { resolved_by, resolution_notes } = req.body;
      
      const { data, error } = await supabase
        .from('energy_rite_fuel_anomalies')
        .update({
          status: 'resolved',
          updated_at: new Date().toISOString(),
          anomaly_data: {
            resolved_by: resolved_by,
            resolution_notes: resolution_notes,
            resolved_at: new Date().toISOString()
          }
        })
        .eq('id', id)
        .select();
      
      if (error) throw new Error(`Database error: ${error.message}`);
      
      if (data.length === 0) {
        return res.status(404).json({
          success: false,
          error: 'Anomaly not found'
        });
      }
      
      res.json({
        success: true,
        message: 'Anomaly marked as resolved',
        data: data[0]
      });
      
    } catch (error) {
      console.error('Error resolving anomaly:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to resolve anomaly',
        message: error.message
      });
    }
  }
  
  // Get fuel consumption analysis
  async getFuelConsumptionAnalysis(req, res) {
    try {
      const { branch, startDate, endDate } = req.query;
      
      let query = supabase
        .from('energy_rite_fuel_data')
        .select('*')
        .not('fuel_probe_1_level', 'is', null);
      
      if (branch) {
        query = query.eq('plate', branch);
      }
      
      if (startDate) {
        query = query.gte('created_at', startDate);
      }
      
      if (endDate) {
        query = query.lte('created_at', endDate);
      }
      
      query = query.order('plate').order('created_at');
      
      const { data, error } = await query;
      if (error) throw new Error(`Database error: ${error.message}`);
      
      // Calculate consumption metrics
      const consumptionData = {};
      
      // Group by plate
      const plateGroups = {};
      data.forEach(row => {
        if (!plateGroups[row.plate]) {
          plateGroups[row.plate] = [];
        }
        plateGroups[row.plate].push(row);
      });
      
      Object.entries(plateGroups).forEach(([plate, records]) => {
        consumptionData[plate] = {
          plate: plate,
          total_fuel_consumed: 0,
          total_fuel_filled: 0,
          consumption_events: 0,
          fill_events: 0,
          avg_consumption_per_event: 0,
          avg_fill_per_event: 0,
          engine_on_consumption: 0,
          engine_off_consumption: 0
        };
        
        for (let i = 1; i < records.length; i++) {
          const current = records[i];
          const previous = records[i - 1];
          
          const currentFuel = parseFloat(current.fuel_probe_1_level);
          const previousFuel = parseFloat(previous.fuel_probe_1_level);
          const fuelDifference = currentFuel - previousFuel;
          
          if (fuelDifference < 0) {
            // Fuel consumption
            consumptionData[plate].total_fuel_consumed += Math.abs(fuelDifference);
            consumptionData[plate].consumption_events++;
            
            if (current.status === 'ON') {
              consumptionData[plate].engine_on_consumption += Math.abs(fuelDifference);
            } else {
              consumptionData[plate].engine_off_consumption += Math.abs(fuelDifference);
            }
          } else if (fuelDifference > 0) {
            // Fuel fill
            consumptionData[plate].total_fuel_filled += fuelDifference;
            consumptionData[plate].fill_events++;
          }
        }
      });
      
      // Calculate averages
      Object.values(consumptionData).forEach(data => {
        if (data.consumption_events > 0) {
          data.avg_consumption_per_event = data.total_fuel_consumed / data.consumption_events;
        }
        if (data.fill_events > 0) {
          data.avg_fill_per_event = data.total_fuel_filled / data.fill_events;
        }
      });
      
      res.json({
        success: true,
        data: {
          analysis_period: {
            start_date: startDate,
            end_date: endDate,
            branch: branch || 'All branches'
          },
          consumption_summary: Object.values(consumptionData)
        }
      });
      
    } catch (error) {
      console.error('Error analyzing fuel consumption:', error);
      res.status(500).json({
        success: false,
        error: 'Failed to analyze fuel consumption',
        message: error.message
      });
    }
  }
}

module.exports = new EnergyRiteFuelAnalysisController();