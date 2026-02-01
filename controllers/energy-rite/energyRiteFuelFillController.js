const { supabase } = require('../../supabase-client');
const { getFuelFillStatistics, getVehicleFillHistory } = require('../../helpers/fuel-fill-detector');

class EnergyRiteFuelFillController {
  
  /**
   * Get all fuel fills with optional filters
   */
  async getAllFuelFills(req, res) {
    try {
      const { 
        limit = 50,
        plate,
        days = 30,
        detection_method,
        orderBy = 'fill_date',
        order = 'desc'
      } = req.query;
      
      let query = supabase
        .from('energy_rite_fuel_fills')
        .select('*');
      
      // Add filters
      if (plate) {
        query = query.eq('plate', plate);
      }
      
      if (detection_method) {
        query = query.eq('detection_method', detection_method);
      }
      
      // Filter by days
      if (days) {
        const startDate = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000).toISOString();
        query = query.gte('fill_date', startDate);
      }
      
      // Order and limit
      query = query.order(orderBy, { ascending: order.toLowerCase() === 'asc' })
                   .limit(parseInt(limit));
      
      const { data, error } = await query;
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      res.status(200).json({
        success: true,
        data: data,
        count: data.length,
        filters: {
          limit: parseInt(limit),
          plate,
          days: parseInt(days),
          detection_method,
          orderBy,
          order
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching fuel fills:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get fuel fill statistics
   */
  async getFuelFillStats(req, res) {
    try {
      const stats = await getFuelFillStatistics();
      
      if (stats.error) {
        throw new Error(stats.error);
      }
      
      res.status(200).json({
        success: true,
        data: stats,
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching fuel fill statistics:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get fuel fill history for a specific vehicle
   */
  async getVehicleFillHistory(req, res) {
    try {
      const { plate } = req.params;
      const { days = 30 } = req.query;
      
      const history = await getVehicleFillHistory(plate, parseInt(days));
      
      if (!history.success) {
        throw new Error(history.error);
      }
      
      res.status(200).json({
        success: true,
        data: history,
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error(`Error fetching fill history for ${req.params.plate}:`, error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get daily fuel fill summary
   */
  async getDailyFillSummary(req, res) {
    try {
      const { date, cost_code } = req.query;
      const targetDate = date || new Date().toISOString().split('T')[0];
      
      let query = supabase
        .from('energy_rite_fuel_fills')
        .select('*')
        .gte('fill_date', targetDate)
        .lt('fill_date', new Date(Date.parse(targetDate) + 24 * 60 * 60 * 1000).toISOString());
      
      const { data: fillData, error } = await query;
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      // Group by vehicle
      const fillsByVehicle = {};
      fillData.forEach(fill => {
        if (!fillsByVehicle[fill.plate]) {
          fillsByVehicle[fill.plate] = {
            plate: fill.plate,
            fill_count: 0,
            total_fill_amount: 0,
            fills: []
          };
        }
        
        fillsByVehicle[fill.plate].fill_count++;
        fillsByVehicle[fill.plate].total_fill_amount += parseFloat(fill.fill_amount || 0);
        fillsByVehicle[fill.plate].fills.push({
          time: fill.fill_date,
          amount: fill.fill_amount,
          method: fill.detection_method,
          fuel_before: fill.fuel_before,
          fuel_after: fill.fuel_after
        });
      });
      
      const summary = {
        date: targetDate,
        total_vehicles_filled: Object.keys(fillsByVehicle).length,
        total_fill_events: fillData.length,
        total_fuel_filled: fillData.reduce((sum, f) => sum + parseFloat(f.fill_amount || 0), 0),
        vehicles: Object.values(fillsByVehicle)
      };
      
      res.status(200).json({
        success: true,
        data: summary,
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching daily fill summary:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get fuel fills for reports integration
   */
  async getFillsForReports(req, res) {
    try {
      const { start_date, end_date, cost_code, site_id } = req.query;
      
      if (!start_date || !end_date) {
        return res.status(400).json({
          success: false,
          error: 'start_date and end_date are required'
        });
      }
      
      let query = supabase
        .from('energy_rite_fuel_fills')
        .select(`
          plate,
          fill_date,
          fill_amount,
          fuel_before,
          fuel_after,
          detection_method
        `)
        .gte('fill_date', start_date)
        .lte('fill_date', end_date);
      
      if (site_id) {
        query = query.eq('plate', site_id);
      }
      
      const { data: fillData, error } = await query;
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      // Group by date and vehicle for report integration
      const fillsByDate = {};
      fillData.forEach(fill => {
        const fillDate = fill.fill_date.split('T')[0];
        
        if (!fillsByDate[fillDate]) {
          fillsByDate[fillDate] = {};
        }
        
        if (!fillsByDate[fillDate][fill.plate]) {
          fillsByDate[fillDate][fill.plate] = {
            plate: fill.plate,
            fill_events: 0,
            total_filled: 0,
            fills: []
          };
        }
        
        fillsByDate[fillDate][fill.plate].fill_events++;
        fillsByDate[fillDate][fill.plate].total_filled += parseFloat(fill.fill_amount || 0);
        fillsByDate[fillDate][fill.plate].fills.push({
          time: fill.fill_date,
          amount: fill.fill_amount,
          method: fill.detection_method
        });
      });
      
      res.status(200).json({
        success: true,
        data: {
          period: {
            start_date,
            end_date
          },
          fills_by_date: fillsByDate,
          summary: {
            total_fill_events: fillData.length,
            total_fuel_filled: fillData.reduce((sum, f) => sum + parseFloat(f.fill_amount || 0), 0),
            unique_vehicles: new Set(fillData.map(f => f.plate)).size
          }
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching fills for reports:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

module.exports = new EnergyRiteFuelFillController();