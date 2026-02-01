const { supabase } = require('../../supabase-client');

// Store active connections for real-time updates
const connections = new Map();

class EnergyRiteDataController {
  
  /**
   * Get all Energy Rite vehicles with optional filters (REAL-TIME)
   */
  async getAllVehicles(req, res) {
    // Check if client wants real-time updates
    const isRealtime = req.query.realtime === 'true' || req.headers.accept?.includes('text/event-stream');
    
    if (isRealtime) {
      return this.getAllVehiclesRealtime(req, res);
    }
    
    // Regular JSON response
    try {
      const { 
        limit = null,
        plate, 
        hasFuel = false, 
        isActive = null,
        orderBy = 'created_at',
        order = 'desc',
        branch,
        company
      } = req.query;
      
      let query = supabase
        .from('energy_rite_fuel_data')
        .select('*');
      
      // Add filters
      if (plate) {
        query = query.ilike('plate', `%${plate}%`);
      }
      
      if (branch) {
        query = query.ilike('plate', `%${branch}%`); // Note: branch info might be in plate or separate field
      }
      
      if (company) {
        query = query.ilike('plate', `%${company}%`); // Note: company info might be in plate or separate field
      }
      
      if (hasFuel === 'true') {
        query = query.not('fuel_probe_1_level', 'is', null);
      }
      
      // Order by
      query = query.order(orderBy, { ascending: order.toLowerCase() === 'asc' });
      
      // Add LIMIT only if specified
      if (limit !== null && limit !== undefined) {
        query = query.limit(parseInt(limit));
      }
      
      const { data, error } = await query;
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      res.status(200).json({
        success: true,
        data: data,
        count: data.length,
        filters: {
          limit: limit !== null ? parseInt(limit) : null,
          plate,
          branch,
          company,
          hasFuel: hasFuel === 'true',
          isActive: isActive !== null ? isActive === 'true' : null,
          orderBy,
          order
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching vehicles:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get all Energy Rite vehicles with real-time updates (Server-Sent Events)
   */
  async getAllVehiclesRealtime(req, res) {
    // Set up Server-Sent Events (SSE)
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Cache-Control',
    });

    const connectionId = Date.now().toString();
    
    // Store connection with filters
    const filters = {
      limit: req.query.limit ? parseInt(req.query.limit) : null,
      plate: req.query.plate,
      branch: req.query.branch,
      company: req.query.company,
      hasFuel: req.query.hasFuel === 'true',
      isActive: req.query.isActive !== null ? req.query.isActive === 'true' : null,
      orderBy: req.query.orderBy || 'created_at',
      order: req.query.order || 'desc'
    };
    
    connections.set(connectionId, { res, filters });

    // Set up Supabase real-time subscription
    const subscription = supabase
      .channel('energy_rite_fuel_data_changes')
      .on('postgres_changes', 
        { event: '*', schema: 'public', table: 'energy_rite_fuel_data' },
        (payload) => {
          try {
            console.log('Real-time notification:', payload.new?.plate);
            
            // Send individual vehicle update
            res.write(`data: ${JSON.stringify({
              type: 'vehicle_update',
              data: payload.new,
              timestamp: new Date().toISOString()
            })}\n\n`);
          } catch (error) {
            console.error('Error processing real-time notification:', error);
          }
        }
      )
      .subscribe();

    // Handle client disconnect
    req.on('close', () => {
      console.log(`Real-time vehicles connection closed: ${connectionId}`);
      connections.delete(connectionId);
      subscription.unsubscribe();
    });

    // Send initial data with filters applied
    try {
      let query = supabase
        .from('energy_rite_fuel_data')
        .select('*');
      
      // Apply same filters as regular endpoint
      if (filters.plate) {
        query = query.ilike('plate', `%${filters.plate}%`);
      }
      
      if (filters.branch) {
        query = query.ilike('plate', `%${filters.branch}%`);
      }
      
      if (filters.company) {
        query = query.ilike('plate', `%${filters.company}%`);
      }
      
      if (filters.hasFuel) {
        query = query.not('fuel_probe_1_level', 'is', null);
      }
      
      query = query.order(filters.orderBy, { ascending: filters.order.toLowerCase() === 'asc' });
      
      // Add LIMIT only if specified
      if (filters.limit !== null && filters.limit !== undefined) {
        query = query.limit(filters.limit);
      }
      
      const { data, error } = await query;
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      res.write(`data: ${JSON.stringify({
        type: 'initial',
        data: data,
        count: data.length,
        filters: filters,
        timestamp: new Date().toISOString()
      })}\n\n`);
      
      console.log(`Sent initial vehicles data: ${data.length} vehicles with filters`);
    } catch (error) {
      console.error('Error fetching initial vehicles data:', error);
      res.write(`data: ${JSON.stringify({
        type: 'error',
        message: error.message,
        timestamp: new Date().toISOString()
      })}\n\n`);
    }
  }

  /**
   * Get a specific vehicle by plate
   */
  async getVehicleByPlate(req, res) {
    try {
      const { plate } = req.params;
      
      const { data, error } = await supabase
        .from('energy_rite_fuel_data')
        .select('*')
        .eq('plate', plate)
        .order('created_at', { ascending: false })
        .limit(1);
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      if (data.length === 0) {
        return res.status(404).json({
          success: false,
          message: `Vehicle with plate ${plate} not found`,
          timestamp: new Date().toISOString()
        });
      }
      
      console.log(`Fetched vehicle: ${plate}`);
      
      res.status(200).json({
        success: true,
        data: data[0],
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error(`Error fetching vehicle ${req.params.plate}:`, error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get vehicles with fuel data
   */
  async getVehiclesWithFuel(req, res) {
    try {
      const { limit = 50, minLevel = 0, maxLevel = 1000 } = req.query;
      
      const { data, error } = await supabase
        .from('energy_rite_fuel_data')
        .select(`
          plate,
          fuel_probe_1_level,
          fuel_probe_1_volume_in_tank,
          fuel_probe_1_temperature,
          fuel_probe_1_level_percentage,
          latitude,
          longitude,
          address,
          created_at,
          message_date
        `)
        .not('fuel_probe_1_level', 'is', null)
        .gte('fuel_probe_1_level', parseFloat(minLevel))
        .lte('fuel_probe_1_level', parseFloat(maxLevel))
        .order('created_at', { ascending: false })
        .limit(parseInt(limit));
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      console.log(`Fetched ${data.length} vehicles with fuel data`);
      
      res.status(200).json({
        success: true,
        data: data,
        count: data.length,
        filters: {
          limit: parseInt(limit),
          minLevel: parseFloat(minLevel),
          maxLevel: parseFloat(maxLevel)
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching vehicles with fuel:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get dashboard statistics
   */
  async getDashboardStats(req, res) {
    try {
      // Get various statistics using Supabase queries
      const [
        totalVehiclesResult,
        vehiclesWithFuelResult,
        recentUpdatesResult,
        fuelStatsResult
      ] = await Promise.all([
        supabase.from('energy_rite_fuel_data').select('*', { count: 'exact', head: true }),
        supabase.from('energy_rite_fuel_data').select('*', { count: 'exact', head: true }).not('fuel_probe_1_level', 'is', null),
        supabase.from('energy_rite_fuel_data').select('*', { count: 'exact', head: true }).gte('created_at', new Date(Date.now() - 60 * 60 * 1000).toISOString()),
        supabase.from('energy_rite_fuel_data')
          .select('fuel_probe_1_level, fuel_probe_1_volume_in_tank, fuel_probe_1_temperature, fuel_probe_1_level_percentage')
          .not('fuel_probe_1_level', 'is', null)
      ]);

      // Calculate fuel statistics
      let fuelStats = {
        avgLevel: 0,
        avgVolume: 0,
        avgTemperature: 0,
        avgPercentage: 0,
        minLevel: 0,
        maxLevel: 0
      };

      if (fuelStatsResult.data && fuelStatsResult.data.length > 0) {
        const levels = fuelStatsResult.data.map(v => parseFloat(v.fuel_probe_1_level || 0));
        const volumes = fuelStatsResult.data.map(v => parseFloat(v.fuel_probe_1_volume_in_tank || 0));
        const temperatures = fuelStatsResult.data.map(v => parseFloat(v.fuel_probe_1_temperature || 0));
        const percentages = fuelStatsResult.data.map(v => parseFloat(v.fuel_probe_1_level_percentage || 0));

        fuelStats = {
          avgLevel: levels.reduce((a, b) => a + b, 0) / levels.length,
          avgVolume: volumes.reduce((a, b) => a + b, 0) / volumes.length,
          avgTemperature: temperatures.reduce((a, b) => a + b, 0) / temperatures.length,
          avgPercentage: percentages.reduce((a, b) => a + b, 0) / percentages.length,
          minLevel: Math.min(...levels),
          maxLevel: Math.max(...levels)
        };
      }

      console.log('Fetched dashboard statistics');
      
      res.status(200).json({
        success: true,
        stats: {
          totalVehicles: totalVehiclesResult.count || 0,
          vehiclesWithFuel: vehiclesWithFuelResult.count || 0,
          activeVehicles: totalVehiclesResult.count || 0, // Assuming all are active in fuel data
          recentUpdates: recentUpdatesResult.count || 0,
          theftAlerts: 0, // Will be implemented with anomalies table
          fuelStats: fuelStats,
          branchStats: [] // Will be implemented when branch data is available
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching dashboard stats:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get real-time updates via Server-Sent Events
   */
  async getRealTimeUpdates(req, res) {
    // Set up Server-Sent Events (SSE)
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Cache-Control',
    });

    const connectionId = Date.now().toString();
    
    // Store connection
    connections.set(connectionId, { res });

    // Set up Supabase real-time subscription
    const subscription = supabase
      .channel('fuel_data_updates')
      .on('postgres_changes', 
        { event: '*', schema: 'public', table: 'energy_rite_fuel_data' },
        (payload) => {
          try {
            console.log('Real-time notification:', payload.new?.plate);
            
            // Send data to client via SSE
            res.write(`data: ${JSON.stringify(payload.new)}\n\n`);
          } catch (error) {
            console.error('Error parsing notification:', error);
          }
        }
      )
      .subscribe();

    // Handle client disconnect
    req.on('close', () => {
      console.log(`Real-time connection closed: ${connectionId}`);
      connections.delete(connectionId);
      subscription.unsubscribe();
    });

    // Send initial data
    try {
      const { data, error } = await supabase
        .from('energy_rite_fuel_data')
        .select(`
          id,
          plate,
          fuel_probe_1_level,
          fuel_probe_1_volume_in_tank,
          fuel_probe_1_temperature,
          fuel_probe_1_level_percentage,
          latitude,
          longitude,
          created_at,
          message_date
        `)
        .not('fuel_probe_1_level', 'is', null)
        .order('created_at', { ascending: false })
        .limit(20);
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      res.write(`data: ${JSON.stringify({
        type: 'initial',
        data: data,
        timestamp: new Date().toISOString(),
        count: data.length
      })}\n\n`);
      
      console.log(`Sent initial data: ${data.length} vehicles`);
    } catch (error) {
      console.error('Error fetching initial data:', error);
      res.write(`data: ${JSON.stringify({
        type: 'error',
        message: error.message,
        timestamp: new Date().toISOString()
      })}\n\n`);
    }
  }

  /**
   * Get recent activity
   */
  async getRecentActivity(req, res) {
    try {
      const { hours = 24, limit = 50 } = req.query;
      
      const hoursAgo = new Date(Date.now() - parseInt(hours) * 60 * 60 * 1000).toISOString();
      
      const { data, error } = await supabase
        .from('energy_rite_fuel_data')
        .select(`
          plate,
          fuel_probe_1_level,
          fuel_probe_1_volume_in_tank,
          fuel_probe_1_temperature,
          fuel_probe_1_level_percentage,
          latitude,
          longitude,
          address,
          created_at,
          message_date
        `)
        .gte('created_at', hoursAgo)
        .order('created_at', { ascending: false })
        .limit(parseInt(limit));
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      console.log(`Fetched ${data.length} recent activities`);
      
      res.status(200).json({
        success: true,
        data: data,
        count: data.length,
        filters: {
          hours: parseInt(hours),
          limit: parseInt(limit)
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching recent activity:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }

  /**
   * Get fuel consumption analysis
   */
  async getFuelAnalysis(req, res) {
    try {
      const { days = 7 } = req.query;
      
      const daysAgo = new Date(Date.now() - parseInt(days) * 24 * 60 * 60 * 1000).toISOString();
      
      const { data, error } = await supabase
        .from('energy_rite_fuel_data')
        .select(`
          plate,
          fuel_probe_1_level,
          fuel_probe_1_volume_in_tank,
          fuel_probe_1_level_percentage,
          created_at,
          message_date
        `)
        .not('fuel_probe_1_level', 'is', null)
        .gte('created_at', daysAgo)
        .order('fuel_probe_1_level', { ascending: false });
      
      if (error) {
        throw new Error(`Database error: ${error.message}`);
      }
      
      console.log(`Fetched fuel analysis for ${data.length} vehicles`);
      
      res.status(200).json({
        success: true,
        data: data,
        count: data.length,
        filters: {
          days: parseInt(days)
        },
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error fetching fuel analysis:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  }
}

module.exports = new EnergyRiteDataController();