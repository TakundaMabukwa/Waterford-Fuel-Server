const { supabase } = require('./supabase-client');

class ImprovedOnOffDetection {
  constructor() {
    this.sessionCache = new Map(); // Track recent sessions to prevent duplicates
    this.debounceTime = 5 * 60 * 1000; // 5 minutes debounce
    this.minSessionDuration = 15 * 60 * 1000; // 15 minutes minimum
  }

  parseEngineStatus(driverName) {
    if (!driverName || driverName.trim() === '') return null;
    
    const normalized = driverName.replace(/\s+/g, ' ').trim().toUpperCase();
    
    // Enhanced pattern matching
    const onPatterns = [
      'PTO ON', 'ENGINE ON', 'GENERATOR ON', 'GEN ON',
      'START', 'STARTED', 'RUNNING', 'ACTIVE',
      'POWER ON', 'SWITCHED ON'
    ];
    
    const offPatterns = [
      'PTO OFF', 'ENGINE OFF', 'GENERATOR OFF', 'GEN OFF',
      'STOP', 'STOPPED', 'IDLE', 'INACTIVE',
      'POWER OFF', 'SWITCHED OFF', 'SHUTDOWN'
    ];
    
    // Check for ON patterns
    if (onPatterns.some(pattern => normalized.includes(pattern))) {
      return 'ON';
    }
    
    // Check for OFF patterns  
    if (offPatterns.some(pattern => normalized.includes(pattern))) {
      return 'OFF';
    }
    
    return null;
  }

  async handleEngineOn(plate, fuelData) {
    try {
      const now = new Date();
      const cacheKey = `${plate}_ON`;
      
      // Check debounce - prevent multiple ON events within 5 minutes
      if (this.sessionCache.has(cacheKey)) {
        const lastEvent = this.sessionCache.get(cacheKey);
        if (now - lastEvent < this.debounceTime) {
          console.log(`🔄 Debounced ON event for ${plate}`);
          return;
        }
      }
      
      // Check if session already exists
      const { data: existing } = await supabase
        .from('energy_rite_operating_sessions')
        .select('id, session_start_time')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .limit(1);
        
      if (existing?.length > 0) {
        console.log(`⚠️  Session already exists for ${plate}`);
        return;
      }
      
      // Create new session
      const openingFuel = parseFloat(fuelData?.fuel_probe_1_level) || 0;
      const openingPercentage = parseFloat(fuelData?.fuel_probe_1_level_percentage) || 0;
      
      const { data: session } = await supabase
        .from('energy_rite_operating_sessions')
        .insert({
          branch: plate,
          company: fuelData?.company || 'KFC',
          cost_code: fuelData?.cost_code,
          session_date: now.toISOString().split('T')[0],
          session_start_time: now.toISOString(),
          opening_fuel: openingFuel,
          opening_percentage: openingPercentage,
          session_status: 'ONGOING',
          notes: `Engine ON detected - Opening: ${openingFuel}L (${openingPercentage}%)`
        })
        .select()
        .single();
      
      // Log activity
      await supabase.from('energy_rite_activity_log').insert({
        activity_type: 'ENGINE_ON',
        description: `Engine ON detected for ${plate}`,
        branch: plate,
        activity_data: { 
          engine_status: 'ON',
          opening_fuel: openingFuel,
          timestamp: now.toISOString()
        }
      });
      
      this.sessionCache.set(cacheKey, now);
      console.log(`🟢 Engine ON: ${plate} - Session ${session.id} started`);
      
    } catch (error) {
      console.error(`❌ Error handling engine ON for ${plate}:`, error.message);
    }
  }

  async handleEngineOff(plate, fuelData) {
    try {
      const now = new Date();
      const cacheKey = `${plate}_OFF`;
      
      // Check debounce
      if (this.sessionCache.has(cacheKey)) {
        const lastEvent = this.sessionCache.get(cacheKey);
        if (now - lastEvent < this.debounceTime) {
          console.log(`🔄 Debounced OFF event for ${plate}`);
          return;
        }
      }
      
      // Find ongoing session
      const { data: sessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('branch', plate)
        .eq('session_status', 'ONGOING')
        .order('session_start_time', { ascending: false })
        .limit(1);
        
      if (!sessions?.length) {
        console.log(`⚠️  No ongoing session found for ${plate}`);
        return;
      }
      
      const session = sessions[0];
      const startTime = new Date(session.session_start_time);
      const duration = now - startTime;
      
      // Check minimum duration - prevent very short sessions
      if (duration < this.minSessionDuration) {
        console.log(`⚠️  Session too short for ${plate} (${Math.round(duration/60000)}min) - ignoring OFF`);
        return;
      }
      
      const operatingHours = duration / 3600000;
      const closingFuel = parseFloat(fuelData?.fuel_probe_1_level) || 0;
      const closingPercentage = parseFloat(fuelData?.fuel_probe_1_level_percentage) || 0;
      const fuelUsed = Math.max(0, session.opening_fuel - closingFuel);
      const fuelCost = fuelUsed * 20; // R20 per liter
      
      // Update session
      await supabase
        .from('energy_rite_operating_sessions')
        .update({
          session_end_time: now.toISOString(),
          operating_hours: operatingHours,
          closing_fuel: closingFuel,
          closing_percentage: closingPercentage,
          total_usage: fuelUsed,
          cost_for_usage: fuelCost,
          session_status: 'COMPLETED',
          notes: `Engine OFF detected - Duration: ${operatingHours.toFixed(2)}h, Used: ${fuelUsed.toFixed(1)}L`
        })
        .eq('id', session.id);
      
      // Log activity
      await supabase.from('energy_rite_activity_log').insert({
        activity_type: 'ENGINE_OFF',
        description: `Engine OFF detected for ${plate}`,
        branch: plate,
        activity_data: { 
          engine_status: 'OFF',
          session_id: session.id,
          duration_hours: operatingHours,
          fuel_used: fuelUsed,
          timestamp: now.toISOString()
        }
      });
      
      this.sessionCache.set(cacheKey, now);
      console.log(`🔴 Engine OFF: ${plate} - Session ${session.id} completed (${operatingHours.toFixed(2)}h, ${fuelUsed.toFixed(1)}L)`);
      
    } catch (error) {
      console.error(`❌ Error handling engine OFF for ${plate}:`, error.message);
    }
  }

  async cleanupOrphanedSessions() {
    try {
      const cutoffTime = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hours ago
      
      const { data: orphaned } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_status', 'ONGOING')
        .lt('session_start_time', cutoffTime.toISOString());
      
      if (orphaned?.length > 0) {
        console.log(`🧹 Cleaning up ${orphaned.length} orphaned sessions`);
        
        for (const session of orphaned) {
          const estimatedHours = 8; // Assume 8-hour session
          const estimatedUsage = estimatedHours * 10; // 10L/hour
          
          await supabase
            .from('energy_rite_operating_sessions')
            .update({
              session_end_time: new Date(new Date(session.session_start_time).getTime() + estimatedHours * 3600000).toISOString(),
              operating_hours: estimatedHours,
              total_usage: estimatedUsage,
              cost_for_usage: estimatedUsage * 20,
              session_status: 'COMPLETED',
              notes: `Auto-completed orphaned session (estimated ${estimatedHours}h)`
            })
            .eq('id', session.id);
        }
      }
      
      return orphaned?.length || 0;
    } catch (error) {
      console.error('❌ Error cleaning orphaned sessions:', error);
      return 0;
    }
  }
}

module.exports = ImprovedOnOffDetection;