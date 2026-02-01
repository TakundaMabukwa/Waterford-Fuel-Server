const { supabase } = require('./supabase-client');

class EnhancedSessionManager {
  
  async startSession(plate, initialFuelLevel, initialPercentage) {
    try {
      const { data, error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert({
          plate,
          session_start: new Date().toISOString(),
          opening_fuel: initialFuelLevel,
          opening_percentage: initialPercentage,
          session_status: 'ONGOING',
          session_data: {
            start_fuel_level: initialFuelLevel,
            start_percentage: initialPercentage,
            start_timestamp: new Date().toISOString()
          }
        })
        .select()
        .single();
      
      if (error) throw error;
      
      console.log(`✅ Session started for ${plate}: ${initialFuelLevel}L (${initialPercentage}%)`);
      return data;
      
    } catch (error) {
      console.error('❌ Error starting session:', error);
      throw error;
    }
  }
  
  async endSession(sessionId, finalFuelLevel, finalPercentage) {
    try {
      const { data: session, error: fetchError } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('id', sessionId)
        .single();
      
      if (fetchError) throw fetchError;
      
      const startTime = new Date(session.session_start);
      const endTime = new Date();
      const durationMinutes = Math.round((endTime - startTime) / (1000 * 60));
      const operatingHours = durationMinutes / 60;
      
      const fuelUsed = Math.max(0, session.opening_fuel - finalFuelLevel);
      const fuelFilled = Math.max(0, finalFuelLevel - session.opening_fuel);
      
      const { data, error } = await supabase
        .from('energy_rite_operating_sessions')
        .update({
          session_end: endTime.toISOString(),
          closing_fuel: finalFuelLevel,
          closing_percentage: finalPercentage,
          duration_minutes: durationMinutes,
          operating_hours: operatingHours,
          fuel_consumed: fuelUsed,
          total_usage: fuelUsed,
          total_fill: fuelFilled,
          session_status: 'COMPLETED',
          session_data: {
            ...session.session_data,
            end_fuel_level: finalFuelLevel,
            end_percentage: finalPercentage,
            end_timestamp: endTime.toISOString(),
            calculated_usage: fuelUsed,
            calculated_fill: fuelFilled
          }
        })
        .eq('id', sessionId)
        .select()
        .single();
      
      if (error) throw error;
      
      console.log(`✅ Session ended for ${session.plate}: ${finalFuelLevel}L (${finalPercentage}%)`);
      return data;
      
    } catch (error) {
      console.error('❌ Error ending session:', error);
      throw error;
    }
  }
}

module.exports = new EnhancedSessionManager();