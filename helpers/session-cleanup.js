const { supabase } = require('../supabase-client');

/**
 * Cleanup orphaned sessions that are stuck in ONGOING status
 */
async function cleanupOrphanedSessions() {
  try {
    // Find sessions that have been ongoing for more than 24 hours
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    
    const { data: orphanedSessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .lt('session_start_time', yesterday);
      
    if (error) throw error;
    
    if (orphanedSessions.length > 0) {
      console.log(`🧹 Found ${orphanedSessions.length} orphaned sessions to cleanup`);
      
      // Mark them as completed with estimated data
      for (const session of orphanedSessions) {
        const startTime = new Date(session.session_start_time);
        const estimatedEndTime = new Date(startTime.getTime() + 8 * 60 * 60 * 1000); // Assume 8 hours
        const estimatedHours = 8;
        const estimatedUsage = estimatedHours * 10; // Assume 10L/hour
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            session_end_time: estimatedEndTime.toISOString(),
            operating_hours: estimatedHours,
            total_usage: estimatedUsage,
            cost_for_usage: estimatedUsage * 20,
            session_status: 'COMPLETED',
            notes: `Auto-completed orphaned session - estimated data (cleanup)`
          })
          .eq('id', session.id);
          
        console.log(`✅ Cleaned up session ${session.id} for ${session.branch}`);
      }
    }
    
    return { cleaned: orphanedSessions.length };
  } catch (error) {
    console.error('❌ Error cleaning up sessions:', error);
    return { error: error.message };
  }
}

module.exports = { cleanupOrphanedSessions };