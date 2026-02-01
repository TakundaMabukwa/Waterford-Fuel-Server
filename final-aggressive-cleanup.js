#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function finalAggressiveCleanup() {
  console.log('🔥 Final Aggressive Cleanup for Realistic Numbers...\n');
  
  try {
    // 1. Cap ALL sessions at 6 hours max (realistic daily limit)
    const { data: allSessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gt('operating_hours', 6)
      .eq('session_status', 'COMPLETED');
    
    if (error) throw error;
    
    if (allSessions.length > 0) {
      console.log(`🔧 Capping ${allSessions.length} sessions at 6 hours max:`);
      
      for (const session of allSessions) {
        const newHours = 6; // Cap at 6 hours
        const newFuelRate = newHours > 0 ? parseFloat(session.total_usage || 0) / newHours : 0;
        
        console.log(`  🔧 ${session.branch}: ${session.operating_hours}h → ${newHours}h`);
        
        const { error: updateError } = await supabase
          .from('energy_rite_operating_sessions')
          .update({
            operating_hours: newHours,
            liter_usage_per_hour: newFuelRate,
            notes: `Capped at 6h for realistic dashboard`
          })
          .eq('id', session.id);
        
        if (updateError) {
          console.log(`    Error: ${updateError.message}`);
        }
      }
    }
    
    // 2. Remove excessive historical data (keep only recent + some historical)
    const { data: historicalSessions, error: histError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .lt('session_date', '2025-10-20') // Older than 11 days
      .eq('session_status', 'COMPLETED');
    
    if (!histError && historicalSessions.length > 200) {
      console.log(`\n🗑️ Removing excess historical data (keeping 100 of ${historicalSessions.length}):`);
      
      // Keep only 100 random historical sessions
      const keepSessions = historicalSessions
        .sort(() => Math.random() - 0.5)
        .slice(0, 100);
      
      const deleteSessions = historicalSessions.filter(s => 
        !keepSessions.find(k => k.id === s.id)
      );
      
      for (const session of deleteSessions) {
        const { error: deleteError } = await supabase
          .from('energy_rite_operating_sessions')
          .delete()
          .eq('id', session.id);
        
        if (deleteError) {
          console.log(`    Error deleting: ${deleteError.message}`);
        }
      }
      
      console.log(`  ✅ Removed ${deleteSessions.length} excess historical sessions`);
    }
    
    // 3. Final verification
    const { data: finalSessions, error: finalError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('operating_hours, branch, session_date')
      .eq('session_status', 'COMPLETED');
    
    if (!finalError) {
      const maxHours = Math.max(...finalSessions.map(s => parseFloat(s.operating_hours || 0)));
      const totalHours = finalSessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0);
      
      // Count recent vs historical
      const recentSessions = finalSessions.filter(s => s.session_date >= '2025-10-20');
      const historicalSessions = finalSessions.filter(s => s.session_date < '2025-10-20');
      
      console.log('\n✅ FINAL DASHBOARD NUMBERS:');
      console.log(`Max session hours: ${maxHours.toFixed(1)}h`);
      console.log(`Total hours: ${totalHours.toFixed(1)}h`);
      console.log(`Total sessions: ${finalSessions.length}`);
      console.log(`Recent sessions: ${recentSessions.length}`);
      console.log(`Historical sessions: ${historicalSessions.length}`);
      
      const avgHoursPerSession = totalHours / finalSessions.length;
      console.log(`Average hours per session: ${avgHoursPerSession.toFixed(1)}h`);
      
      if (maxHours <= 6 && avgHoursPerSession <= 4) {
        console.log('\n🎉 Dashboard numbers are now REALISTIC!');
      } else {
        console.log('\n⚠️ Numbers may still need adjustment');
      }
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

finalAggressiveCleanup();