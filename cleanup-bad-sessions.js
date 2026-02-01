require('dotenv').config();
const { supabase } = require('./supabase-client');

async function cleanupBadSessions() {
  console.log('🧹 CLEANING UP PROBLEMATIC SESSIONS\n');
  
  try {
    // 1. Remove very short sessions (< 5 minutes)
    console.log('1. REMOVING VERY SHORT SESSIONS (<5min):');
    const { data: shortSessions, error: shortError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, branch, operating_hours')
      .eq('session_status', 'COMPLETED')
      .lt('operating_hours', 0.083); // 5 minutes = 0.083 hours
    
    if (shortError) throw shortError;
    
    if (shortSessions?.length > 0) {
      console.log(`   Found ${shortSessions.length} very short sessions to remove`);
      
      const { error: deleteError } = await supabase
        .from('energy_rite_operating_sessions')
        .delete()
        .in('id', shortSessions.map(s => s.id));
      
      if (deleteError) throw deleteError;
      console.log(`   ✅ Removed ${shortSessions.length} short sessions`);
    } else {
      console.log('   ✅ No very short sessions found');
    }
    
    // 2. Fix sessions with 0 operating hours but fuel usage
    console.log('\n2. FIXING ZERO-HOUR SESSIONS WITH FUEL USAGE:');
    const { data: zeroHourSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .eq('operating_hours', 0)
      .gt('total_usage', 0);
    
    if (zeroHourSessions?.length > 0) {
      console.log(`   Found ${zeroHourSessions.length} zero-hour sessions with fuel usage`);
      
      for (const session of zeroHourSessions) {
        // Estimate reasonable operating hours based on fuel usage
        const estimatedHours = Math.max(0.5, session.total_usage / 10); // 10L/hour average
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            operating_hours: estimatedHours,
            notes: `${session.notes || ''} | Fixed: Estimated ${estimatedHours.toFixed(1)}h based on fuel usage`
          })
          .eq('id', session.id);
      }
      
      console.log(`   ✅ Fixed ${zeroHourSessions.length} zero-hour sessions`);
    } else {
      console.log('   ✅ No zero-hour sessions with fuel usage found');
    }
    
    // 3. Close any remaining ongoing sessions older than 24 hours
    console.log('\n3. CLOSING OLD ONGOING SESSIONS:');
    const cutoffTime = new Date(Date.now() - 24 * 60 * 60 * 1000);
    
    const { data: oldOngoing } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .lt('session_start_time', cutoffTime.toISOString());
    
    if (oldOngoing?.length > 0) {
      console.log(`   Found ${oldOngoing.length} old ongoing sessions`);
      
      for (const session of oldOngoing) {
        const startTime = new Date(session.session_start_time);
        const estimatedEndTime = new Date(startTime.getTime() + 8 * 60 * 60 * 1000); // 8 hours
        const estimatedHours = 8;
        const estimatedUsage = estimatedHours * 10;
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            session_end_time: estimatedEndTime.toISOString(),
            operating_hours: estimatedHours,
            total_usage: estimatedUsage,
            cost_for_usage: estimatedUsage * 20,
            session_status: 'COMPLETED',
            notes: `Auto-completed old ongoing session (estimated ${estimatedHours}h)`
          })
          .eq('id', session.id);
      }
      
      console.log(`   ✅ Closed ${oldOngoing.length} old ongoing sessions`);
    } else {
      console.log('   ✅ No old ongoing sessions found');
    }
    
    // 4. Summary
    console.log('\n📊 CLEANUP SUMMARY:');
    const { data: finalStats } = await supabase
      .from('energy_rite_operating_sessions')
      .select('session_status')
      .gte('session_date', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]);
    
    const stats = finalStats?.reduce((acc, session) => {
      acc[session.session_status] = (acc[session.session_status] || 0) + 1;
      return acc;
    }, {});
    
    console.log('   Recent sessions (last 7 days):');
    Object.entries(stats || {}).forEach(([status, count]) => {
      console.log(`     ${status}: ${count}`);
    });
    
    console.log('\n✅ Session cleanup completed!');
    
  } catch (error) {
    console.error('❌ Cleanup failed:', error);
  }
}

cleanupBadSessions();