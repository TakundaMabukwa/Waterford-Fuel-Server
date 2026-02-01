require('dotenv').config();
const { supabase } = require('./supabase-client');

async function diagnoseOnOffDetection() {
  console.log('🔍 DIAGNOSING ON/OFF DETECTION ISSUES\n');
  
  try {
    // 1. Check recent activity logs
    console.log('📋 1. CHECKING ACTIVITY LOGS:');
    const { data: activityLogs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .in('activity_type', ['ENGINE_ON', 'ENGINE_OFF'])
      .order('created_at', { ascending: false })
      .limit(20);
    
    if (activityLogs?.length > 0) {
      console.log(`   Found ${activityLogs.length} recent engine events:`);
      activityLogs.forEach(log => {
        console.log(`   ${log.created_at}: ${log.branch} - ${log.activity_type}`);
      });
    } else {
      console.log('   ❌ NO ENGINE EVENTS FOUND - This indicates detection is not working');
    }
    
    // 2. Check ongoing sessions
    console.log('\n📊 2. CHECKING ONGOING SESSIONS:');
    const { data: ongoingSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .order('session_start_time', { ascending: false });
    
    if (ongoingSessions?.length > 0) {
      console.log(`   Found ${ongoingSessions.length} ongoing sessions:`);
      ongoingSessions.forEach(session => {
        const duration = (new Date() - new Date(session.session_start_time)) / 3600000;
        console.log(`   ${session.branch}: ${duration.toFixed(1)}h ongoing`);
        if (duration > 12) {
          console.log('     ⚠️  LONG SESSION - Possible missed OFF detection');
        }
      });
    } else {
      console.log('   ✅ No ongoing sessions');
    }
    
    // 3. Check recent completed sessions
    console.log('\n✅ 3. CHECKING RECENT COMPLETED SESSIONS:');
    const { data: recentSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .gte('session_date', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0])
      .order('session_start_time', { ascending: false })
      .limit(10);
    
    if (recentSessions?.length > 0) {
      console.log(`   Found ${recentSessions.length} recent completed sessions:`);
      recentSessions.forEach(session => {
        console.log(`   ${session.branch}: ${session.operating_hours?.toFixed(1) || 0}h - ${session.total_usage?.toFixed(1) || 0}L`);
      });
    } else {
      console.log('   ❌ NO RECENT COMPLETED SESSIONS - Detection may not be working');
    }
    
    // 4. Check for very short sessions (false triggers)
    console.log('\n⚠️  4. CHECKING FOR SHORT SESSIONS (<30min):');
    const { data: shortSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .lt('operating_hours', 0.5)
      .gte('session_date', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0])
      .order('session_start_time', { ascending: false })
      .limit(10);
    
    if (shortSessions?.length > 0) {
      console.log(`   Found ${shortSessions.length} short sessions (possible false triggers):`);
      shortSessions.forEach(session => {
        const minutes = (session.operating_hours * 60).toFixed(0);
        console.log(`   ${session.branch}: ${minutes}min - ${session.notes || 'No notes'}`);
      });
    } else {
      console.log('   ✅ No unusually short sessions');
    }
    
    // 5. Analyze detection patterns
    console.log('\n📈 5. DETECTION PATTERN ANALYSIS:');
    const { data: dailyStats } = await supabase
      .from('energy_rite_operating_sessions')
      .select('session_date, session_status')
      .gte('session_date', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]);
    
    const dailyBreakdown = {};
    dailyStats?.forEach(session => {
      const date = session.session_date;
      if (!dailyBreakdown[date]) {
        dailyBreakdown[date] = { ongoing: 0, completed: 0 };
      }
      dailyBreakdown[date][session.session_status.toLowerCase()]++;
    });
    
    Object.entries(dailyBreakdown).forEach(([date, stats]) => {
      console.log(`   ${date}: ${stats.completed} completed, ${stats.ongoing} ongoing`);
    });
    
    // 6. Recommendations
    console.log('\n💡 RECOMMENDATIONS:');
    
    if (!activityLogs || activityLogs.length === 0) {
      console.log('   🔧 WebSocket connection may be down - check server logs');
      console.log('   🔧 DriverName parsing may be failing - check message format');
    }
    
    const longOngoing = ongoingSessions?.filter(s => {
      const duration = (new Date() - new Date(s.session_start_time)) / 3600000;
      return duration > 12;
    }) || [];
    
    if (longOngoing.length > 0) {
      console.log('   🔧 Run session cleanup to close orphaned sessions');
      console.log('   🔧 Check OFF detection patterns - may be missing OFF signals');
    }
    
    if (shortSessions?.length > 5) {
      console.log('   🔧 Too many short sessions - may need debouncing logic');
      console.log('   🔧 Consider minimum session duration threshold');
    }
    
  } catch (error) {
    console.error('❌ Diagnosis failed:', error);
  }
}

diagnoseOnOffDetection();