require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkNov11Sessions() {
  console.log('🔍 ANALYZING NOVEMBER 11TH SESSIONS (WHEN IT WAS WORKING)\n');
  
  try {
    // Check sessions from Nov 11th
    const { data: nov11Sessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_date', '2024-11-11')
      .order('session_start_time', { ascending: true });
    
    console.log(`📊 Found ${nov11Sessions?.length || 0} sessions on 2024-11-11:`);
    
    if (nov11Sessions?.length > 0) {
      console.log('\n✅ WORKING SESSIONS FROM NOV 11TH:');
      nov11Sessions.forEach(session => {
        console.log(`${session.branch}:`);
        console.log(`  Start: ${session.session_start_time}`);
        console.log(`  End: ${session.session_end_time}`);
        console.log(`  Duration: ${session.operating_hours?.toFixed(2) || 0}h`);
        console.log(`  Usage: ${session.total_usage?.toFixed(1) || 0}L`);
        console.log(`  Status: ${session.session_status}`);
        console.log(`  Notes: ${session.notes || 'None'}`);
        console.log('');
      });
      
      // Analyze patterns
      const avgDuration = nov11Sessions.reduce((sum, s) => sum + (s.operating_hours || 0), 0) / nov11Sessions.length;
      const avgUsage = nov11Sessions.reduce((sum, s) => sum + (s.total_usage || 0), 0) / nov11Sessions.length;
      const shortSessions = nov11Sessions.filter(s => (s.operating_hours || 0) < 0.5);
      
      console.log('📈 NOV 11TH ANALYSIS:');
      console.log(`  Average Duration: ${avgDuration.toFixed(2)}h`);
      console.log(`  Average Usage: ${avgUsage.toFixed(1)}L`);
      console.log(`  Short Sessions (<30min): ${shortSessions.length}`);
      console.log(`  Completed Sessions: ${nov11Sessions.filter(s => s.session_status === 'COMPLETED').length}`);
      console.log(`  Ongoing Sessions: ${nov11Sessions.filter(s => s.session_status === 'ONGOING').length}`);
    }
    
    // Compare with recent sessions
    console.log('\n🔄 COMPARING WITH RECENT SESSIONS:');
    const { data: recentSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', '2024-11-08')
      .lte('session_date', '2024-11-08')
      .order('session_start_time', { ascending: true });
    
    if (recentSessions?.length > 0) {
      const recentAvgDuration = recentSessions.reduce((sum, s) => sum + (s.operating_hours || 0), 0) / recentSessions.length;
      const recentAvgUsage = recentSessions.reduce((sum, s) => sum + (s.total_usage || 0), 0) / recentSessions.length;
      const recentShortSessions = recentSessions.filter(s => (s.operating_hours || 0) < 0.5);
      
      console.log(`📊 Recent Sessions (Nov 8th): ${recentSessions.length}`);
      console.log(`  Average Duration: ${recentAvgDuration.toFixed(2)}h`);
      console.log(`  Average Usage: ${recentAvgUsage.toFixed(1)}L`);
      console.log(`  Short Sessions: ${recentShortSessions.length}`);
    }
    
    // Check activity logs around Nov 11th
    console.log('\n📋 CHECKING ACTIVITY LOGS AROUND NOV 11TH:');
    const { data: activityLogs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .gte('created_at', '2024-11-11T00:00:00Z')
      .lte('created_at', '2024-11-11T23:59:59Z')
      .in('activity_type', ['ENGINE_ON', 'ENGINE_OFF'])
      .order('created_at', { ascending: true });
    
    if (activityLogs?.length > 0) {
      console.log(`  Found ${activityLogs.length} engine events on Nov 11th:`);
      activityLogs.slice(0, 10).forEach(log => {
        console.log(`    ${log.created_at}: ${log.branch} - ${log.activity_type}`);
      });
    } else {
      console.log('  ❌ No engine activity logs found for Nov 11th');
    }
    
  } catch (error) {
    console.error('❌ Analysis failed:', error);
  }
}

checkNov11Sessions();