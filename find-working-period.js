require('dotenv').config();
const { supabase } = require('./supabase-client');

async function findWorkingPeriod() {
  console.log('🔍 FINDING WHEN DETECTION WAS ACTUALLY WORKING\n');
  
  try {
    // Check sessions by date to find when it was working
    const { data: allSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('session_date, operating_hours, total_usage, session_status, notes')
      .gte('session_date', '2024-11-01')
      .order('session_date', { ascending: false });
    
    // Group by date and analyze
    const dailyStats = {};
    allSessions?.forEach(session => {
      const date = session.session_date;
      if (!dailyStats[date]) {
        dailyStats[date] = {
          count: 0,
          avgHours: 0,
          avgUsage: 0,
          shortSessions: 0,
          completed: 0,
          ongoing: 0
        };
      }
      
      const stats = dailyStats[date];
      stats.count++;
      stats.avgHours += session.operating_hours || 0;
      stats.avgUsage += session.total_usage || 0;
      
      if ((session.operating_hours || 0) < 0.5) stats.shortSessions++;
      if (session.session_status === 'COMPLETED') stats.completed++;
      if (session.session_status === 'ONGOING') stats.ongoing++;
    });
    
    // Calculate averages and find good days
    Object.keys(dailyStats).forEach(date => {
      const stats = dailyStats[date];
      stats.avgHours = stats.avgHours / stats.count;
      stats.avgUsage = stats.avgUsage / stats.count;
      stats.shortSessionPercent = (stats.shortSessions / stats.count) * 100;
    });
    
    console.log('📊 DAILY SESSION ANALYSIS (Recent to Oldest):');
    Object.entries(dailyStats)
      .sort(([a], [b]) => new Date(b) - new Date(a))
      .forEach(([date, stats]) => {
        const quality = stats.shortSessionPercent < 20 && stats.avgHours > 2 ? '✅ GOOD' : 
                       stats.shortSessionPercent > 50 ? '❌ BAD' : '⚠️  MIXED';
        
        console.log(`${date}: ${stats.count} sessions, ${stats.avgHours.toFixed(1)}h avg, ${stats.shortSessionPercent.toFixed(0)}% short ${quality}`);
      });
    
    // Find the best working day
    const goodDays = Object.entries(dailyStats)
      .filter(([date, stats]) => stats.shortSessionPercent < 20 && stats.avgHours > 2 && stats.count > 5)
      .sort(([,a], [,b]) => b.avgHours - a.avgHours);
    
    if (goodDays.length > 0) {
      const [bestDate, bestStats] = goodDays[0];
      console.log(`\n🏆 BEST WORKING DAY: ${bestDate}`);
      console.log(`   ${bestStats.count} sessions, ${bestStats.avgHours.toFixed(1)}h avg, ${bestStats.shortSessionPercent.toFixed(0)}% short`);
      
      // Get detailed sessions from best day
      const { data: bestDaySessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('session_date', bestDate)
        .order('session_start_time', { ascending: true });
      
      console.log('\n📋 SAMPLE SESSIONS FROM BEST DAY:');
      bestDaySessions?.slice(0, 5).forEach(session => {
        console.log(`  ${session.branch}: ${session.operating_hours?.toFixed(1)}h, ${session.total_usage?.toFixed(1)}L`);
        console.log(`    Notes: ${session.notes || 'None'}`);
      });
      
      // Check activity logs from that day
      const { data: bestDayLogs } = await supabase
        .from('energy_rite_activity_log')
        .select('*')
        .gte('created_at', `${bestDate}T00:00:00Z`)
        .lte('created_at', `${bestDate}T23:59:59Z`)
        .in('activity_type', ['ENGINE_ON', 'ENGINE_OFF'])
        .order('created_at', { ascending: true });
      
      console.log(`\n📋 ACTIVITY LOGS FROM BEST DAY: ${bestDayLogs?.length || 0} events`);
      if (bestDayLogs?.length > 0) {
        bestDayLogs.slice(0, 10).forEach(log => {
          console.log(`  ${log.created_at}: ${log.branch} - ${log.activity_type}`);
        });
      }
    } else {
      console.log('\n❌ No clearly good working days found in recent data');
    }
    
  } catch (error) {
    console.error('❌ Analysis failed:', error);
  }
}

findWorkingPeriod();