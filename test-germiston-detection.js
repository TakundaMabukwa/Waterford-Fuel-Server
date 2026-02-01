require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testGermistonDetection() {
  console.log('🧪 TESTING GERMISTON ON/OFF DETECTION\n');
  
  try {
    // 1. Check current status
    console.log('1. CURRENT GERMISTON STATUS:');
    const { data: currentSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('branch', 'GERMISTON')
      .eq('session_status', 'ONGOING')
      .order('session_start_time', { ascending: false });
    
    if (currentSessions?.length > 0) {
      const session = currentSessions[0];
      const duration = (new Date() - new Date(session.session_start_time)) / 3600000;
      console.log(`   🟢 ONGOING SESSION: Started ${duration.toFixed(1)}h ago`);
      console.log(`   Opening Fuel: ${session.opening_fuel}L`);
    } else {
      console.log('   ⚪ No ongoing session - Generator is OFF');
    }
    
    // 2. Check recent activity logs
    console.log('\n2. RECENT GERMISTON ACTIVITY LOGS:');
    const { data: recentLogs } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .eq('branch', 'GERMISTON')
      .in('activity_type', ['ENGINE_ON', 'ENGINE_OFF'])
      .gte('created_at', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
      .order('created_at', { ascending: false })
      .limit(10);
    
    if (recentLogs?.length > 0) {
      console.log(`   Found ${recentLogs.length} recent events:`);
      recentLogs.forEach(log => {
        console.log(`   ${log.created_at}: ${log.activity_type}`);
      });
    } else {
      console.log('   ❌ No recent activity logs - Detection may not be working');
    }
    
    // 3. Check recent sessions
    console.log('\n3. RECENT GERMISTON SESSIONS:');
    const { data: recentSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('branch', 'GERMISTON')
      .gte('session_date', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0])
      .order('session_start_time', { ascending: false })
      .limit(5);
    
    if (recentSessions?.length > 0) {
      console.log(`   Found ${recentSessions.length} recent sessions:`);
      recentSessions.forEach(session => {
        console.log(`   ${session.session_date}: ${session.operating_hours?.toFixed(1) || 0}h - ${session.total_usage?.toFixed(1) || 0}L - ${session.session_status}`);
      });
    } else {
      console.log('   ❌ No recent sessions found');
    }
    
    // 4. Monitor for new events (run for 2 minutes)
    console.log('\n4. MONITORING FOR NEW EVENTS (2 minutes)...');
    console.log('   Simulate ON/OFF events for GERMISTON to test detection');
    
    let eventCount = 0;
    const startTime = Date.now();
    const monitorDuration = 2 * 60 * 1000; // 2 minutes
    
    const checkInterval = setInterval(async () => {
      try {
        const { data: newLogs } = await supabase
          .from('energy_rite_activity_log')
          .select('*')
          .eq('branch', 'GERMISTON')
          .in('activity_type', ['ENGINE_ON', 'ENGINE_OFF'])
          .gte('created_at', new Date(startTime).toISOString())
          .order('created_at', { ascending: false });
        
        if (newLogs?.length > eventCount) {
          const newEvents = newLogs.slice(0, newLogs.length - eventCount);
          newEvents.forEach(event => {
            console.log(`   🆕 NEW EVENT: ${event.created_at} - ${event.activity_type}`);
          });
          eventCount = newLogs.length;
        }
        
        if (Date.now() - startTime > monitorDuration) {
          clearInterval(checkInterval);
          console.log('\n✅ Monitoring completed');
          
          if (eventCount === 0) {
            console.log('❌ No new events detected during monitoring');
            console.log('💡 This suggests WebSocket connection or detection logic issues');
          } else {
            console.log(`✅ Detected ${eventCount} new events - Detection is working!`);
          }
        }
      } catch (error) {
        console.error('❌ Monitoring error:', error.message);
        clearInterval(checkInterval);
      }
    }, 5000); // Check every 5 seconds
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

testGermistonDetection();