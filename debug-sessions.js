const axios = require('axios');

async function debugSessions() {
  const baseUrl = 'http://localhost:4000';
  
  console.log('🔍 Debugging Session Management...\n');
  
  try {
    // Check recent sessions
    const sessionsResponse = await axios.get(`${baseUrl}/api/energy-rite/reports?days=1`);
    const sessions = sessionsResponse.data.data.sessions || [];
    
    console.log(`📊 Recent Sessions (last 24h): ${sessions.length}`);
    
    // Group by status
    const ongoing = sessions.filter(s => s.session_status === 'ONGOING');
    const completed = sessions.filter(s => s.session_status === 'COMPLETED');
    
    console.log(`🟢 Ongoing Sessions: ${ongoing.length}`);
    console.log(`✅ Completed Sessions: ${completed.length}\n`);
    
    // Show ongoing sessions
    if (ongoing.length > 0) {
      console.log('🟢 ONGOING SESSIONS:');
      ongoing.forEach(session => {
        const duration = new Date() - new Date(session.session_start_time);
        const hours = (duration / 3600000).toFixed(1);
        console.log(`  ${session.branch} - Started: ${session.session_start_time} (${hours}h ago)`);
      });
      console.log('');
    }
    
    // Show recent completed sessions
    if (completed.length > 0) {
      console.log('✅ RECENT COMPLETED SESSIONS:');
      completed.slice(0, 10).forEach(session => {
        console.log(`  ${session.branch} - ${session.operating_hours?.toFixed(1) || 0}h - ${session.total_usage?.toFixed(1) || 0}L`);
        console.log(`    Start: ${session.session_start_time}`);
        console.log(`    End: ${session.session_end_time}`);
      });
    }
    
    // Check for very short sessions (potential early closures)
    const shortSessions = completed.filter(s => (s.operating_hours || 0) < 0.5);
    if (shortSessions.length > 0) {
      console.log(`\n⚠️  SHORT SESSIONS (<30min): ${shortSessions.length}`);
      shortSessions.slice(0, 5).forEach(session => {
        console.log(`  ${session.branch} - ${(session.operating_hours * 60).toFixed(0)}min`);
      });
    }
    
    // Check for very long ongoing sessions (potential missed closures)
    const longOngoing = ongoing.filter(s => {
      const duration = new Date() - new Date(s.session_start_time);
      return duration > 12 * 3600000; // >12 hours
    });
    
    if (longOngoing.length > 0) {
      console.log(`\n⚠️  LONG ONGOING SESSIONS (>12h): ${longOngoing.length}`);
      longOngoing.forEach(session => {
        const duration = new Date() - new Date(session.session_start_time);
        const hours = (duration / 3600000).toFixed(1);
        console.log(`  ${session.branch} - ${hours}h ongoing`);
      });
    }
    
  } catch (error) {
    console.error('❌ Debug failed:', error.response?.data || error.message);
  }
}

debugSessions();