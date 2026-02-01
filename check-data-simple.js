require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkData() {
  try {
    console.log('🔍 Checking actual session data...\n');
    
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .order('created_at', { ascending: false });
    
    if (error) throw error;
    
    console.log(`📊 Total sessions: ${sessions.length}\n`);
    
    sessions.forEach((session, index) => {
      const created = new Date(session.created_at);
      const sessionStart = session.session_start ? new Date(session.session_start) : null;
      const sessionEnd = session.session_end ? new Date(session.session_end) : null;
      
      console.log(`${index + 1}. Session ${session.id}`);
      console.log(`   Plate: ${session.plate || 'Unknown'}`);
      console.log(`   Created: ${created.toLocaleString()}`);
      console.log(`   Start: ${sessionStart ? sessionStart.toLocaleString() : 'N/A'}`);
      console.log(`   End: ${sessionEnd ? sessionEnd.toLocaleString() : 'N/A'}`);
      
      if (sessionStart && sessionEnd) {
        const duration = (sessionEnd - sessionStart) / (1000 * 60 * 60);
        console.log(`   Duration: ${duration.toFixed(2)} hours`);
      } else if (sessionStart) {
        const duration = (new Date() - sessionStart) / (1000 * 60 * 60);
        console.log(`   Running for: ${duration.toFixed(2)} hours`);
      }
      console.log('');
    });
    
    // Check database age
    const { data: firstRecord } = await supabase
      .from('energy_rite_fuel_data')
      .select('created_at')
      .order('created_at', { ascending: true })
      .limit(1);
    
    if (firstRecord && firstRecord.length > 0) {
      const dbAge = (new Date() - new Date(firstRecord[0].created_at)) / (1000 * 60 * 60);
      console.log(`📅 Database age: ${dbAge.toFixed(2)} hours (${(dbAge/24).toFixed(2)} days)`);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkData();