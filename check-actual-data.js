require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkActualData() {
  try {
    console.log('🔍 Checking actual data in database...\n');
    
    // Check operating sessions
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .order('created_at', { ascending: false });
    
    if (error) throw error;
    
    console.log('📊 OPERATING SESSIONS DATA:');
    console.log('===========================');
    console.log(`Total sessions: ${sessions.length}`);
    
    if (sessions.length > 0) {
      sessions.forEach((session, index) => {
        const created = new Date(session.created_at);
        const sessionStart = session.session_start ? new Date(session.session_start) : null;
        const sessionEnd = session.session_end ? new Date(session.session_end) : null;
        
        console.log(`\n${index + 1}. Session ID: ${session.id}`);
        console.log(`   Plate: ${session.plate || 'Unknown'}`);
        console.log(`   Created: ${created.toISOString()}`);
        console.log(`   Session Start: ${sessionStart ? sessionStart.toISOString() : 'N/A'}`);
        console.log(`   Session End: ${sessionEnd ? sessionEnd.toISOString() : 'N/A'}`);
        console.log(`   Duration: ${session.duration_minutes || 'N/A'} minutes`);
        
        if (sessionStart && sessionEnd) {
          const actualDuration = (sessionEnd - sessionStart) / (1000 * 60 * 60); // hours
          console.log(`   Calculated Duration: ${actualDuration.toFixed(2)} hours`);
        }
      });
    }
    
    // Check when database was created/first data
    const { data: fuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('created_at')
      .order('created_at', { ascending: true })
      .limit(1);
    
    if (!fuelError && fuelData.length > 0) {
      const firstRecord = new Date(fuelData[0].created_at);
      const now = new Date();
      const dbAge = (now - firstRecord) / (1000 * 60 * 60); // hours
      
      console.log(`\n📅 DATABASE AGE:');
      console.log(`First record: ${firstRecord.toISOString()}`);
      console.log(`Database age: ${dbAge.toFixed(2)} hours`);
      console.log(`Database age: ${(dbAge / 24).toFixed(2)} days`);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkActualData();