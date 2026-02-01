require('dotenv').config();
const { supabase } = require('./supabase-client');

async function fixInflatedHours() {
  try {
    console.log('🔧 Fixing inflated operating hours...');
    
    // Get sessions with unrealistic hours (>18 hours)
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gt('operating_hours', 18);

    if (error) throw error;

    console.log(`📊 Found ${sessions.length} sessions with inflated hours`);

    let fixed = 0;
    for (const session of sessions) {
      let newHours = session.operating_hours;
      
      // Cap at 18 hours max
      if (newHours > 18) {
        newHours = Math.min(newHours / 2, 18); // Divide by 2 or cap at 18
      }
      
      // Recalculate usage per hour
      const newUsagePerHour = newHours > 0 ? session.total_usage / newHours : 0;
      
      const { error: updateError } = await supabase
        .from('energy_rite_operating_sessions')
        .update({
          operating_hours: Math.round(newHours * 100) / 100,
          liter_usage_per_hour: Math.round(newUsagePerHour * 100) / 100
        })
        .eq('id', session.id);

      if (!updateError) fixed++;
    }

    console.log(`✅ Fixed ${fixed} sessions`);
  } catch (error) {
    console.error('❌ Fix failed:', error.message);
  }
}

fixInflatedHours();