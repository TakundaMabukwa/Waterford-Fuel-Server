require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkCalculationLogic() {
  console.log('🔍 CHECKING CALCULATION LOGIC\n');
  
  try {
    // Get sample sessions to analyze calculation logic
    const { data: sessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .order('created_at', { ascending: false })
      .limit(10);
    
    console.log('📊 ANALYZING CALCULATION LOGIC:\n');
    
    sessions?.forEach((session, i) => {
      console.log(`${i + 1}. ${session.branch} (${session.session_date})`);
      
      // Check operating hours calculation
      if (session.session_start_time && session.session_end_time) {
        const start = new Date(session.session_start_time);
        const end = new Date(session.session_end_time);
        const calculatedHours = (end - start) / (1000 * 60 * 60);
        const storedHours = session.operating_hours || 0;
        
        console.log(`   ⏰ Operating Hours:`);
        console.log(`      Start: ${session.session_start_time}`);
        console.log(`      End: ${session.session_end_time}`);
        console.log(`      Calculated: ${calculatedHours.toFixed(2)}h`);
        console.log(`      Stored: ${storedHours}h`);
        console.log(`      Match: ${Math.abs(calculatedHours - storedHours) < 0.1 ? '✅' : '❌'}`);
      }
      
      // Check fuel usage calculation
      const openingFuel = session.opening_fuel || 0;
      const closingFuel = session.closing_fuel || 0;
      const calculatedUsage = Math.max(0, openingFuel - closingFuel);
      const storedUsage = session.total_usage || 0;
      
      console.log(`   ⛽ Fuel Usage:`);
      console.log(`      Opening: ${openingFuel}L`);
      console.log(`      Closing: ${closingFuel}L`);
      console.log(`      Calculated: ${calculatedUsage.toFixed(2)}L`);
      console.log(`      Stored: ${storedUsage}L`);
      console.log(`      Match: ${Math.abs(calculatedUsage - storedUsage) < 0.1 ? '✅' : '❌'}`);
      
      // Check if values are realistic
      const hoursRealistic = (session.operating_hours || 0) <= 24;
      const usageRealistic = (session.total_usage || 0) <= 100;
      const rateRealistic = session.operating_hours > 0 ? (session.total_usage / session.operating_hours) <= 50 : true;
      
      console.log(`   🔍 Realistic Check:`);
      console.log(`      Hours ≤24h: ${hoursRealistic ? '✅' : '❌'}`);
      console.log(`      Usage ≤100L: ${usageRealistic ? '✅' : '❌'}`);
      console.log(`      Rate ≤50L/h: ${rateRealistic ? '✅' : '❌'}`);
      
      console.log('');
    });
    
    // Check for calculation inconsistencies
    console.log('🔧 CALCULATION ISSUES FOUND:');
    
    const { data: badHours } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, branch, session_start_time, session_end_time, operating_hours')
      .eq('session_status', 'COMPLETED')
      .not('session_start_time', 'is', null)
      .not('session_end_time', 'is', null);
    
    let hoursMismatches = 0;
    badHours?.forEach(session => {
      const start = new Date(session.session_start_time);
      const end = new Date(session.session_end_time);
      const calculatedHours = (end - start) / (1000 * 60 * 60);
      const storedHours = session.operating_hours || 0;
      
      if (Math.abs(calculatedHours - storedHours) > 0.1) {
        hoursMismatches++;
      }
    });
    
    const { data: badUsage } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, branch, opening_fuel, closing_fuel, total_usage')
      .eq('session_status', 'COMPLETED')
      .not('opening_fuel', 'is', null)
      .not('closing_fuel', 'is', null);
    
    let usageMismatches = 0;
    badUsage?.forEach(session => {
      const calculatedUsage = Math.max(0, (session.opening_fuel || 0) - (session.closing_fuel || 0));
      const storedUsage = session.total_usage || 0;
      
      if (Math.abs(calculatedUsage - storedUsage) > 0.1) {
        usageMismatches++;
      }
    });
    
    console.log(`❌ Operating Hours Mismatches: ${hoursMismatches}/${badHours?.length || 0}`);
    console.log(`❌ Fuel Usage Mismatches: ${usageMismatches}/${badUsage?.length || 0}`);
    
  } catch (error) {
    console.error('❌ Check failed:', error);
  }
}

checkCalculationLogic();