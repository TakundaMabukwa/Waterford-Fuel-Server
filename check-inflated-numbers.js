require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkInflatedNumbers() {
  console.log('🔍 CHECKING FOR INFLATED EXECUTIVE DASHBOARD NUMBERS\n');
  
  try {
    // Check recent sessions for anomalies
    const { data: sessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0])
      .eq('session_status', 'COMPLETED')
      .order('total_usage', { ascending: false });
    
    console.log(`📊 Analyzing ${sessions?.length || 0} recent sessions...\n`);
    
    // 1. Check for unrealistic fuel usage
    console.log('1. UNREALISTIC FUEL USAGE:');
    const highUsage = sessions?.filter(s => (s.total_usage || 0) > 100) || [];
    if (highUsage.length > 0) {
      console.log(`   ⚠️  Found ${highUsage.length} sessions with >100L usage:`);
      highUsage.slice(0, 5).forEach(s => {
        console.log(`     ${s.branch}: ${s.total_usage}L in ${s.operating_hours}h (${s.session_date})`);
      });
    } else {
      console.log('   ✅ No unrealistic fuel usage found');
    }
    
    // 2. Check for unrealistic operating hours
    console.log('\n2. UNREALISTIC OPERATING HOURS:');
    const longHours = sessions?.filter(s => (s.operating_hours || 0) > 24) || [];
    if (longHours.length > 0) {
      console.log(`   ⚠️  Found ${longHours.length} sessions with >24h:`);
      longHours.slice(0, 5).forEach(s => {
        console.log(`     ${s.branch}: ${s.operating_hours}h, ${s.total_usage}L (${s.session_date})`);
      });
    } else {
      console.log('   ✅ No unrealistic operating hours found');
    }
    
    // 3. Check for duplicate sessions (same site, same day, multiple sessions)
    console.log('\n3. POTENTIAL DUPLICATE SESSIONS:');
    const dailySessions = {};
    sessions?.forEach(s => {
      const key = `${s.branch}_${s.session_date}`;
      if (!dailySessions[key]) dailySessions[key] = [];
      dailySessions[key].push(s);
    });
    
    const duplicates = Object.entries(dailySessions).filter(([key, sessions]) => sessions.length > 3);
    if (duplicates.length > 0) {
      console.log(`   ⚠️  Found ${duplicates.length} days with >3 sessions:`);
      duplicates.slice(0, 5).forEach(([key, sessions]) => {
        const [site, date] = key.split('_');
        const totalUsage = sessions.reduce((sum, s) => sum + (s.total_usage || 0), 0);
        console.log(`     ${site} on ${date}: ${sessions.length} sessions, ${totalUsage.toFixed(1)}L total`);
      });
    } else {
      console.log('   ✅ No excessive daily sessions found');
    }
    
    // 4. Check for imported vs real-time data mixing
    console.log('\n4. DATA SOURCE ANALYSIS:');
    const imported = sessions?.filter(s => s.notes?.includes('Imported')) || [];
    const realtime = sessions?.filter(s => !s.notes?.includes('Imported')) || [];
    
    console.log(`   📥 Imported sessions: ${imported.length}`);
    console.log(`   🔄 Real-time sessions: ${realtime.length}`);
    
    if (imported.length > 0) {
      const importedUsage = imported.reduce((sum, s) => sum + (s.total_usage || 0), 0);
      const realtimeUsage = realtime.reduce((sum, s) => sum + (s.total_usage || 0), 0);
      console.log(`   📥 Imported total usage: ${importedUsage.toFixed(1)}L`);
      console.log(`   🔄 Real-time total usage: ${realtimeUsage.toFixed(1)}L`);
    }
    
    // 5. Calculate dashboard totals
    console.log('\n5. DASHBOARD TOTALS:');
    const totalHours = sessions?.reduce((sum, s) => sum + (s.operating_hours || 0), 0) || 0;
    const totalUsage = sessions?.reduce((sum, s) => sum + (s.total_usage || 0), 0) || 0;
    const totalCost = sessions?.reduce((sum, s) => sum + (s.cost_for_usage || 0), 0) || 0;
    
    console.log(`   ⏰ Total Operating Hours: ${totalHours.toFixed(1)}h`);
    console.log(`   ⛽ Total Fuel Usage: ${totalUsage.toFixed(1)}L`);
    console.log(`   💰 Total Cost: R${totalCost.toFixed(2)}`);
    console.log(`   📊 Average Usage/Hour: ${totalHours > 0 ? (totalUsage / totalHours).toFixed(1) : 0}L/h`);
    
    // 6. Recommendations
    console.log('\n💡 RECOMMENDATIONS:');
    if (highUsage.length > 0) {
      console.log('   🔧 Review sessions with >100L usage - may be data errors');
    }
    if (longHours.length > 0) {
      console.log('   🔧 Review sessions with >24h - likely orphaned sessions');
    }
    if (duplicates.length > 0) {
      console.log('   🔧 Check for duplicate session creation on same days');
    }
    if (imported.length > realtime.length) {
      console.log('   🔧 Most data is imported - may not reflect real operations');
    }
    
  } catch (error) {
    console.error('❌ Check failed:', error);
  }
}

checkInflatedNumbers();