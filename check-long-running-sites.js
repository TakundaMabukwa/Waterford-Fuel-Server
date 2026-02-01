require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkLongRunningSites() {
  try {
    console.log('⏰ Checking for long-running sites and top usage...\n');
    
    // Check for ongoing sessions longer than 24 hours
    const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
    
    const { data: longRunningSessions, error: longError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .lt('session_start_time', twentyFourHoursAgo.toISOString())
      .order('session_start_time', { ascending: true });
    
    if (longError) {
      console.error('❌ Long running error:', longError);
    } else {
      console.log(`🚨 Sites running longer than 24 hours: ${longRunningSessions.length}\n`);
      
      longRunningSessions.forEach((session, index) => {
        const startTime = new Date(session.session_start_time);
        const hoursRunning = (Date.now() - startTime.getTime()) / (1000 * 60 * 60);
        
        console.log(`⚠️  Long Running Site ${index + 1}:`);
        console.log(`   Branch: ${session.branch} (${session.cost_code})`);
        console.log(`   Started: ${session.session_start_time}`);
        console.log(`   Running for: ${hoursRunning.toFixed(1)} hours`);
        console.log(`   Opening Fuel: ${session.opening_fuel}L`);
        console.log('');
      });
    }
    
    // Get top 10 usage sites (last 30 days)
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    
    const { data: allSessions, error: usageError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('branch, cost_code, total_usage, operating_hours, cost_for_usage, session_date')
      .eq('session_status', 'COMPLETED')
      .gte('session_date', thirtyDaysAgo.toISOString().split('T')[0])
      .not('total_usage', 'is', null);
    
    if (usageError) {
      console.error('❌ Usage error:', usageError);
    } else {
      // Group by site and calculate totals
      const siteUsage = {};
      
      allSessions.forEach(session => {
        const site = session.branch;
        if (!siteUsage[site]) {
          siteUsage[site] = {
            branch: site,
            cost_code: session.cost_code,
            total_usage: 0,
            total_hours: 0,
            total_cost: 0,
            sessions: 0
          };
        }
        
        siteUsage[site].total_usage += parseFloat(session.total_usage || 0);
        siteUsage[site].total_hours += parseFloat(session.operating_hours || 0);
        siteUsage[site].total_cost += parseFloat(session.cost_for_usage || 0);
        siteUsage[site].sessions++;
      });
      
      // Sort by usage and get top 10
      const topUsageSites = Object.values(siteUsage)
        .sort((a, b) => b.total_usage - a.total_usage)
        .slice(0, 10);
      
      console.log(`🏆 Top 10 Usage Sites (Last 30 days):\n`);
      
      topUsageSites.forEach((site, index) => {
        const avgUsagePerHour = site.total_hours > 0 ? site.total_usage / site.total_hours : 0;
        
        console.log(`${index + 1}. ${site.branch} (${site.cost_code || 'N/A'})`);
        console.log(`   Total Usage: ${site.total_usage.toFixed(1)}L`);
        console.log(`   Total Hours: ${site.total_hours.toFixed(1)}h`);
        console.log(`   Sessions: ${site.sessions}`);
        console.log(`   Avg Usage/Hour: ${avgUsagePerHour.toFixed(2)}L/h`);
        console.log(`   Total Cost: R${site.total_cost.toFixed(2)}`);
        console.log('');
      });
    }
    
    // Check current ongoing sessions
    const { data: ongoingSessions, error: ongoingError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .order('session_start_time', { ascending: true });
    
    if (!ongoingError && ongoingSessions.length > 0) {
      console.log(`🔄 Currently Running Sessions: ${ongoingSessions.length}\n`);
      
      ongoingSessions.forEach((session, index) => {
        const startTime = new Date(session.session_start_time);
        const hoursRunning = (Date.now() - startTime.getTime()) / (1000 * 60 * 60);
        
        console.log(`🟢 Active Session ${index + 1}:`);
        console.log(`   Branch: ${session.branch}`);
        console.log(`   Running for: ${hoursRunning.toFixed(1)} hours`);
        console.log(`   Started: ${session.session_start_time}`);
        console.log('');
      });
    }
    
  } catch (error) {
    console.error('❌ Check error:', error);
  }
}

checkLongRunningSites();