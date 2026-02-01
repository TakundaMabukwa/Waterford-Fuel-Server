const { supabase } = require('./supabase-client');

async function testReportData() {
  try {
    console.log('Testing report data retrieval...');
    
    // Test date range (last 7 days)
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 6);
    
    console.log(`Date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);
    
    // Get operating sessions data
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_start_time', startDate.toISOString())
      .lte('session_start_time', endDate.toISOString())
      .order('session_start_time', { ascending: false });
    
    if (error) {
      console.error('Database error:', error);
      return;
    }
    
    console.log(`\nFound ${sessions.length} sessions:`);
    
    // Group by branch and show summary
    const siteGroups = {};
    sessions.forEach(session => {
      if (!siteGroups[session.branch]) {
        siteGroups[session.branch] = {
          branch: session.branch,
          sessions: [],
          total_operating_hours: 0,
          total_fuel_usage: 0,
          total_fuel_filled: 0,
          total_cost: 0
        };
      }
      
      const site = siteGroups[session.branch];
      site.sessions.push(session);
      site.total_operating_hours += parseFloat(session.operating_hours || 0);
      site.total_fuel_usage += parseFloat(session.total_usage || 0);
      site.total_fuel_filled += parseFloat(session.total_fill || 0);
      site.total_cost += parseFloat(session.cost_for_usage || 0);
    });
    
    // Display results
    Object.values(siteGroups).forEach(site => {
      console.log(`\n${site.branch}:`);
      console.log(`  Sessions: ${site.sessions.length}`);
      console.log(`  Operating Hours: ${site.total_operating_hours.toFixed(2)}`);
      console.log(`  Fuel Usage: ${site.total_fuel_usage.toFixed(2)} L`);
      console.log(`  Fuel Filled: ${site.total_fuel_filled.toFixed(2)} L`);
      console.log(`  Total Cost: R${site.total_cost.toFixed(2)}`);
      
      // Show individual sessions
      site.sessions.forEach(session => {
        console.log(`    ${session.session_date}: ${session.operating_hours}h, ${session.total_usage}L used, R${session.cost_for_usage}`);
      });
    });
    
    console.log(`\nTotal across all sites:`);
    console.log(`  Sites: ${Object.keys(siteGroups).length}`);
    console.log(`  Sessions: ${sessions.length}`);
    console.log(`  Operating Hours: ${Object.values(siteGroups).reduce((sum, site) => sum + site.total_operating_hours, 0).toFixed(2)}`);
    console.log(`  Fuel Usage: ${Object.values(siteGroups).reduce((sum, site) => sum + site.total_fuel_usage, 0).toFixed(2)} L`);
    console.log(`  Total Cost: R${Object.values(siteGroups).reduce((sum, site) => sum + site.total_cost, 0).toFixed(2)}`);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

testReportData();