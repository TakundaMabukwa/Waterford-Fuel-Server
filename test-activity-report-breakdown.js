const axios = require('axios');

async function testActivityReportBreakdown() {
  const baseUrl = 'http://localhost:4000';
  
  console.log('🧪 Testing Activity Report with Session Breakdown...\n');
  
  try {
    // Test activity report
    const response = await axios.get(`${baseUrl}/api/energy-rite/reports/activity`);
    
    if (response.data.success) {
      const data = response.data.data;
      console.log(`✅ Activity Report Generated`);
      console.log(`📅 Date: ${data.date}`);
      console.log(`📊 Summary: ${data.summary.total_sites} sites, ${data.summary.total_sessions} sessions\n`);
      
      // Find sites with multiple sessions
      const sitesWithMultipleSessions = data.sites.filter(site => site.has_multiple_sessions);
      const sitesWithSingleSession = data.sites.filter(site => !site.has_multiple_sessions && site.session_count > 0);
      
      console.log(`🔢 Sites with Multiple Sessions: ${sitesWithMultipleSessions.length}`);
      console.log(`🔢 Sites with Single Session: ${sitesWithSingleSession.length}\n`);
      
      // Show examples of expandable sites
      if (sitesWithMultipleSessions.length > 0) {
        console.log('📋 EXPANDABLE SITES (Multiple Sessions):\n');
        sitesWithMultipleSessions.slice(0, 3).forEach((site, index) => {
          console.log(`${index + 1}. ${site.branch} (${site.cost_code})`);
          console.log(`   🔄 Expandable: ${site.expandable ? 'YES' : 'NO'}`);
          console.log(`   📊 Sessions: ${site.session_count}`);
          console.log(`   ⏱️  Total Hours: ${site.total_operating_hours.toFixed(2)}h`);
          console.log(`   ⛽ Total Usage: ${site.total_fuel_usage.toFixed(1)}L`);
          console.log(`   💰 Total Cost: R${site.total_cost.toFixed(2)}`);
          
          console.log('   📝 Session Breakdown:');
          site.sessions.forEach((session, sessionIndex) => {
            const duration = session.duration_hours;
            const hours = Math.floor(duration);
            const minutes = Math.round((duration - hours) * 60);
            
            console.log(`     ${sessionIndex + 1}. ${hours}h ${minutes}m - ${session.fuel_usage.toFixed(1)}L - R${session.cost.toFixed(2)}`);
            console.log(`        Opening: ${session.opening_fuel.toFixed(1)}L (${session.opening_percentage}%)`);
            console.log(`        Closing: ${session.closing_fuel.toFixed(1)}L (${session.closing_percentage}%)`);
          });
          console.log('');
        });
      }
      
      // Show examples of single session sites
      if (sitesWithSingleSession.length > 0) {
        console.log('📋 SINGLE SESSION SITES:\n');
        sitesWithSingleSession.slice(0, 3).forEach((site, index) => {
          console.log(`${index + 1}. ${site.branch} (${site.cost_code})`);
          console.log(`   🔄 Expandable: ${site.expandable ? 'YES' : 'NO'}`);
          console.log(`   📊 Sessions: ${site.session_count}`);
          console.log(`   ⏱️  Hours: ${site.total_operating_hours.toFixed(2)}h`);
          console.log(`   ⛽ Usage: ${site.total_fuel_usage.toFixed(1)}L`);
          console.log(`   💰 Cost: R${site.total_cost.toFixed(2)}`);
          console.log('');
        });
      }
      
      // Summary statistics
      console.log('📈 BREAKDOWN STATISTICS:');
      console.log(`   Total Sites: ${data.sites.length}`);
      console.log(`   Sites with Multiple Sessions: ${sitesWithMultipleSessions.length}`);
      console.log(`   Sites with Single Session: ${sitesWithSingleSession.length}`);
      console.log(`   Sites with No Sessions: ${data.sites.length - sitesWithMultipleSessions.length - sitesWithSingleSession.length}`);
      
    } else {
      console.log('❌ Failed:', response.data.error);
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.response?.data || error.message);
  }
}

testActivityReportBreakdown();