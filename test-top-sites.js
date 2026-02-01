const axios = require('axios');

async function testTopSites() {
  try {
    console.log('🔥 Testing Enhanced Executive Dashboard - Top 10 Sites by Fuel Usage\n');
    
    const response = await axios.get('http://localhost:4000/api/energy-rite/enhanced-executive-dashboard?period=30');
    
    if (response.data.success) {
      const { top_performing_sites, key_metrics } = response.data.data;
      
      console.log('📊 CUMULATIVE METRICS (30 days):');
      console.log(`   Total Sites: ${key_metrics.total_sites_operated}`);
      console.log(`   Total Fuel Used: ${key_metrics.total_litres_used}L`);
      console.log(`   Total Hours: ${key_metrics.total_operational_hours}h`);
      console.log('');
      
      console.log('🏆 TOP 10 SITES BY FUEL USAGE (Cumulative 30 days):');
      console.log('─'.repeat(120));
      console.log('Rank | Site Name          | Cost Code                  | Sessions | Hours  | Fuel Used | Net Usage | Efficiency');
      console.log('─'.repeat(120));
      
      top_performing_sites.forEach((site, index) => {
        console.log(
          `${(index + 1).toString().padStart(4)} | ` +
          `${site.site.padEnd(18)} | ` +
          `${(site.cost_code || 'UNKNOWN').padEnd(26)} | ` +
          `${site.sessions.toString().padStart(8)} | ` +
          `${site.operating_hours.toFixed(1).padStart(6)} | ` +
          `${site.fuel_usage.toFixed(1).padStart(9)}L | ` +
          `${site.net_fuel_usage.toFixed(1).padStart(9)}L | ` +
          `${site.efficiency.toFixed(2).padStart(10)}L/h`
        );
      });
      
      console.log('─'.repeat(120));
      console.log('');
      console.log('🎯 KEY INSIGHTS:');
      console.log(`   • Highest consumer: ${top_performing_sites[0].site} (${top_performing_sites[0].fuel_usage.toFixed(1)}L)`);
      console.log(`   • Most efficient: ${top_performing_sites.find(s => s.efficiency > 0 && s.efficiency === Math.min(...top_performing_sites.filter(s => s.efficiency > 0).map(s => s.efficiency)))?.site || 'N/A'}`);
      console.log(`   • Most sessions: ${top_performing_sites.find(s => s.sessions === Math.max(...top_performing_sites.map(s => s.sessions)))?.site || 'N/A'} (${Math.max(...top_performing_sites.map(s => s.sessions))} sessions)`);
      
    } else {
      console.error('❌ Failed to fetch dashboard data');
    }
    
  } catch (error) {
    console.error('❌ Error testing top sites:', error.message);
  }
}

testTopSites();