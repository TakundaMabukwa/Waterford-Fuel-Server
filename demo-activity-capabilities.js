require('dotenv').config();
const axios = require('axios');

async function demonstrateActivityReports() {
  console.log('🎯 ACTIVITY REPORTS DEMONSTRATION');
  console.log('=' .repeat(60));
  console.log('Showing how to get operational hours, fuel usage, and fuel fills\n');
  
  try {
    // 1. Get today's activity for all sites
    console.log('1️⃣ TODAY\'S OPERATIONAL OVERVIEW');
    console.log('-'.repeat(40));
    
    const today = new Date().toISOString().split('T')[0];
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { date: today }
    });
    
    if (response.data.success) {
      const data = response.data.data;
      
      console.log(`📅 Date: ${data.date}`);
      console.log(`🏢 Active Sites: ${data.summary.total_sites}`);
      console.log(`📊 Total Sessions: ${data.summary.total_sessions}`);
      console.log(`⏰ Total Operating Hours: ${data.summary.total_operating_hours.toFixed(2)}h`);
      console.log(`⛽ Total Fuel Usage: ${data.summary.total_fuel_usage.toFixed(2)}L`);
      console.log(`💰 Total Cost: R${data.summary.total_cost}`);
      
      // Show operational details for top 5 most active sites
      console.log('\n📋 TOP 5 MOST ACTIVE SITES:');
      const activeSites = data.sites
        .filter(site => site.total_operating_hours > 0)
        .sort((a, b) => b.total_operating_hours - a.total_operating_hours)
        .slice(0, 5);
      
      activeSites.forEach((site, index) => {
        console.log(`\n  ${index + 1}. ${site.branch} (${site.cost_code})`);
        console.log(`     ⏰ Operating Hours: ${site.total_operating_hours.toFixed(2)}h`);
        console.log(`     ⛽ Fuel Usage: ${site.total_fuel_usage.toFixed(2)}L`);
        console.log(`     💰 Cost: R${site.total_cost}`);
        console.log(`     📊 Sessions: ${site.session_count}`);
        
        if (site.sessions && site.sessions.length > 0) {
          console.log(`     🕐 Session Times:`);
          site.sessions.forEach((session, sIndex) => {
            const start = new Date(session.start_time).toLocaleTimeString();
            const end = session.end_time ? new Date(session.end_time).toLocaleTimeString() : 'ongoing';
            console.log(`       ${sIndex + 1}. ${start} - ${end} (${session.duration_hours}h)`);
          });
        }
      });
    }
    
    // 2. Show fuel fill detection
    console.log('\n\n2️⃣ FUEL FILL ANALYSIS');
    console.log('-'.repeat(40));
    
    if (response.data.data.fuel_analysis.fuel_fills) {
      const fuelFills = response.data.data.fuel_analysis.fuel_fills;
      console.log(`⛽ Total Fill Events: ${fuelFills.total_fill_events}`);
      console.log(`🚛 Total Fuel Filled: ${fuelFills.total_fuel_filled}L`);
      
      if (Object.keys(fuelFills.fills_by_vehicle).length > 0) {
        console.log('\n📋 FUEL FILLS BY VEHICLE:');
        Object.entries(fuelFills.fills_by_vehicle).forEach(([vehicle, amount]) => {
          console.log(`   ${vehicle}: ${amount}L`);
        });
      } else {
        console.log('   ✅ No fuel fills detected today');
      }
    }
    
    // 3. Show time period breakdown
    console.log('\n\n3️⃣ TIME PERIOD BREAKDOWN');
    console.log('-'.repeat(40));
    
    if (response.data.data.time_periods) {
      const periods = response.data.data.time_periods;
      
      Object.entries(periods).forEach(([period, data]) => {
        console.log(`\n🕐 ${data.name}:`);
        console.log(`   ⛽ Fuel Consumption: ${data.fuel_consumption.usage.toFixed(2)}L`);
        
        if (data.data && data.data.vehicles_breakdown) {
          const activeVehicles = data.data.vehicles_breakdown.filter(v => v.is_active).length;
          console.log(`   🚗 Active Vehicles: ${activeVehicles}/${data.data.total_vehicles}`);
          console.log(`   📊 Utilization: ${data.data.utilization_rate}%`);
        }
      });
    }
    
    // 4. Test specific site query
    console.log('\n\n4️⃣ SPECIFIC SITE EXAMPLE (SITE-B)');
    console.log('-'.repeat(40));
    
    const siteResponse = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: today,
        site_id: 'SITE-B'
      }
    });
    
    if (siteResponse.data.success && siteResponse.data.data.sites.length > 0) {
      const site = siteResponse.data.data.sites[0];
      console.log(`🏢 Site: ${site.branch}`);
      console.log(`⏰ Operating Hours: ${site.total_operating_hours}h`);
      console.log(`⛽ Fuel Usage: ${site.total_fuel_usage}L`);
      console.log(`💰 Cost: R${site.total_cost}`);
      console.log(`📊 Sessions: ${site.session_count}`);
    } else {
      console.log('   ℹ️ No data for SITE-B today');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

demonstrateActivityReports();