require('dotenv').config();
const axios = require('axios');

async function getActivityReportToday() {
  console.log('📊 Activity Report for All Sites with Sessions Today\n');
  console.log('=' .repeat(60));
  
  try {
    const today = new Date().toISOString().split('T')[0];
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { date: today }
    });
    
    const data = response.data.data;
    
    console.log(`📅 Date: ${data.date}`);
    console.log(`🏪 Sites with Sessions: ${data.summary.total_sites}`);
    console.log(`📊 Total Sessions: ${data.summary.total_sessions}`);
    console.log(`⏱️ Total Operating Hours: ${data.summary.total_operating_hours}`);
    console.log(`⛽ Total Fuel Usage: ${data.summary.total_fuel_usage}L`);
    console.log(`💰 Total Cost: R${data.summary.total_cost}`);
    
    console.log('\n⛽ FUEL ANALYSIS');
    console.log('-'.repeat(40));
    console.log(`Daily Total Consumption: ${data.fuel_analysis.daily_total_consumption}L`);
    console.log(`Peak Usage Period: ${data.fuel_analysis.peak_usage_period.name} (${data.fuel_analysis.peak_usage_period.usage.toFixed(2)}L)`);
    console.log(`Peak Usage Site: ${data.fuel_analysis.peak_usage_site.site || 'None'} (${data.fuel_analysis.peak_usage_site.usage.toFixed(2)}L)`);
    
    console.log('\n📊 PERIOD BREAKDOWN');
    console.log('-'.repeat(40));
    console.log(`🌅 Morning (6AM-12PM): ${data.fuel_analysis.period_breakdown.morning}L`);
    console.log(`☀️ Afternoon (12PM-5PM): ${data.fuel_analysis.period_breakdown.afternoon}L`);
    console.log(`🌙 Evening (5PM-11PM): ${data.fuel_analysis.period_breakdown.evening}L`);
    
    console.log('\n🕐 TIME PERIOD SNAPSHOTS');
    console.log('-'.repeat(40));
    Object.entries(data.time_periods).forEach(([period, periodData]) => {
      console.log(`\n${periodData.name}:`);
      if (periodData.data) {
        console.log(`   📸 Snapshot: ${new Date(periodData.data.snapshot_time).toLocaleString()}`);
        console.log(`   🚗 Active: ${periodData.data.active_vehicles}/${periodData.data.total_vehicles} (${periodData.data.utilization_rate}%)`);
        console.log(`   ⛽ Avg Fuel: ${periodData.data.average_fuel_percentage}%`);
      } else {
        console.log('   ⚠️ No snapshot data available');
      }
      if (periodData.fuel_consumption) {
        console.log(`   🔥 Consumption: ${periodData.fuel_consumption.usage.toFixed(2)}L`);
      }
    });
    
    console.log('\n🏪 SITES WITH SESSIONS');
    console.log('-'.repeat(40));
    data.sites.forEach(site => {
      if (site.session_count > 0) {
        console.log(`\n${site.branch} (${site.cost_code}):`);
        console.log(`   📊 Sessions: ${site.session_count}`);
        console.log(`   ⏱️ Operating Hours: ${site.total_operating_hours}`);
        console.log(`   ⛽ Fuel Usage: ${site.total_fuel_usage}L`);
        console.log(`   💰 Cost: R${site.total_cost}`);
        
        if (site.sessions && site.sessions.length > 0) {
          console.log('   📋 Session Details:');
          site.sessions.forEach((session, index) => {
            const start = new Date(session.start_time).toLocaleTimeString();
            const end = session.end_time ? new Date(session.end_time).toLocaleTimeString() : 'ongoing';
            console.log(`     ${index + 1}. ${start} - ${end} (${session.duration_hours}h)`);
          });
        }
      }
    });
    
    console.log('\n' + '='.repeat(60));
    console.log('✅ Activity Report Complete');
    
  } catch (error) {
    console.error('❌ Failed to get activity report:', error.response?.data || error.message);
  }
}

getActivityReportToday();