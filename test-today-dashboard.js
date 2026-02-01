const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testTodayDashboard() {
  console.log('📊 Testing Today\'s Activity Dashboard\n');

  try {
    const today = new Date().toISOString().split('T')[0];
    console.log(`📅 Getting dashboard for: ${today}\n`);

    const response = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports/dashboard?startDate=${today}&endDate=${today}`);
    
    if (response.data.success) {
      const data = response.data.data;
      
      console.log('✅ Dashboard Response:');
      console.log('='.repeat(50));
      console.log(`📊 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`📈 Total Snapshots: ${data.summary.total_snapshots}`);
      console.log(`📅 Days Analyzed: ${data.summary.total_days}`);
      console.log(`🏆 Peak Time Slot: ${data.summary.overall_peak_time_slot}`);
      console.log(`🌅 Evening Peak Days: ${data.summary.evening_peak_days}`);
      console.log(`📊 Evening Peak %: ${data.summary.evening_peak_percentage.toFixed(1)}%`);
      
      console.log('\n📸 Daily Snapshots:');
      data.daily_snapshots.forEach(day => {
        console.log(`\n📅 ${day.date}:`);
        if (day.morning) console.log(`  🌅 Morning: ${day.morning.active_vehicles}/${day.morning.total_vehicles} (${day.morning.activity_percentage.toFixed(1)}%)`);
        if (day.afternoon) console.log(`  ☀️ Afternoon: ${day.afternoon.active_vehicles}/${day.afternoon.total_vehicles} (${day.afternoon.activity_percentage.toFixed(1)}%)`);
        if (day.evening) console.log(`  🌆 Evening: ${day.evening.active_vehicles}/${day.evening.total_vehicles} (${day.evening.activity_percentage.toFixed(1)}%)`);
        console.log(`  🏆 Peak: ${day.peak_slot} (${day.peak_activity} vehicles)`);
      });
      
      console.log('\n⏰ Time Slot Analysis:');
      console.log(`🌅 Morning: ${data.time_slot_analysis.morning.total_activity} total, ${data.time_slot_analysis.morning.avg_activity.toFixed(1)} avg`);
      console.log(`☀️ Afternoon: ${data.time_slot_analysis.afternoon.total_activity} total, ${data.time_slot_analysis.afternoon.avg_activity.toFixed(1)} avg`);
      console.log(`🌆 Evening: ${data.time_slot_analysis.evening.total_activity} total, ${data.time_slot_analysis.evening.avg_activity.toFixed(1)} avg`);
      
      console.log('\n💼 Cost Code Patterns:');
      Object.entries(data.cost_code_patterns).forEach(([code, pattern]) => {
        console.log(`  ${code}: Morning(${pattern.morning}) Afternoon(${pattern.afternoon}) Evening(${pattern.evening})`);
      });
      
      console.log('\n🏢 Site Utilization (Top 10):');
      Object.entries(data.site_utilization)
        .sort(([,a], [,b]) => b.utilization_rate - a.utilization_rate)
        .slice(0, 10)
        .forEach(([site, util]) => {
          console.log(`  ${site}: ${util.utilization_rate.toFixed(1)}% (Peak: ${util.peak_time_slot})`);
        });
        
    } else {
      console.log('❌ Dashboard failed:', response.data.error);
    }

  } catch (error) {
    console.log('❌ Dashboard error:', error.response?.data?.error || error.message);
  }
}

testTodayDashboard().catch(console.error);