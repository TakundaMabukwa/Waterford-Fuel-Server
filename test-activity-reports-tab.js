const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testActivityReportsTab() {
  console.log('📊 Testing Activity Reports Tab Endpoints\n');

  try {
    // Test 1: Main Activity Report
    console.log('1️⃣ Testing main activity report...\n');
    
    const response1 = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports?days=7`);
    
    if (response1.data.success) {
      const data = response1.data.data;
      
      console.log('✅ Main Activity Report:');
      console.log('='.repeat(50));
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`🏢 Total Sites: ${data.total_sites}`);
      console.log(`📈 Total Sessions: ${data.summary.total_sessions}`);
      console.log(`⏰ Total Hours: ${data.summary.total_operating_hours.toFixed(1)}h`);
      console.log(`⛽ Total Fuel Usage: ${data.summary.total_fuel_usage.toFixed(1)}L`);
      console.log(`💰 Total Cost: R${data.summary.total_cost.toFixed(2)}`);
      
      if (data.generator_reports.length > 0) {
        console.log('\n🏭 Top Generators:');
        data.generator_reports.slice(0, 3).forEach((gen, index) => {
          console.log(`   ${index + 1}. ${gen.generator}: ${gen.total_sessions} sessions, ${gen.total_operating_hours.toFixed(1)}h`);
        });
      }
      
      if (data.peak_usage_analysis.peak_usage_hours.length > 0) {
        console.log('\n🏆 Peak Usage Hours:');
        data.peak_usage_analysis.peak_usage_hours.slice(0, 3).forEach(hour => {
          console.log(`   Hour ${hour.hour}: ${hour.total_usage.toFixed(1)}L (${hour.sessions} sessions)`);
        });
      }
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 2: Peak Usage Analysis
    console.log('2️⃣ Testing peak usage analysis...\n');
    
    const response2 = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports/peak-usage?days=30`);
    
    if (response2.data.success) {
      const data = response2.data.data;
      
      console.log('✅ Peak Usage Analysis:');
      console.log('='.repeat(50));
      console.log(`📊 Analysis Period: ${data.analysis_period}`);
      console.log(`📈 Peak Usage Days: ${data.peak_usage_days.length}`);
      
      if (data.highest_usage_day) {
        console.log(`🏆 Highest Usage Day: ${data.highest_usage_day.date}`);
        console.log(`   Usage: ${data.highest_usage_day.total_usage.toFixed(1)}L`);
        console.log(`   Sites: ${data.highest_usage_day.sites.length}`);
      }
      
      console.log('\n📅 Top Peak Days:');
      data.peak_usage_days.slice(0, 5).forEach((day, index) => {
        console.log(`   ${index + 1}. ${day.date}: ${day.total_usage.toFixed(1)}L (${day.sites.length} sites)`);
      });
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 3: Site Comparison
    console.log('3️⃣ Testing site comparison...\n');
    
    const response3 = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports/site-comparison?days=30`);
    
    if (response3.data.success) {
      const data = response3.data.data;
      
      console.log('✅ Site Comparison:');
      console.log('='.repeat(50));
      console.log(`📊 Comparison Period: ${data.comparison_period}`);
      console.log(`🏢 Sites Analyzed: ${data.site_comparison.length}`);
      
      if (data.top_consumer) {
        console.log(`🏆 Top Consumer: ${data.top_consumer.site}`);
        console.log(`   Usage: ${data.top_consumer.total_usage.toFixed(1)}L`);
        console.log(`   Cost: R${data.top_consumer.total_cost.toFixed(2)}`);
      }
      
      if (data.most_efficient) {
        console.log(`⚡ Most Efficient: ${data.most_efficient.site}`);
        console.log(`   Efficiency: ${data.most_efficient.efficiency_score.toFixed(1)}L/h`);
      }
      
      console.log('\n🏭 Top Sites by Usage:');
      data.site_comparison.slice(0, 5).forEach((site, index) => {
        console.log(`   ${index + 1}. ${site.site}: ${site.total_usage.toFixed(1)}L, ${site.total_hours.toFixed(1)}h`);
      });
    }

    console.log('\n' + '='.repeat(60) + '\n');

    // Test 4: Activity Dashboard (Snapshots)
    console.log('4️⃣ Testing activity dashboard (snapshots)...\n');
    
    const response4 = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports/dashboard?days=7`);
    
    if (response4.data.success) {
      const data = response4.data.data;
      
      console.log('✅ Activity Dashboard (Snapshots):');
      console.log('='.repeat(50));
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`📸 Total Snapshots: ${data.summary.total_snapshots}`);
      console.log(`📊 Days Analyzed: ${data.summary.total_days}`);
      console.log(`🏆 Peak Time Slot: ${data.summary.overall_peak_time_slot}`);
      console.log(`🌆 Evening Peak Days: ${data.summary.evening_peak_days}`);
      
      console.log('\n⏰ Time Slot Analysis:');
      console.log(`   🌅 Morning: ${data.time_slot_analysis.morning.total_activity} total`);
      console.log(`   ☀️ Afternoon: ${data.time_slot_analysis.afternoon.total_activity} total`);
      console.log(`   🌆 Evening: ${data.time_slot_analysis.evening.total_activity} total`);
      
      console.log(`\n💼 Cost Codes Tracked: ${Object.keys(data.cost_code_patterns).length}`);
      console.log(`🏢 Sites Tracked: ${Object.keys(data.site_utilization).length}`);
    }

    console.log('\n🏁 Activity Reports Tab Testing Complete!');

  } catch (error) {
    console.log('❌ Error:', error.response?.data?.error || error.message);
  }
}

testActivityReportsTab().catch(console.error);