const axios = require('axios');

async function testUpdatedActivityReport() {
  const baseUrl = 'http://localhost:4000';
  
  console.log('🧪 Testing Updated Activity Report with Time Slots...\n');
  
  try {
    // Test main activity report
    console.log('📊 Testing Main Activity Report...');
    const response = await axios.get(`${baseUrl}/api/energy-rite/activity-reports`);
    
    if (response.data.success) {
      const data = response.data.data;
      console.log(`✅ Success! Found ${data.total_sites} sites`);
      console.log(`📅 Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`🏆 Overall Peak Time Slot: ${data.overall_peak_time_slot} (${data.overall_peak_usage.toFixed(1)}L)`);
      
      console.log('\n⏰ Time Slot Totals:');
      console.log(`  Morning: ${data.time_slot_totals.morning.toFixed(1)}L`);
      console.log(`  Afternoon: ${data.time_slot_totals.afternoon.toFixed(1)}L`);
      console.log(`  Evening: ${data.time_slot_totals.evening.toFixed(1)}L`);
      
      console.log('\n🏢 Top 5 Sites by Total Fuel:');
      data.site_reports.slice(0, 5).forEach((site, index) => {
        const totalFuel = site.morning_fuel + site.afternoon_fuel + site.evening_fuel;
        console.log(`  ${index + 1}. ${site.generator} (${site.cost_code})`);
        console.log(`     Morning: ${site.morning_fuel.toFixed(1)}L | Afternoon: ${site.afternoon_fuel.toFixed(1)}L | Evening: ${site.evening_fuel.toFixed(1)}L`);
        console.log(`     Peak: ${site.peak_time_slot} (${site.peak_fuel_usage.toFixed(1)}L) | Total: ${totalFuel.toFixed(1)}L`);
      });
      
      console.log('\n📈 Summary:');
      console.log(`  Total Sessions: ${data.summary.total_sessions}`);
      console.log(`  Total Operating Hours: ${data.summary.total_operating_hours.toFixed(1)}h`);
      console.log(`  Total Fuel Across All Slots: ${data.summary.total_fuel_across_slots.toFixed(1)}L`);
      
    } else {
      console.log('❌ Failed:', response.data.error);
    }
    
    // Test with cost code filter
    console.log('\n\n🔍 Testing with Cost Code Filter (cost code 1)...');
    const costCodeResponse = await axios.get(`${baseUrl}/api/energy-rite/activity-reports?costCode=1`);
    
    if (costCodeResponse.data.success) {
      const data = costCodeResponse.data.data;
      console.log(`✅ Success! Found ${data.total_sites} sites for cost code 1`);
      console.log(`🏆 Peak Time Slot: ${data.overall_peak_time_slot} (${data.overall_peak_usage.toFixed(1)}L)`);
      
      if (data.site_reports.length > 0) {
        console.log('\n🏢 Sites for Cost Code 1:');
        data.site_reports.slice(0, 3).forEach((site, index) => {
          const totalFuel = site.morning_fuel + site.afternoon_fuel + site.evening_fuel;
          console.log(`  ${index + 1}. ${site.generator} (${site.cost_code})`);
          console.log(`     M: ${site.morning_fuel.toFixed(1)}L | A: ${site.afternoon_fuel.toFixed(1)}L | E: ${site.evening_fuel.toFixed(1)}L | Peak: ${site.peak_time_slot}`);
        });
      }
    } else {
      console.log('❌ Failed:', costCodeResponse.data.error);
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.response?.data || error.message);
  }
}

testUpdatedActivityReport();