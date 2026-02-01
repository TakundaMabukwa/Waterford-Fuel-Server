const axios = require('axios');

async function testActivityEndpoint() {
  try {
    console.log('🧪 Testing Activity Report Endpoint...\n');
    
    // Test the main activity report endpoint
    const response = await axios.get('http://localhost:4000/api/energy-rite/activity-reports', {
      params: {
        startDate: '2025-01-01',
        endDate: '2025-01-31'
      }
    });
    
    const data = response.data;
    
    console.log('✅ Response Status:', response.status);
    console.log('📊 Activity Report Data:\n');
    
    if (data.success) {
      const reportData = data.data;
      
      console.log('📅 Period:', reportData.period);
      console.log('🏭 Total Sites:', reportData.total_sites);
      console.log('⏰ Peak Time Slot:', reportData.overall_peak_time_slot);
      console.log('⛽ Peak Usage:', reportData.overall_peak_usage);
      
      console.log('\n📈 Summary:');
      console.log('  Total Sessions:', reportData.summary.total_sessions);
      console.log('  Total Operating Hours:', reportData.summary.total_operating_hours);
      console.log('  Morning Usage:', reportData.summary.total_morning_to_afternoon_usage);
      console.log('  Afternoon Usage:', reportData.summary.total_afternoon_to_evening_usage);
      console.log('  Full Day Usage:', reportData.summary.total_full_day_usage);
      
      console.log('\n🏭 Site Reports:');
      reportData.site_reports.slice(0, 5).forEach(site => {
        console.log(`  ${site.generator} (${site.cost_code}):`);
        console.log(`    Operating Hours: ${site.total_operating_hours}h`);
        console.log(`    Fuel Usage: ${site.total_fuel_usage}L`);
        console.log(`    Sessions: ${site.total_sessions}`);
        console.log(`    Peak Period: ${site.peak_time_slot}`);
        console.log('');
      });
      
    } else {
      console.log('❌ Error:', data.error);
    }
    
  } catch (error) {
    console.error('❌ Request failed:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
  }
}

testActivityEndpoint();