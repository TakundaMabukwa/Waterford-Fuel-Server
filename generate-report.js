const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function generateReport() {
  try {
    console.log('🔄 Generating daily report...');
    
    // Generate today's daily report
    const response = await axios.get(`${BASE_URL}/api/energy-rite/reports/daily`);
    
    console.log('✅ Daily report generated successfully!');
    console.log('📊 Report Summary:');
    console.log(`- Report Date: ${response.data.data.report_date}`);
    console.log(`- Period: ${response.data.data.period}`);
    console.log(`- Total Sites: ${response.data.data.sites.length}`);
    
    // Display site summary
    response.data.data.sites.forEach(site => {
      console.log(`\n🏢 Site: ${site.branch}`);
      console.log(`   Cost Code: ${site.cost_code}`);
      console.log(`   Current Fuel: ${site.current_fuel_level}L`);
      console.log(`   Monthly Usage: ${site.monthly_data.total_fuel_usage}L`);
      console.log(`   Monthly Hours: ${site.monthly_data.total_running_hours}h`);
    });
    
  } catch (error) {
    console.error('❌ Error generating report:', error.message);
    if (error.response) {
      console.error('Response:', error.response.data);
    }
  }
}

// Run the report generation
generateReport();