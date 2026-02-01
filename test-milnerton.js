require('dotenv').config();
const axios = require('axios');

async function testMilnerton() {
  console.log('🔍 Testing MILNERTON Activity Report...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30',
        site_id: 'MILNERTON'
      }
    });
    
    console.log('✅ MILNERTON activity report retrieved!');
    console.log('📊 Summary:', response.data.data.summary);
    console.log('🏢 Sites returned:', response.data.data.sites.length);
    
    response.data.data.sites.forEach(site => {
      console.log(`\n🏪 Site: ${site.branch}`);
      console.log(`   Sessions: ${site.session_count}`);
      console.log(`   Operating Hours: ${site.total_operating_hours}`);
      console.log(`   Fuel Usage: ${site.total_fuel_usage}`);
      console.log(`   Cost: ${site.total_cost}`);
      
      if (site.sessions && site.sessions.length > 0) {
        console.log('   📋 Session Details:');
        site.sessions.forEach((session, index) => {
          console.log(`     ${index + 1}. ${session.start_time} - ${session.end_time || 'ongoing'}`);
          console.log(`        Duration: ${session.duration_hours}h, Usage: ${session.fuel_usage}L`);
        });
      }
    });
    
  } catch (error) {
    console.error('❌ MILNERTON test failed:', error.response?.data || error.message);
  }
}

testMilnerton();