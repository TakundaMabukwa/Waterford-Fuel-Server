require('dotenv').config();
const axios = require('axios');

async function testActivityToday() {
  console.log('🔍 Testing Activity Report for Today - MILNERTON...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        site_id: 'MILNERTON',
        cost_code: 'KFC-0001-0001-0002-0002'
      }
    });
    
    console.log('✅ MILNERTON activity report for today retrieved!');
    console.log('📊 Summary:', response.data.data.summary);
    console.log('🏢 Sites returned:', response.data.data.sites.length);
    console.log('📅 Date:', response.data.data.date);
    console.log('🎯 Site ID:', response.data.data.site_id);
    console.log('🏷️ Cost Code:', response.data.data.cost_code);
    
    response.data.data.sites.forEach(site => {
      console.log(`\n🏪 Site: ${site.branch}`);
      console.log(`   Sessions: ${site.session_count}`);
      console.log(`   Operating Hours: ${site.total_operating_hours}`);
      console.log(`   Fuel Usage: ${site.total_fuel_usage}`);
      
      if (site.sessions && site.sessions.length > 0) {
        console.log('   📋 Sessions:');
        site.sessions.forEach((session, index) => {
          console.log(`     ${index + 1}. ${new Date(session.start_time).toLocaleString()}`);
          console.log(`        Duration: ${session.duration_hours}h, Usage: ${session.fuel_usage}L`);
        });
      }
    });
    
  } catch (error) {
    console.error('❌ Activity test failed:', error.response?.data || error.message);
  }
}

testActivityToday();