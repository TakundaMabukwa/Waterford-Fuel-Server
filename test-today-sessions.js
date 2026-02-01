require('dotenv').config();
const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function getTodaysSessions() {
  try {
    console.log('📅 Getting today\'s sessions...');
    
    const response = await axios.get(`${BASE_URL}/api/energy-rite/reports/today`);
    
    console.log('✅ Today\'s sessions retrieved successfully!');
    console.log('📊 Session Summary:');
    console.log(`   Date: ${response.data.date}`);
    console.log(`   Total Sessions: ${response.data.total_sessions}`);
    
    if (response.data.sessions && response.data.sessions.length > 0) {
      console.log('\n🏢 Active Sessions:');
      response.data.sessions.slice(0, 5).forEach((session, index) => {
        console.log(`   ${index + 1}. ${session.branch}`);
        console.log(`      Cost Code: ${session.cost_code}`);
        console.log(`      Status: ${session.session_status}`);
        console.log(`      Operating Hours: ${session.operating_hours}h`);
        console.log(`      Fuel Usage: ${session.total_usage}L`);
        console.log('');
      });
      
      if (response.data.sessions.length > 5) {
        console.log(`   ... and ${response.data.sessions.length - 5} more sessions`);
      }
    } else {
      console.log('   No active sessions today');
    }
    
  } catch (error) {
    console.error('❌ Error getting today\'s sessions:', error.message);
    if (error.response) {
      console.error('Response:', error.response.data);
    }
  }
}

getTodaysSessions();