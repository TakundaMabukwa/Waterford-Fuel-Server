require('dotenv').config();
const axios = require('axios');

async function testActivityFinal() {
  console.log('🧪 Testing Activity Report with Time Periods...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30'
      }
    });
    
    console.log('✅ Activity report with time periods working!');
    console.log('📅 Date:', response.data.data.date);
    console.log('📊 Summary:', response.data.data.summary);
    
    console.log('\n🕐 Time Period Breakdown:');
    Object.entries(response.data.data.time_periods).forEach(([period, periodData]) => {
      console.log(`\n${periodData.name}:`);
      if (periodData.data) {
        console.log(`   📸 Snapshot: ${new Date(periodData.data.snapshot_time).toLocaleString()}`);
        console.log(`   🚗 Active: ${periodData.data.active_vehicles}/${periodData.data.total_vehicles} (${periodData.data.utilization_rate}%)`);
        console.log(`   ⛽ Fuel: ${periodData.data.average_fuel_percentage}% avg`);
      } else {
        console.log('   ⚠️ No snapshot data');
      }
    });
    
  } catch (error) {
    console.error('❌ Test failed:', error.response?.data || error.message);
  }
}

testActivityFinal();