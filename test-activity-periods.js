require('dotenv').config();
const axios = require('axios');

async function testActivityPeriods() {
  console.log('🕐 Testing Activity Report with Time Periods...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30',
        site_id: 'MILNERTON'
      }
    });
    
    console.log('✅ Activity report with time periods retrieved!');
    console.log('📅 Date:', response.data.data.date);
    console.log('🎯 Site ID:', response.data.data.site_id);
    console.log('📊 Summary:', response.data.data.summary);
    
    console.log('\n🕐 Time Period Breakdown:');
    Object.entries(response.data.data.time_periods).forEach(([period, data]) => {
      console.log(`\n${data.name}:`);
      if (data.data) {
        console.log(`   📸 Snapshot Time: ${new Date(data.data.snapshot_time).toLocaleString()}`);
        console.log(`   🚗 Vehicles: ${data.data.active_vehicles}/${data.data.total_vehicles} (${data.data.utilization_rate}%)`);
        console.log(`   ⛽ Fuel Level: ${data.data.total_fuel_level}L (${data.data.average_fuel_percentage}%)`);
        console.log(`   📋 Vehicle Details: ${data.data.vehicles_breakdown.length} vehicles`);
      } else {
        console.log('   ⚠️ No snapshot data available');
      }
    });
    
    console.log('\n🏪 Site Activity:', response.data.data.sites.length, 'sites');
    
  } catch (error) {
    console.error('❌ Activity periods test failed:', error.response?.data || error.message);
  }
}

testActivityPeriods();