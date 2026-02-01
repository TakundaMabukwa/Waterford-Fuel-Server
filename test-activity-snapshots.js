const axios = require('axios');

const BASE_URL = 'http://localhost:4000';

async function testActivityEndpoints() {
  console.log('🧪 Testing Activity Snapshot Endpoints\n');

  try {
    // Test 1: Take a manual snapshot
    console.log('1️⃣ Testing manual snapshot...');
    const snapshotResponse = await axios.post(`${BASE_URL}/api/energy-rite/activity-reports/snapshot`);
    
    if (snapshotResponse.data.success) {
      console.log('✅ Manual snapshot successful');
      console.log('   Response:', JSON.stringify(snapshotResponse.data, null, 2));
      
      if (snapshotResponse.data.data) {
        const data = snapshotResponse.data.data;
        console.log(`   - Time slot: ${data.time_slot_name || data.time_slot}`);
        console.log(`   - Active vehicles: ${data.active_vehicles}/${data.total_vehicles}`);
        console.log(`   - Vehicles with data: ${data.vehicles_data?.length || 0}`);
      }
    } else {
      console.log('❌ Manual snapshot failed:', snapshotResponse.data.error);
    }

  } catch (error) {
    console.log('❌ Manual snapshot error:', error.response?.data?.error || error.message);
  }

  console.log('\n' + '='.repeat(50) + '\n');

  try {
    // Test 2: Get activity dashboard (should include the snapshot we just took)
    console.log('2️⃣ Testing activity dashboard...');
    const dashboardResponse = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports/dashboard?days=1`);
    
    if (dashboardResponse.data.success) {
      console.log('✅ Activity dashboard successful');
      const data = dashboardResponse.data.data;
      
      console.log(`   - Period: ${data.period.start_date} to ${data.period.end_date}`);
      console.log(`   - Total snapshots: ${data.summary.total_snapshots}`);
      console.log(`   - Overall peak time: ${data.summary.overall_peak_time_slot}`);
      
      if (data.daily_snapshots.length > 0) {
        console.log(`   - Daily snapshots: ${data.daily_snapshots.length} days`);
        const today = data.daily_snapshots[data.daily_snapshots.length - 1];
        console.log(`   - Today's peak: ${today.peak_slot} (${today.peak_activity} vehicles)`);
      }
      
      console.log(`   - Cost codes tracked: ${Object.keys(data.cost_code_patterns).length}`);
      console.log(`   - Sites tracked: ${Object.keys(data.site_utilization).length}`);
      
    } else {
      console.log('❌ Activity dashboard failed:', dashboardResponse.data.error);
    }

  } catch (error) {
    console.log('❌ Activity dashboard error:', error.response?.data?.error || error.message);
  }

  console.log('\n' + '='.repeat(50) + '\n');

  try {
    // Test 3: Test with specific date range
    console.log('3️⃣ Testing dashboard with date range...');
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    const rangeResponse = await axios.get(`${BASE_URL}/api/energy-rite/activity-reports/dashboard?startDate=${yesterday}&endDate=${today}`);
    
    if (rangeResponse.data.success) {
      console.log('✅ Date range dashboard successful');
      const data = rangeResponse.data.data;
      console.log(`   - Snapshots in range: ${data.summary.total_snapshots}`);
      console.log(`   - Days analyzed: ${data.summary.total_days}`);
    } else {
      console.log('❌ Date range dashboard failed:', rangeResponse.data.error);
    }

  } catch (error) {
    console.log('❌ Date range dashboard error:', error.response?.data?.error || error.message);
  }

  console.log('\n🏁 Testing completed!');
}

// Run the tests
testActivityEndpoints().catch(console.error);