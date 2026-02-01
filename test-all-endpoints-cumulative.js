require('dotenv').config();
const axios = require('axios');

async function testAllEndpoints() {
  console.log('📊 Testing All Dashboard Endpoints - Cumulative Analysis\n');

  const baseUrl = 'http://localhost:4000';
  const costCode = 'KFC-0001-0001-0002-0004';

  try {
    // 1. Enhanced Executive Dashboard
    console.log('1️⃣ Enhanced Executive Dashboard');
    console.log('=' .repeat(50));
    const enhanced = await axios.get(`${baseUrl}/api/energy-rite/enhanced-executive-dashboard?period=30&costCode=${costCode}`);
    const enhancedData = enhanced.data.data;
    console.log(`📅 Period: ${enhancedData.period.days} days (${enhancedData.period.start_date} to ${enhancedData.period.end_date})`);
    console.log(`📊 Type: ${enhancedData.period.is_cumulative ? 'CUMULATIVE' : 'NON-CUMULATIVE'}`);
    console.log(`⛽ Fuel Used: ${enhancedData.key_metrics.total_litres_used}L`);
    console.log(`⏱️  Hours: ${enhancedData.key_metrics.total_operational_hours}h`);

    // 2. Standard Executive Dashboard  
    console.log('\n2️⃣ Standard Executive Dashboard');
    console.log('=' .repeat(50));
    const standard = await axios.get(`${baseUrl}/api/energy-rite/executive-dashboard?days=30&costCode=${costCode}`);
    const standardData = standard.data.data;
    console.log(`📅 Period: ${standardData.period.days} days (${standardData.period.start_date} to ${standardData.period.end_date})`);
    console.log(`📊 Type: CUMULATIVE (rolling ${standardData.period.days} days)`);
    console.log(`⛽ Fuel Used: ${standardData.operational_metrics.total_fuel_usage_liters}L`);
    console.log(`⏱️  Hours: ${standardData.operational_metrics.total_operating_hours}h`);

    // 3. Activity Report
    console.log('\n3️⃣ Activity Report');
    console.log('=' .repeat(50));
    const activity = await axios.get(`${baseUrl}/api/energy-rite/reports/activity?start_date=2025-11-01&end_date=2025-11-30&cost_code=${costCode}`);
    const activityData = activity.data.data;
    console.log(`📅 Period: 2025-11-01 to 2025-11-30 (30 days)`);
    console.log(`📊 Type: CUMULATIVE (date range)`);
    console.log(`📋 Sessions: ${activityData.summary?.total_sessions || 0}`);
    console.log(`⛽ Fuel: ${activityData.summary?.total_fuel_usage || 0}L`);

    // 4. Snapshots
    console.log('\n4️⃣ Snapshots');
    console.log('=' .repeat(50));
    const snapshots = await axios.get(`${baseUrl}/api/energy-rite/reports/snapshots?date=2025-11-13&cost_code=${costCode}`);
    const snapshotsData = snapshots.data.data;
    console.log(`📅 Date: 2025-11-13 (single day)`);
    console.log(`📊 Type: POINT-IN-TIME (daily snapshots)`);
    console.log(`📸 Snapshots: ${snapshotsData.snapshots?.length || 0}`);

    // 5. Fuel Analysis
    console.log('\n5️⃣ Fuel Analysis');
    console.log('=' .repeat(50));
    try {
      const fuelAnalysis = await axios.get(`${baseUrl}/api/energy-rite/fuel-analysis?days=30&costCode=${costCode}`);
      const fuelData = fuelAnalysis.data.data;
      console.log(`📅 Period: Last 30 days`);
      console.log(`📊 Type: CUMULATIVE (rolling period)`);
      console.log(`⛽ Analysis: ${fuelData.summary ? 'Available' : 'No data'}`);
    } catch (error) {
      console.log(`❌ Fuel Analysis: ${error.response?.status || 'Error'}`);
    }

    // 6. Vehicles (Current Status)
    console.log('\n6️⃣ Vehicles');
    console.log('=' .repeat(50));
    const vehicles = await axios.get(`${baseUrl}/api/energy-rite/vehicles`);
    const vehiclesData = vehicles.data.data;
    console.log(`📅 Time: Current moment`);
    console.log(`📊 Type: REAL-TIME (current status)`);
    console.log(`🚗 Vehicles: ${vehiclesData?.length || 0}`);

    console.log('\n\n📋 CUMULATIVE ANALYSIS SUMMARY:');
    console.log('=' .repeat(60));
    console.log('✅ CUMULATIVE ENDPOINTS:');
    console.log('   • Enhanced Executive Dashboard (30 days cumulative)');
    console.log('   • Standard Executive Dashboard (rolling days)');
    console.log('   • Activity Report (date range cumulative)');
    console.log('   • Fuel Analysis (rolling period cumulative)');
    console.log('');
    console.log('❌ NON-CUMULATIVE ENDPOINTS:');
    console.log('   • Snapshots (point-in-time daily data)');
    console.log('   • Vehicles (real-time current status)');

  } catch (error) {
    console.error('❌ Error:', error.response?.data || error.message);
  }
}

testAllEndpoints().then(() => process.exit(0));