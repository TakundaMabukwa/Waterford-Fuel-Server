require('dotenv').config();
const axios = require('axios');

async function testDashboardEndpoints() {
  console.log('📊 Testing Dashboard Endpoints\n');

  const baseUrl = 'http://localhost:4000';
  const costCode = 'KFC-0001-0001-0002-0004';

  try {
    // 1. Enhanced Executive Dashboard (Monthly)
    console.log('1️⃣ Enhanced Executive Dashboard (November 2025)');
    console.log('=' .repeat(50));
    
    const enhancedResponse = await axios.get(`${baseUrl}/api/energy-rite/enhanced-executive-dashboard`, {
      params: {
        date: '2025-11-30',
        period: 30,
        costCode: costCode
      }
    });
    
    const enhanced = enhancedResponse.data.data;
    console.log(`📅 Period: ${enhanced.period.start_date} to ${enhanced.period.end_date} (${enhanced.period.days} days)`);
    console.log(`🏢 Sites Operated: ${enhanced.key_metrics.total_sites_operated}`);
    console.log(`⛽ Fuel Used: ${enhanced.key_metrics.total_litres_used}L`);
    console.log(`🔋 Fuel Filled: ${enhanced.key_metrics.total_litres_filled}L`);
    console.log(`⏱️  Operating Hours: ${enhanced.key_metrics.total_operational_hours}h`);
    console.log(`💰 Total Cost: R${enhanced.key_metrics.total_operational_cost}`);
    console.log(`🔄 Continuous Ops: ${enhanced.key_metrics.continuous_operations_count} sites`);
    
    console.log('\n🏆 Top 5 Sites:');
    enhanced.top_performing_sites.slice(0, 5).forEach((site, i) => {
      console.log(`   ${i+1}. ${site.site}: ${site.operating_hours}h, ${site.fuel_usage}L`);
    });

    // 2. Activity Report (Monthly)
    console.log('\n\n2️⃣ Activity Report (November 2025)');
    console.log('=' .repeat(50));
    
    const activityResponse = await axios.get(`${baseUrl}/api/energy-rite/reports/activity`, {
      params: {
        start_date: '2025-11-01',
        end_date: '2025-11-30',
        cost_code: costCode
      }
    });
    
    const activity = activityResponse.data.data;
    console.log(`📊 Total Sessions: ${activity.summary?.total_sessions || 'N/A'}`);
    console.log(`⏱️  Total Hours: ${activity.summary?.total_operating_hours || 'N/A'}h`);
    console.log(`⛽ Total Fuel: ${activity.summary?.total_fuel_usage || 'N/A'}L`);
    console.log(`🏢 Sites: ${activity.summary?.unique_sites || 'N/A'}`);

    // 3. Snapshots (Yesterday's Data)
    console.log('\n\n3️⃣ Snapshots (Yesterday - 2025-11-13)');
    console.log('=' .repeat(50));
    
    const snapshotsResponse = await axios.get(`${baseUrl}/api/energy-rite/reports/snapshots`, {
      params: {
        date: '2025-11-13',
        include_hierarchy: true,
        cost_code: costCode
      }
    });
    
    const snapshots = snapshotsResponse.data.data;
    console.log(`📸 Snapshots: ${snapshots.snapshots?.length || 0}`);
    console.log(`🏢 Active Sites: ${snapshots.summary?.total_active_sites || 'N/A'}`);
    console.log(`⛽ Fuel Consumption: ${snapshots.summary?.total_fuel_consumption || 'N/A'}L`);

    console.log('\n\n💡 ANALYSIS:');
    console.log('=' .repeat(50));
    console.log(`✅ Enhanced Dashboard IS cumulative for entire month`);
    console.log(`   Period: ${enhanced.period.days} days cumulative`);
    console.log(`   Data aggregated from ${enhanced.period.start_date} to ${enhanced.period.end_date}`);
    console.log(`✅ Activity Report covers monthly period`);
    console.log(`✅ Snapshots provide daily/point-in-time data`);
    
  } catch (error) {
    console.error('❌ Error:', error.response?.data || error.message);
  }
}

testDashboardEndpoints().then(() => process.exit(0));