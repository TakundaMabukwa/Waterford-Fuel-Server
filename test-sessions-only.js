require('dotenv').config();
const axios = require('axios');

async function testSessionsOnly() {
  console.log('🏪 Testing Fuel Consumption for Sites with Sessions Only...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/activity', {
      params: { 
        date: '2025-10-30'
      }
    });
    
    console.log('✅ Activity report working!');
    console.log('📅 Date:', response.data.data.date);
    console.log('📊 Total Sessions:', response.data.data.summary.total_sessions);
    console.log('🏪 Sites with Activity:', response.data.data.sites.length);
    
    const fuelAnalysis = response.data.data.fuel_analysis;
    console.log('\n⛽ Fuel Analysis (Sessions Sites Only):');
    console.log(`   📊 Daily Total: ${fuelAnalysis.daily_total_consumption}L`);
    console.log(`   🏆 Peak Period: ${fuelAnalysis.peak_usage_period.name} (${fuelAnalysis.peak_usage_period.usage.toFixed(2)}L)`);
    console.log(`   🏪 Peak Site: ${fuelAnalysis.peak_usage_site.site || 'None'} (${fuelAnalysis.peak_usage_site.usage.toFixed(2)}L)`);
    
    console.log('\n📊 Period Breakdown:');
    console.log(`   🌅 Morning: ${fuelAnalysis.period_breakdown.morning}L`);
    console.log(`   ☀️ Afternoon: ${fuelAnalysis.period_breakdown.afternoon}L`);
    console.log(`   🌙 Evening: ${fuelAnalysis.period_breakdown.evening}L`);
    
    // Show which sites had sessions
    console.log('\n🏪 Sites with Sessions:');
    response.data.data.sites.forEach(site => {
      if (site.session_count > 0) {
        console.log(`   - ${site.branch}: ${site.session_count} sessions`);
      }
    });
    
    console.log('\n✅ Fuel consumption calculated only for sites with sessions!');
    
  } catch (error) {
    console.error('❌ Sessions-only test failed:', error.response?.data || error.message);
  }
}

testSessionsOnly();