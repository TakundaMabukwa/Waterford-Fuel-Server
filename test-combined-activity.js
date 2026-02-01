/**
 * Test Combined Fills in Activity Report
 */

const axios = require('axios');

async function testCombinedFills() {
  try {
    const url = 'http://localhost:4000/api/energy-rite/reports/activity?date=2026-01-14&cost_code=KFC-0001-0001-0002';
    
    console.log('🧪 Testing Combined Fills in Activity Report');
    console.log('='.repeat(70));
    console.log(`URL: ${url}\n`);
    
    const response = await axios.get(url);
    const data = response.data.data;
    
    console.log('✅ Response received\n');
    
    // Check summary
    console.log('📊 Summary:');
    console.log(`  Total Sites: ${data.summary.total_sites}`);
    console.log(`  Total Sessions: ${data.summary.total_sessions}`);
    console.log(`  Total Fill Events: ${data.summary.total_fuel_fills}`);
    console.log(`  Total Filled: ${data.summary.total_fuel_filled_amount}L\n`);
    
    // Check sessions
    console.log('📋 Sessions:');
    console.log('='.repeat(70));
    
    const sessionsByVehicle = {};
    data.sessions.forEach(session => {
      if (!sessionsByVehicle[session.branch]) {
        sessionsByVehicle[session.branch] = [];
      }
      sessionsByVehicle[session.branch].push(session);
    });
    
    Object.entries(sessionsByVehicle).forEach(([vehicle, sessions]) => {
      console.log(`\n🚗 ${vehicle}`);
      sessions.forEach(session => {
        const duration = session.duration_hours < 1 
          ? `${Math.round(session.duration_hours * 60)}m`
          : `${Math.floor(session.duration_hours)}h ${Math.round((session.duration_hours % 1) * 60)}m`;
        
        const status = session.status === 'FUEL_FILL_COMPLETED' ? '⛽' : '🔧';
        const combined = session.is_combined ? `🔗(${session.combined_count})` : '';
        
        console.log(`  ${status} ${duration} | ${session.fuel_usage.toFixed(1)}L used | ${session.fuel_filled.toFixed(1)}L filled ${combined}`);
        
        if (session.is_combined) {
          console.log(`      Combined: ${session.combined_count} fills into one`);
          console.log(`      ${session.opening_fuel.toFixed(1)}L → ${session.closing_fuel.toFixed(1)}L`);
        }
      });
    });
    
    console.log('\n' + '='.repeat(70));
    
    // Check for BOITEKONG specifically
    const boitekong = sessionsByVehicle['BOITEKONG'];
    if (boitekong) {
      console.log('\n✅ BOITEKONG Analysis:');
      const fills = boitekong.filter(s => s.status === 'FUEL_FILL_COMPLETED');
      console.log(`  Found ${fills.length} fill session(s)`);
      fills.forEach((fill, i) => {
        console.log(`  ${i+1}. ${fill.is_combined ? 'COMBINED' : 'SINGLE'}`);
        console.log(`     ${fill.opening_fuel}L → ${fill.closing_fuel}L = ${fill.fuel_filled}L`);
      });
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Data:', error.response.data);
    }
  }
}

testCombinedFills();
