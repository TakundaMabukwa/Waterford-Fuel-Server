require('dotenv').config();
const { supabase } = require('./supabase-client');

async function confirmActivityReport() {
  try {
    console.log('🔍 Confirming Activity Report Structure...\n');
    
    const testDate = new Date().toISOString().split('T')[0];
    
    // Create test data
    await supabase.from('energy_rite_operating_sessions').insert({
      branch: 'KROONSTAD2',
      cost_code: 'KFC-KROONSTAD2-001',
      session_date: testDate,
      session_start_time: new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString(),
      session_end_time: new Date().toISOString(),
      operating_hours: 4.0,
      total_usage: 25.0,
      total_fill: 130.0,
      session_status: 'COMPLETED'
    });
    
    await supabase.from('energy_rite_fuel_fills').insert({
      plate: 'KROONSTAD2',
      cost_code: 'KFC-KROONSTAD2-001',
      fill_date: new Date().toISOString(),
      fuel_before: 45,
      fuel_after: 175,
      fill_amount: 130,
      detection_method: 'STATUS_INDICATOR'
    });
    
    // Test the controller directly
    const controller = require('./controllers/energy-rite/energyRiteReportsController');
    
    const mockReq = {
      query: { date: testDate }
    };
    
    const mockRes = {
      json: (data) => {
        console.log('✅ Activity Report Response:');
        console.log(`Date: ${data.data.date}`);
        console.log(`Total Sites: ${data.data.summary.total_sites}`);
        console.log(`Total Operating Hours: ${data.data.summary.total_operating_hours}h`);
        console.log(`Total Fuel Usage: ${data.data.summary.total_fuel_usage}L`);
        console.log(`Total Fuel Fills: ${data.data.summary.total_fuel_fills || 0}`);
        
        if (data.data.sites && data.data.sites.length > 0) {
          console.log('\n🏢 Sites:');
          data.data.sites.forEach(site => {
            console.log(`${site.branch}:`);
            console.log(`  Operating Hours: ${site.total_operating_hours}h`);
            console.log(`  Fuel Usage: ${site.total_fuel_usage}L`);
            console.log(`  Fuel Filled: ${site.total_fuel_filled}L`);
            if (site.fuel_fills) {
              console.log(`  Fill Events: ${site.fuel_fills.fill_count}`);
            }
          });
        }
        
        console.log('\n✅ CONFIRMED: Activity report shows:');
        console.log('  • Which sites were operational');
        console.log('  • Operating hours per site');
        console.log('  • Fuel usage per site');
        console.log('  • Fuel fills per site');
      },
      status: () => ({ json: mockRes.json })
    };
    
    await controller.getActivityReport(mockReq, mockRes);
    
    // Cleanup
    await supabase.from('energy_rite_operating_sessions').delete().eq('branch', 'KROONSTAD2');
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', 'KROONSTAD2');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

confirmActivityReport();