/**
 * Quick test for Activity Report Fill Combining
 */

const axios = require('axios');

async function testFills() {
  try {
    // Test both localhost and production
    const servers = [
      { name: 'Localhost', url: 'http://localhost:4000' },
      { name: 'Production', url: 'http://64.227.138.235:4000' }
    ];

    for (const server of servers) {
      console.log(`\n${'='.repeat(60)}`);
      console.log(`Testing: ${server.name} (${server.url})`);
      console.log('='.repeat(60));

      try {
        const response = await axios.get(
          `${server.url}/api/energy-rite/reports/activity?date=2026-01-14&cost_code=KFC-0001-0001-0002`,
          { timeout: 5000 }
        );

        if (response.data.success) {
          const fills = response.data.data.fuel_analysis?.fuel_fills;
          
          console.log('\n✅ Success!');
          console.log(`Total Fill Events: ${fills?.total_fill_events || 0}`);
          console.log(`Total Fuel Filled: ${fills?.total_fuel_filled || 0}L`);
          
          if (fills?.fills_by_vehicle) {
            const vehicles = Object.keys(fills.fills_by_vehicle);
            console.log(`Vehicles with fills: ${vehicles.length}`);
            
            vehicles.forEach(vehicle => {
              const vData = fills.fills_by_vehicle[vehicle];
              console.log(`\n  ${vehicle}:`);
              console.log(`    Fills: ${vData.fill_count}`);
              console.log(`    Total: ${vData.total_filled.toFixed(2)}L`);
              
              vData.fills.forEach((fill, i) => {
                console.log(`    ${i + 1}. ${fill.amount.toFixed(2)}L ${fill.is_combined ? '🔗 COMBINED(' + fill.combined_count + ')' : '⚪'}`);
              });
            });
          }
        }
      } catch (err) {
        console.log(`\n❌ ${server.name} Error: ${err.message}`);
      }
    }

  } catch (error) {
    console.error('Fatal error:', error.message);
  }
}

testFills();
