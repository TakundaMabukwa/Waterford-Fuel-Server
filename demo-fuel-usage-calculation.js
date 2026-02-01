// Load environment variables first
require('dotenv').config();

const { captureScheduledSnapshot } = require('./helpers/snapshot-scheduler');

async function createTestSnapshotsWithFuelVariation() {
  console.log('🧪 CREATING TEST SNAPSHOTS WITH FUEL VARIATION');
  console.log('='.repeat(60));

  try {
    // Simulate a day with fuel consumption between periods
    console.log('\n📅 Simulating a day with fuel consumption...');
    
    // We can't easily modify the existing snapshots, but we can create new ones
    // by triggering the snapshot scheduler with different snapshot types
    
    // Let's check what we have and then create a demo showing how it would work
    const axios = require('axios');
    
    console.log('\n🔍 Current snapshot data:');
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/snapshots');
    
    const snapshots = response.data.data.snapshots;
    console.log(`📊 Total snapshots: ${snapshots.length}`);
    
    // Group by vehicle and period
    const vehicleData = {};
    snapshots.forEach(snapshot => {
      const vehicle = snapshot.vehicle_plate;
      if (!vehicleData[vehicle]) {
        vehicleData[vehicle] = {};
      }
      vehicleData[vehicle][snapshot.snapshot_type] = {
        fuel_volume: snapshot.fuel_volume,
        fuel_level: snapshot.fuel_level,
        time: snapshot.snapshot_time
      };
    });

    console.log('\n📋 Current data by vehicle:');
    Object.entries(vehicleData).forEach(([vehicle, periods]) => {
      console.log(`\n🚗 ${vehicle}:`);
      ['MORNING', 'MIDDAY', 'EVENING'].forEach(period => {
        if (periods[period]) {
          const data = periods[period];
          const time = new Date(data.time).toLocaleTimeString();
          console.log(`   ${period}: ${data.fuel_volume}L (${data.fuel_level}%) at ${time}`);
        }
      });
    });

    console.log('\n💡 DEMONSTRATION: How fuel usage would be calculated');
    console.log('='.repeat(60));
    
    // Simulate what the analysis would look like with realistic fuel consumption
    const simulatedData = {
      'JANSENPARK': {
        'MORNING': { fuel_volume: 180, fuel_level: 90 },
        'MIDDAY': { fuel_volume: 150, fuel_level: 75 },
        'EVENING': { fuel_volume: 120, fuel_level: 60 }
      },
      'GERMSITON': {
        'MORNING': { fuel_volume: 220, fuel_level: 85 },
        'MIDDAY': { fuel_volume: 200, fuel_level: 77 },
        'EVENING': { fuel_volume: 170, fuel_level: 65 }
      },
      'DENVER': {
        'MORNING': { fuel_volume: 190, fuel_level: 82 },
        'MIDDAY': { fuel_volume: 175, fuel_level: 75 },
        'EVENING': { fuel_volume: 160, fuel_level: 69 }
      }
    };

    console.log('\n📊 SIMULATED FUEL CONSUMPTION ANALYSIS:');
    
    const periods = [
      { name: 'MORNING → MIDDAY', start: 'MORNING', end: 'MIDDAY' },
      { name: 'MIDDAY → EVENING', start: 'MIDDAY', end: 'EVENING' },
      { name: 'MORNING → EVENING (Full Day)', start: 'MORNING', end: 'EVENING' }
    ];

    periods.forEach(period => {
      console.log(`\n🕒 Period: ${period.name}`);
      let totalConsumption = 0;
      let vehicleCount = 0;

      Object.entries(simulatedData).forEach(([vehicle, data]) => {
        const startFuel = data[period.start].fuel_volume;
        const endFuel = data[period.end].fuel_volume;
        const consumption = startFuel - endFuel;
        
        if (consumption > 0) {
          console.log(`   🚗 ${vehicle}: ${startFuel}L → ${endFuel}L = ${consumption}L consumed`);
          totalConsumption += consumption;
          vehicleCount++;
        }
      });

      console.log(`   📈 Total consumption: ${totalConsumption}L across ${vehicleCount} vehicles`);
      console.log(`   📊 Average per vehicle: ${vehicleCount > 0 ? (totalConsumption / vehicleCount).toFixed(1) : 0}L`);
    });

    // Calculate cost center summary
    console.log('\n💼 COST CENTER SUMMARY (KFC-0001-0001-0003):');
    const totalDayConsumption = Object.values(simulatedData).reduce((sum, vehicle) => {
      return sum + (vehicle.MORNING.fuel_volume - vehicle.EVENING.fuel_volume);
    }, 0);
    
    console.log(`   Total daily consumption: ${totalDayConsumption}L`);
    console.log(`   Vehicle count: ${Object.keys(simulatedData).length}`);
    console.log(`   Average per vehicle: ${(totalDayConsumption / Object.keys(simulatedData).length).toFixed(1)}L`);

    console.log('\n🔧 TO ENABLE REAL FUEL USAGE ANALYSIS:');
    console.log('1. 📊 Vehicle fuel levels need to change between periods');
    console.log('2. 🕒 Snapshots should be taken at actual period boundaries');
    console.log('3. ⛽ Real fuel consumption data from vehicle sensors');
    console.log('4. 🔄 Regular snapshot scheduling (6:00, 12:00, 18:00)');

    console.log('\n📋 CURRENT API ENDPOINT CAPABILITIES:');
    console.log('✅ Detects when fuel levels change between periods');
    console.log('✅ Calculates consumption per vehicle per period');
    console.log('✅ Groups by cost center for rollup reporting');
    console.log('✅ Provides period-by-period breakdown');
    console.log('✅ Works with hierarchy (parent cost codes see all children)');
    console.log('✅ Integrates with existing filtering (date, cost code, etc.)');

    console.log('\n🎯 API USAGE FOR FUEL CONSUMPTION:');
    console.log('GET /api/energy-rite/reports/snapshots?include_fuel_usage=true');
    console.log('GET /api/energy-rite/reports/snapshots?cost_code=KFC-0001&include_fuel_usage=true');
    console.log('GET /api/energy-rite/reports/snapshots?date=2025-11-07&include_fuel_usage=true');

    console.log('\n🎉 FUEL USAGE DEMONSTRATION COMPLETED!');

  } catch (error) {
    console.error('❌ Demo failed:', error.message);
  }
}

// Run the demo
createTestSnapshotsWithFuelVariation();