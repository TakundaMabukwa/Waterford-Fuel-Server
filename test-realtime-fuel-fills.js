require('dotenv').config();
const { supabase } = require('./supabase-client');
const { detectFuelFill } = require('./helpers/fuel-fill-detector');

/**
 * Real-time fuel fill simulation that mimics actual WebSocket data flow
 * Tests fuel fill detection without affecting operating sessions
 */

class RealtimeFuelFillTest {
  constructor() {
    this.testData = [
      {
        plate: 'SUNVALLEY',
        sequence: [
          // Normal operation
          { fuel: 166.2, percentage: 81, status: '', time: 0 },
          { fuel: 165.8, percentage: 80, status: '', time: 2000 },
          { fuel: 165.1, percentage: 80, status: '', time: 4000 },
          
          // Fuel fill starts
          { fuel: 164.5, percentage: 79, status: 'Possible Fuel Fill', time: 6000 },
          { fuel: 175.2, percentage: 85, status: 'Possible Fuel Fill', time: 8000 },
          { fuel: 189.8, percentage: 92, status: 'Possible Fuel Fill', time: 10000 },
          
          // Fill completed
          { fuel: 195.5, percentage: 95, status: '', time: 12000 },
          { fuel: 195.2, percentage: 95, status: '', time: 14000 }
        ]
      },
      {
        plate: 'MORULA',
        sequence: [
          // Normal operation
          { fuel: 120.5, percentage: 65, status: '', time: 1000 },
          { fuel: 119.8, percentage: 64, status: '', time: 3000 },
          
          // Large fuel increase (no status indicator)
          { fuel: 118.9, percentage: 64, status: '', time: 5000 },
          { fuel: 158.3, percentage: 85, status: '', time: 7000 }, // +39.4L increase
          { fuel: 157.9, percentage: 85, status: '', time: 9000 },
          { fuel: 157.1, percentage: 84, status: '', time: 11000 }
        ]
      }
    ];
  }

  generateMessage(plate, fuelData) {
    return {
      Plate: plate,
      Speed: Math.floor(Math.random() * 20),
      Latitude: -25.522689 + (Math.random() - 0.5) * 0.01,
      Longitude: 28.034974 + (Math.random() - 0.5) * 0.01,
      LocTime: Date.now().toString(),
      Quality: "60.42.2.146",
      Mileage: null,
      Pocsagstr: "",
      Head: "",
      Geozone: fuelData.status.includes('Fuel Fill') ? "Fuel Station, South Africa" : "Normal Location, South Africa",
      DriverName: fuelData.status,
      NameEvent: "",
      Temperature: "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
      fuel_probe_1_level: fuelData.fuel,
      fuel_probe_1_volume_in_tank: fuelData.fuel * 2.2, // Approximate volume
      fuel_probe_1_temperature: 25 + Math.random() * 5,
      fuel_probe_1_level_percentage: fuelData.percentage,
      message_type: 405
    };
  }

  async storeVehicleData(message) {
    try {
      const { error } = await supabase.from('energy_rite_fuel_data').insert({
        plate: message.Plate,
        fuel_probe_1_level: message.fuel_probe_1_level,
        fuel_probe_1_volume_in_tank: message.fuel_probe_1_volume_in_tank,
        fuel_probe_1_temperature: message.fuel_probe_1_temperature,
        fuel_probe_1_level_percentage: message.fuel_probe_1_level_percentage,
        speed: message.Speed,
        latitude: message.Latitude,
        longitude: message.Longitude,
        drivername: message.DriverName,
        address: message.Geozone,
        status: message.Speed > 0 ? 'ON' : 'OFF'
      });

      if (error) throw error;
      return true;
    } catch (error) {
      console.error(`❌ Error storing data for ${message.Plate}:`, error.message);
      return false;
    }
  }

  async runSimulation() {
    console.log('🚀 REAL-TIME FUEL FILL SIMULATION');
    console.log('=' .repeat(50));
    console.log('Testing fuel fill detection with streaming data\n');

    // Check initial state
    await this.checkInitialState();

    // Process data streams for all vehicles simultaneously
    const startTime = Date.now();
    const maxTime = Math.max(...this.testData.flatMap(v => v.sequence.map(s => s.time)));

    console.log('📡 Starting data stream simulation...\n');

    // Create timeline of all events
    const timeline = [];
    this.testData.forEach(vehicle => {
      vehicle.sequence.forEach(dataPoint => {
        timeline.push({
          time: dataPoint.time,
          plate: vehicle.plate,
          data: dataPoint
        });
      });
    });

    // Sort by time
    timeline.sort((a, b) => a.time - b.time);

    // Process timeline
    for (const event of timeline) {
      const message = this.generateMessage(event.plate, event.data);
      
      console.log(`⏰ ${new Date().toLocaleTimeString()} - ${event.plate}:`);
      console.log(`   Fuel: ${event.data.fuel}L (${event.data.percentage}%)`);
      if (event.data.status) {
        console.log(`   Status: "${event.data.status}"`);
      }

      // Store data
      const stored = await this.storeVehicleData(message);
      if (!stored) continue;

      // Run fuel fill detection
      try {
        const fillResult = await detectFuelFill(
          event.plate, 
          event.data.fuel, 
          event.data.status
        );

        if (fillResult.isFill) {
          console.log(`   ⛽ FUEL FILL DETECTED!`);
          console.log(`      Method: ${fillResult.fillDetails.detectionMethod}`);
          console.log(`      Amount: +${fillResult.fillDetails.fillAmount}L`);
          console.log(`      Percentage: ${fillResult.fillDetails.fillPercentage}%`);
        }
      } catch (error) {
        console.error(`   ❌ Detection error: ${error.message}`);
      }

      // Wait before next event
      await this.sleep(1500);
      console.log('');
    }

    console.log('⏳ Processing complete, checking results...\n');
    await this.sleep(3000);
    await this.checkResults();
  }

  async checkInitialState() {
    console.log('🔍 Initial State Check\n');

    // Check active sessions
    const { data: sessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .in('branch', this.testData.map(v => v.plate));

    console.log(`📋 Active Sessions: ${sessions?.length || 0}`);
    if (sessions && sessions.length > 0) {
      sessions.forEach(session => {
        console.log(`   🟢 ${session.branch}: Started ${new Date(session.session_start_time).toLocaleTimeString()}`);
      });
    }

    // Check recent fuel fills
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const { data: fills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .gte('fill_date', oneHourAgo)
      .in('plate', this.testData.map(v => v.plate));

    console.log(`⛽ Recent Fuel Fills (1h): ${fills?.length || 0}`);
    if (fills && fills.length > 0) {
      fills.forEach(fill => {
        console.log(`   📊 ${fill.plate}: +${fill.fill_amount}L at ${new Date(fill.fill_date).toLocaleTimeString()}`);
      });
    }
    console.log('');
  }

  async checkResults() {
    console.log('📊 SIMULATION RESULTS');
    console.log('=' .repeat(50));

    // Check new fuel fills
    const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    const { data: newFills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .gte('fill_date', tenMinutesAgo)
      .in('plate', this.testData.map(v => v.plate))
      .order('fill_date', { ascending: false });

    console.log(`\n⛽ NEW FUEL FILLS DETECTED: ${newFills?.length || 0}`);
    if (newFills && newFills.length > 0) {
      newFills.forEach(fill => {
        console.log(`\n   ✅ ${fill.plate}:`);
        console.log(`      Fill Amount: +${fill.fill_amount}L`);
        console.log(`      Before: ${fill.fuel_before}L → After: ${fill.fuel_after}L`);
        console.log(`      Detection Method: ${fill.detection_method}`);
        console.log(`      Time: ${new Date(fill.fill_date).toLocaleTimeString()}`);
        
        if (fill.fill_data) {
          console.log(`      Conditions Met:`);
          const conditions = fill.fill_data.conditions || {};
          console.log(`        • Significant Increase: ${conditions.significantIncrease || 'N/A'}`);
          console.log(`        • Percentage Increase: ${conditions.percentageIncrease || 'N/A'}`);
          console.log(`        • Within Time Window: ${conditions.withinTimeWindow || 'N/A'}`);
          console.log(`        • Status Indicator: ${conditions.statusIndicatesFill || 'N/A'}`);
        }
      });
    } else {
      console.log('   ❌ No fuel fills detected - Check detection logic');
    }

    // Check session impact
    const { data: sessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .in('branch', this.testData.map(v => v.plate));

    console.log(`\n🔄 ACTIVE SESSIONS: ${sessions?.length || 0}`);
    if (sessions && sessions.length > 0) {
      sessions.forEach(session => {
        console.log(`\n   🟢 ${session.branch}:`);
        console.log(`      Started: ${new Date(session.session_start_time).toLocaleTimeString()}`);
        console.log(`      Opening Fuel: ${session.opening_fuel}L`);
        console.log(`      Fill Events: ${session.fill_events || 0}`);
        console.log(`      Fill Amount During Session: ${session.fill_amount_during_session || 0}L`);
        
        if (session.notes) {
          console.log(`      Notes: ${session.notes.substring(0, 100)}...`);
        }
      });
    } else {
      console.log('   ℹ️  No active sessions (normal if engines are off)');
    }

    // Check activity log
    const { data: activities } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .gte('activity_time', tenMinutesAgo)
      .in('branch', this.testData.map(v => v.plate))
      .order('activity_time', { ascending: false });

    console.log(`\n📝 RECENT ACTIVITY: ${activities?.length || 0} entries`);
    if (activities && activities.length > 0) {
      activities.slice(0, 8).forEach(activity => {
        console.log(`   📋 ${activity.branch}: ${activity.activity_type} at ${new Date(activity.activity_time).toLocaleTimeString()}`);
        if (activity.description) {
          console.log(`      ${activity.description}`);
        }
      });
    }

    // Summary
    console.log('\n' + '=' .repeat(50));
    console.log('📈 SUMMARY:');
    console.log(`   • Expected Fills: 2 (SUNVALLEY + MORULA)`);
    console.log(`   • Detected Fills: ${newFills?.length || 0}`);
    console.log(`   • Session Impact: ${sessions?.length || 0} sessions affected`);
    console.log(`   • Activity Entries: ${activities?.length || 0}`);

    if ((newFills?.length || 0) >= 2) {
      console.log('\n✅ SUCCESS: Fuel fills detected correctly!');
    } else {
      console.log('\n⚠️  PARTIAL: Some fills may not have been detected');
    }

    console.log('\n💡 Key Observations:');
    console.log('   • SUNVALLEY: Status indicator method');
    console.log('   • MORULA: Level increase method (+39L)');
    console.log('   • Sessions should remain independent');
    console.log('   • All events should be logged');
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Run the simulation
async function runTest() {
  const test = new RealtimeFuelFillTest();
  
  try {
    await test.runSimulation();
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    console.log('\n🏁 Test completed');
    process.exit(0);
  }
}

if (require.main === module) {
  runTest();
}

module.exports = RealtimeFuelFillTest;