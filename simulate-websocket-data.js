require('dotenv').config();
const { supabase } = require('./supabase-client');
const { detectFuelFill } = require('./helpers/fuel-fill-detector');

/**
 * WebSocket Data Simulator - Shows exactly which tables data flows into
 * Based on your actual WebSocket messages
 */

class WebSocketDataSimulator {
  constructor() {
    console.log('🔌 WebSocket Data Simulator');
    console.log('📊 Tables that receive data:');
    console.log('   1. energy_rite_fuel_data - All vehicle data');
    console.log('   2. energy_rite_fuel_fills - Detected fuel fills');
    console.log('   3. energy_rite_operating_sessions - Engine on/off sessions');
    console.log('   4. energy_rite_activity_log - All activities\n');
  }

  // Simulate your actual WebSocket messages with fuel fills
  getTestMessages() {
    return [
      // Normal SUNVALLEY data
      {
        "Plate": "SUNVALLEY",
        "Speed": 0,
        "Latitude": -34.122268,
        "Longitude": 18.390642,
        "LocTime": "2276",
        "Quality": "61.13.2.44",
        "Mileage": null,
        "Pocsagstr": "",
        "Head": "",
        "Geozone": "0B Buller Louw Blvd, Sunnydale, Cape Town, 7975, South Africa",
        "DriverName": "",
        "NameEvent": "",
        "Temperature": "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
        "fuel_probe_1_level": 166.2,
        "fuel_probe_1_volume_in_tank": 343.6,
        "fuel_probe_1_temperature": 27,
        "fuel_probe_1_level_percentage": 81,
        "message_type": 405
      },
      
      // SUNVALLEY with fuel consumption
      {
        "Plate": "SUNVALLEY",
        "Speed": 0,
        "Latitude": -34.122268,
        "Longitude": 18.390642,
        "LocTime": "2280",
        "Quality": "61.13.2.44",
        "Mileage": null,
        "Pocsagstr": "",
        "Head": "",
        "Geozone": "0B Buller Louw Blvd, Sunnydale, Cape Town, 7975, South Africa",
        "DriverName": "",
        "NameEvent": "",
        "Temperature": "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
        "fuel_probe_1_level": 164.8,
        "fuel_probe_1_volume_in_tank": 340.2,
        "fuel_probe_1_temperature": 27,
        "fuel_probe_1_level_percentage": 80,
        "message_type": 405
      },

      // SUNVALLEY - FUEL FILL DETECTED!
      {
        "Plate": "SUNVALLEY",
        "Speed": 0,
        "Latitude": -34.122268,
        "Longitude": 18.390642,
        "LocTime": "2285",
        "Quality": "61.13.2.44",
        "Mileage": null,
        "Pocsagstr": "",
        "Head": "",
        "Geozone": "Shell Fuel Station, Cape Town, South Africa",
        "DriverName": "Possible Fuel Fill",
        "NameEvent": "",
        "Temperature": "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
        "fuel_probe_1_level": 198.5,
        "fuel_probe_1_volume_in_tank": 410.0,
        "fuel_probe_1_temperature": 28,
        "fuel_probe_1_level_percentage": 96,
        "message_type": 405
      },

      // Normal MORULA data
      {
        "Plate": "MORULA",
        "Speed": 0,
        "Latitude": -25.522689,
        "Longitude": 28.034974,
        "LocTime": "21201",
        "Quality": "60.42.2.146",
        "Mileage": null,
        "Pocsagstr": "",
        "Head": "",
        "Geozone": "F2GM+WX Mabopane, South Africa",
        "DriverName": "",
        "NameEvent": "",
        "Temperature": "",
        "fuel_probe_1_level": 125.3,
        "fuel_probe_1_volume_in_tank": 260.0,
        "fuel_probe_1_temperature": 25,
        "fuel_probe_1_level_percentage": 68,
        "message_type": 405
      },

      // MORULA - Large fuel increase (no status indicator)
      {
        "Plate": "MORULA",
        "Speed": 0,
        "Latitude": -25.522689,
        "Longitude": 28.034974,
        "LocTime": "21205",
        "Quality": "60.42.2.146",
        "Mileage": null,
        "Pocsagstr": "",
        "Head": "",
        "Geozone": "BP Fuel Station, Mabopane, South Africa",
        "DriverName": "",
        "NameEvent": "",
        "Temperature": "",
        "fuel_probe_1_level": 168.7,
        "fuel_probe_1_volume_in_tank": 350.0,
        "fuel_probe_1_temperature": 26,
        "fuel_probe_1_level_percentage": 91,
        "message_type": 405
      },

      // VORNAVALL - Normal data (no fuel data)
      {
        "Plate": "VORNAVALL",
        "Speed": 0,
        "Latitude": -26.00026,
        "Longitude": 28.10373,
        "LocTime": "779",
        "Quality": "60.41.2.237",
        "Mileage": null,
        "Pocsagstr": "",
        "Head": "",
        "Geozone": "51 Albertyn St, Midrand, 1686, South Africa",
        "DriverName": "",
        "NameEvent": "",
        "Temperature": ""
      }
    ];
  }

  async processWebSocketMessage(message, messageIndex) {
    console.log(`\n📡 Processing Message ${messageIndex + 1}: ${message.Plate}`);
    console.log(`   Fuel: ${message.fuel_probe_1_level || 'N/A'}L (${message.fuel_probe_1_level_percentage || 'N/A'}%)`);
    console.log(`   Status: "${message.DriverName || 'Normal'}"`);
    console.log(`   Location: ${message.Geozone}`);

    try {
      // 1. Store in energy_rite_fuel_data (main data table)
      if (message.fuel_probe_1_level) {
        const { error: fuelError } = await supabase
          .from('energy_rite_fuel_data')
          .insert({
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

        if (fuelError) {
          console.log(`   ❌ Error storing fuel data: ${fuelError.message}`);
          return;
        }
        console.log(`   ✅ Stored in energy_rite_fuel_data`);

        // 2. Run fuel fill detection
        const fillResult = await detectFuelFill(
          message.Plate, 
          message.fuel_probe_1_level, 
          message.DriverName
        );

        if (fillResult.isFill) {
          console.log(`   ⛽ FUEL FILL DETECTED!`);
          console.log(`      Method: ${fillResult.fillDetails.detectionMethod}`);
          console.log(`      Amount: +${fillResult.fillDetails.fillAmount}L`);
          console.log(`      ✅ Stored in energy_rite_fuel_fills`);
          console.log(`      ✅ Logged in energy_rite_activity_log`);
        }
      } else {
        console.log(`   ℹ️  No fuel data - skipping fuel tables`);
      }

      // 3. Session management would happen here (engine on/off detection)
      // This is handled by the WebSocket client's session management logic

    } catch (error) {
      console.log(`   ❌ Processing error: ${error.message}`);
    }
  }

  async runSimulation() {
    console.log('\n🚀 Starting WebSocket Data Simulation\n');
    console.log('=' .repeat(60));

    const messages = this.getTestMessages();
    
    for (let i = 0; i < messages.length; i++) {
      await this.processWebSocketMessage(messages[i], i);
      await this.sleep(2000); // 2 second delay between messages
    }

    console.log('\n⏳ Processing complete, checking results...\n');
    await this.sleep(3000);
    await this.showResults();
  }

  async showResults() {
    console.log('📊 SIMULATION RESULTS');
    console.log('=' .repeat(60));

    // Check fuel data stored
    const { data: fuelData } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .gte('created_at', new Date(Date.now() - 10 * 60 * 1000).toISOString())
      .in('plate', ['SUNVALLEY', 'MORULA', 'VORNAVALL'])
      .order('created_at', { ascending: false });

    console.log(`\n📋 energy_rite_fuel_data: ${fuelData?.length || 0} records`);
    if (fuelData && fuelData.length > 0) {
      fuelData.forEach(record => {
        console.log(`   • ${record.plate}: ${record.fuel_probe_1_level}L at ${new Date(record.created_at).toLocaleTimeString()}`);
      });
    }

    // Check fuel fills detected
    const { data: fillData } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .gte('fill_date', new Date(Date.now() - 10 * 60 * 1000).toISOString())
      .in('plate', ['SUNVALLEY', 'MORULA'])
      .order('fill_date', { ascending: false });

    console.log(`\n⛽ energy_rite_fuel_fills: ${fillData?.length || 0} records`);
    if (fillData && fillData.length > 0) {
      fillData.forEach(fill => {
        console.log(`   • ${fill.plate}: +${fill.fill_amount}L (${fill.detection_method}) at ${new Date(fill.fill_date).toLocaleTimeString()}`);
      });
    }

    // Check activity log
    const { data: activityData } = await supabase
      .from('energy_rite_activity_log')
      .select('*')
      .gte('activity_time', new Date(Date.now() - 10 * 60 * 1000).toISOString())
      .in('branch', ['SUNVALLEY', 'MORULA', 'VORNAVALL'])
      .order('activity_time', { ascending: false });

    console.log(`\n📝 energy_rite_activity_log: ${activityData?.length || 0} records`);
    if (activityData && activityData.length > 0) {
      activityData.slice(0, 5).forEach(activity => {
        console.log(`   • ${activity.branch}: ${activity.activity_type} at ${new Date(activity.activity_time).toLocaleTimeString()}`);
      });
    }

    // Check sessions (if any)
    const { data: sessionData } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'ONGOING')
      .in('branch', ['SUNVALLEY', 'MORULA', 'VORNAVALL']);

    console.log(`\n🔄 energy_rite_operating_sessions: ${sessionData?.length || 0} active sessions`);
    if (sessionData && sessionData.length > 0) {
      sessionData.forEach(session => {
        console.log(`   • ${session.branch}: Started ${new Date(session.session_start_time).toLocaleTimeString()}`);
      });
    }

    console.log('\n' + '=' .repeat(60));
    console.log('💡 DATA FLOW SUMMARY:');
    console.log('   1. All vehicle data → energy_rite_fuel_data');
    console.log('   2. Fuel fills detected → energy_rite_fuel_fills');
    console.log('   3. Activities logged → energy_rite_activity_log');
    console.log('   4. Sessions managed → energy_rite_operating_sessions');
    console.log('\n✅ Fuel fills are captured independently of sessions!');
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Run simulation
async function runSimulation() {
  const simulator = new WebSocketDataSimulator();
  
  try {
    await simulator.runSimulation();
  } catch (error) {
    console.error('❌ Simulation error:', error.message);
  } finally {
    console.log('\n🏁 Simulation completed');
    process.exit(0);
  }
}

if (require.main === module) {
  runSimulation();
}

module.exports = WebSocketDataSimulator;