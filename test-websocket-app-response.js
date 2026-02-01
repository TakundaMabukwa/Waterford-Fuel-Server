require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

/**
 * Test pushing data through the actual WebSocket client to see app response
 */

class WebSocketAppTester {
  constructor() {
    this.wsClient = new EnergyRiteWebSocketClient();
  }

  async testEngineOnSession() {
    console.log('🚗 Testing Engine ON through WebSocket client...\n');

    const message = {
      "Plate": "SUNVALLEY",
      "Speed": 25, // Engine ON
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
    };

    console.log('📡 Sending Engine ON message:');
    console.log(`   Vehicle: ${message.Plate}`);
    console.log(`   Speed: ${message.Speed} (Engine ON)`);
    console.log(`   Fuel: ${message.fuel_probe_1_level}L (${message.fuel_probe_1_level_percentage}%)`);

    // Process through WebSocket client
    await this.wsClient.processVehicleUpdate(message);
    
    await this.sleep(2000);
  }

  async testFuelFill() {
    console.log('\n⛽ Testing Fuel Fill through WebSocket client...\n');

    // Pre-fill message
    const preFillMessage = {
      "Plate": "SUNVALLEY",
      "Speed": 0,
      "Latitude": -34.122268,
      "Longitude": 18.390642,
      "LocTime": "2280",
      "Quality": "61.13.2.44",
      "Mileage": null,
      "Pocsagstr": "",
      "Head": "",
      "Geozone": "Shell Fuel Station, Cape Town, South Africa",
      "DriverName": "Possible Fuel Fill",
      "NameEvent": "",
      "Temperature": "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
      "fuel_probe_1_level": 160.5,
      "fuel_probe_1_volume_in_tank": 332.0,
      "fuel_probe_1_temperature": 27,
      "fuel_probe_1_level_percentage": 78,
      "message_type": 405
    };

    console.log('📡 Sending Pre-fill message:');
    console.log(`   Vehicle: ${preFillMessage.Plate}`);
    console.log(`   Status: "${preFillMessage.DriverName}"`);
    console.log(`   Fuel: ${preFillMessage.fuel_probe_1_level}L (${preFillMessage.fuel_probe_1_level_percentage}%)`);

    await this.wsClient.processVehicleUpdate(preFillMessage);
    await this.sleep(3000);

    // Post-fill message
    const postFillMessage = {
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
      "DriverName": "",
      "NameEvent": "",
      "Temperature": "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
      "fuel_probe_1_level": 198.7,
      "fuel_probe_1_volume_in_tank": 410.5,
      "fuel_probe_1_temperature": 28,
      "fuel_probe_1_level_percentage": 96,
      "message_type": 405
    };

    console.log('\n📡 Sending Post-fill message:');
    console.log(`   Vehicle: ${postFillMessage.Plate}`);
    console.log(`   Status: "${postFillMessage.DriverName || 'Normal'}"`);
    console.log(`   Fuel: ${postFillMessage.fuel_probe_1_level}L (${postFillMessage.fuel_probe_1_level_percentage}%)`);
    console.log(`   Expected Fill: +${(postFillMessage.fuel_probe_1_level - preFillMessage.fuel_probe_1_level).toFixed(1)}L`);

    await this.wsClient.processVehicleUpdate(postFillMessage);
    await this.sleep(3000);
  }

  async testEngineOff() {
    console.log('\n🛑 Testing Engine OFF through WebSocket client...\n');

    const message = {
      "Plate": "SUNVALLEY",
      "Speed": 0, // Engine OFF
      "Latitude": -34.122268,
      "Longitude": 18.390642,
      "LocTime": "2290",
      "Quality": "61.13.2.44",
      "Mileage": null,
      "Pocsagstr": "",
      "Head": "",
      "Geozone": "0B Buller Louw Blvd, Sunnydale, Cape Town, 7975, South Africa",
      "DriverName": "",
      "NameEvent": "",
      "Temperature": "25,405,1007,2020,067E,2021,0D6C,2022,1B,2023,51",
      "fuel_probe_1_level": 195.2,
      "fuel_probe_1_volume_in_tank": 403.0,
      "fuel_probe_1_temperature": 29,
      "fuel_probe_1_level_percentage": 94,
      "message_type": 405
    };

    console.log('📡 Sending Engine OFF message:');
    console.log(`   Vehicle: ${message.Plate}`);
    console.log(`   Speed: ${message.Speed} (Engine OFF)`);
    console.log(`   Fuel: ${message.fuel_probe_1_level}L (${message.fuel_probe_1_level_percentage}%)`);

    await this.wsClient.processVehicleUpdate(message);
    await this.sleep(2000);
  }

  async runTest() {
    console.log('🧪 WEBSOCKET APP RESPONSE TEST');
    console.log('Testing how the app responds to WebSocket data\n');
    console.log('=' .repeat(50));

    try {
      await this.testEngineOnSession();
      await this.testFuelFill();
      await this.testEngineOff();

      console.log('\n✅ All WebSocket messages sent through app!');
      console.log('📊 Check the console output above to see app responses');
      console.log('💡 The app should have:');
      console.log('   • Created engine session');
      console.log('   • Detected fuel fill');
      console.log('   • Completed engine session');

    } catch (error) {
      console.error('❌ Test error:', error.message);
    }
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Run test
async function runTest() {
  const tester = new WebSocketAppTester();
  
  try {
    await tester.runTest();
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