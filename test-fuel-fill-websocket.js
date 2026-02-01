require('dotenv').config();
const EnergyRiteWebSocketClient = require('./websocket-client');

class TestFuelFillWebSocket {
  constructor() {
    this.client = new EnergyRiteWebSocketClient('dummy');
    this.testData = {
      plate: 'TEST001',
      currentFuel: 100,
      isRunning: false
    };
  }

  async start() {
    console.log('🧪 Starting fuel fill test for 2 minutes...');
    
    // Simulate fuel fill scenario
    await this.simulateFuelFillScenario();
    
    setTimeout(() => {
      console.log('✅ Test completed after 2 minutes');
      process.exit(0);
    }, 120000); // 2 minutes
  }

  async simulateFuelFillScenario() {
    let currentFuel = 100;
    let step = 0;
    
    const interval = setInterval(async () => {
      step++;
      
      let driverName = '';
      let fuelChange = 0;
      
      // Simulate different scenarios
      if (step === 5) {
        driverName = 'Possible Fuel Fill Detected';
        console.log('🔥 Starting fuel fill simulation...');
      } else if (step >= 6 && step <= 15) {
        driverName = 'Fuel Fill In Progress';
        fuelChange = Math.random() * 5 + 2; // 2-7L increase
      } else if (step === 16) {
        driverName = 'Normal Operation';
        console.log('🔥 Ending fuel fill simulation...');
      } else if (step >= 30 && step <= 35) {
        // Simulate fuel decrease (consumption)
        fuelChange = -(Math.random() * 2 + 0.5); // 0.5-2.5L decrease
      }
      
      currentFuel += fuelChange;
      currentFuel = Math.max(0, Math.min(300, currentFuel)); // Keep within bounds
      
      const testMessage = {
        Plate: 'TEST001',
        DriverName: driverName,
        fuel_probe_1_level: currentFuel.toFixed(1),
        fuel_probe_1_level_percentage: ((currentFuel / 300) * 100).toFixed(1),
        timestamp: new Date().toISOString()
      };
      
      console.log(`📊 Step ${step}: ${testMessage.Plate} - ${currentFuel.toFixed(1)}L (${testMessage.fuel_probe_1_level_percentage}%) - ${driverName}`);
      
      // Process the test data through the client
      await this.client.processVehicleUpdate(testMessage);
      
      if (step >= 60) { // Stop after 60 steps (2 minutes at 2-second intervals)
        clearInterval(interval);
      }
    }, 2000); // Every 2 seconds
  }
}

// Start the test
const test = new TestFuelFillWebSocket();
test.start();