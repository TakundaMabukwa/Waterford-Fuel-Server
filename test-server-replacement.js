require('dotenv').config();
const WebSocket = require('ws');
const EnergyRiteWebSocketClient = require('./websocket-client');

class TestServerReplacement {
  constructor() {
    this.wss = null;
    this.client = null;
    this.sites = [
      { plate: "EBONY", quality: "60.115.1.10" },
      { plate: "FLORENTIA", quality: "61.13.2.35" },
      { plate: "BRAAMFONTE", quality: "53.16.1.59" },
      { plate: "RYNFIELD", quality: "53.15.1.99" }
    ];
    this.sessions = new Map();
    this.fuelFills = new Map();
  }

  start() {
    // Start WebSocket server on port 8005 (replacing real server)
    this.wss = new WebSocket.Server({ port: 8005 });
    console.log('🚀 Test server running on port 8005 (replacing real server)');
    
    // Create WebSocket client to process messages
    this.client = new EnergyRiteWebSocketClient('ws://localhost:8005');
    
    this.wss.on('connection', (ws) => {
      console.log('📡 WebSocket client connected');
      
      ws.on('close', () => {
        console.log('📡 WebSocket client disconnected');
      });
    });

    // Wait for server to be ready, then connect client
    setTimeout(() => {
      this.client.connect();
      
      // Start test scenario after client connects
      setTimeout(() => {
        this.runTestScenario();
      }, 2000);
    }, 1000);
  }

  broadcast(data) {
    this.wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(data));
      }
    });
  }

  createMessage(site, driverName = "", fuelLevel = null) {
    const message = {
      Plate: site.plate,
      Speed: 0,
      Latitude: -26.004057,
      Longitude: 28.17784,
      LocTime: "4057",
      Quality: site.quality,
      Mileage: null,
      Pocsagstr: "",
      Head: "",
      Geozone: "Test Location, South Africa",
      DriverName: driverName,
      NameEvent: "",
      Temperature: "25,405,1004,2020,093F,2021,1404,2022,1B,2023,5E"
    };

    if (fuelLevel !== null) {
      message.fuel_probe_1_level = fuelLevel;
      message.fuel_probe_1_volume_in_tank = fuelLevel * 3;
      message.fuel_probe_1_temperature = 25 + Math.random() * 5;
      message.fuel_probe_1_level_percentage = Math.round((fuelLevel / 300) * 100);
      message.message_type = 405;
    }

    return message;
  }

  runTestScenario() {
    console.log('🧪 Starting 10-minute test scenario...');
    console.log('📋 Test Plan:');
    console.log('  - EBONY: Fuel fill 1-2 minutes');
    console.log('  - FLORENTIA: Fuel fill 3-4 minutes');
    console.log('  - BRAAMFONTE: Engine session 5-8 minutes');
    console.log('  - Total duration: 10 minutes');

    // Send regular updates every 3 seconds
    const updateInterval = setInterval(() => {
      this.sendRegularUpdates();
    }, 3000);

    // EBONY fuel fill: 1-2 minutes
    setTimeout(() => {
      console.log('⛽ Starting fuel fill for EBONY');
      this.fuelFills.set('EBONY', { startLevel: 120, targetLevel: 180, startTime: Date.now() });
    }, 60000);

    setTimeout(() => {
      console.log('⛽ Stopping fuel fill for EBONY');
      this.fuelFills.delete('EBONY');
    }, 120000);

    // FLORENTIA fuel fill: 3-4 minutes
    setTimeout(() => {
      console.log('⛽ Starting fuel fill for FLORENTIA');
      this.fuelFills.set('FLORENTIA', { startLevel: 110, targetLevel: 170, startTime: Date.now() });
    }, 180000);

    setTimeout(() => {
      console.log('⛽ Stopping fuel fill for FLORENTIA');
      this.fuelFills.delete('FLORENTIA');
    }, 240000);

    // BRAAMFONTE engine session: 5-8 minutes
    setTimeout(() => {
      console.log('🟢 Starting engine session for BRAAMFONTE');
      this.sessions.set('BRAAMFONTE', { startLevel: 200, startTime: Date.now() });
      const msg = this.createMessage(this.sites[2], "ENGINE ON", 200);
      this.broadcast(msg);
      console.log('📤 Sent ENGINE ON for BRAAMFONTE');
    }, 300000);

    setTimeout(() => {
      console.log('🔴 Stopping engine session for BRAAMFONTE');
      const session = this.sessions.get('BRAAMFONTE');
      const finalFuel = session ? session.startLevel - 15 : 185; // 15L consumed
      this.sessions.delete('BRAAMFONTE');
      const msg = this.createMessage(this.sites[2], "ENGINE OFF", finalFuel);
      this.broadcast(msg);
      console.log('📤 Sent ENGINE OFF for BRAAMFONTE');
    }, 480000);

    // Stop after 10 minutes
    setTimeout(() => {
      console.log('🛑 Test scenario complete');
      clearInterval(updateInterval);
      this.stop();
    }, 600000);
  }

  sendRegularUpdates() {
    this.sites.forEach(site => {
      let message;
      const session = this.sessions.get(site.plate);
      const fuelFill = this.fuelFills.get(site.plate);

      if (fuelFill) {
        // Fuel fill in progress
        const elapsed = (Date.now() - fuelFill.startTime) / 1000;
        const progress = Math.min(elapsed / 60, 1); // 1 minute fill
        const currentLevel = fuelFill.startLevel + (fuelFill.targetLevel - fuelFill.startLevel) * progress;
        message = this.createMessage(site, "POSSIBLE FUEL FILL", currentLevel);
      } else if (session) {
        // Engine session in progress
        const elapsed = (Date.now() - session.startTime) / 60000;
        const currentFuel = Math.max(50, session.startLevel - (elapsed * 1.5)); // 1.5L/min consumption
        message = this.createMessage(site, "ENGINE ON", currentFuel);
      } else {
        // Regular update with base fuel
        const baseFuel = 120 + Math.random() * 80;
        message = this.createMessage(site, "", baseFuel);
      }

      this.broadcast(message);
    });
  }

  stop() {
    if (this.client) {
      this.client.close();
    }
    if (this.wss) {
      this.wss.close();
    }
    console.log('🛑 Test server stopped');
  }
}

// Start the test server
const testServer = new TestServerReplacement();
testServer.start();

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down test server...');
  testServer.stop();
  process.exit(0);
});