const WebSocket = require('ws');

class LocalWebSocketServer {
  constructor() {
    this.wss = null;
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
    // Start WebSocket server on port 8005 (same as production)
    this.wss = new WebSocket.Server({ port: 8005 });
    console.log('🚀 Local WebSocket server running on ws://localhost:8005');
    console.log('📡 Waiting for your app to connect...');
    
    this.wss.on('connection', (ws) => {
      console.log('✅ Your app connected to local WebSocket server');
      
      // Start sending test data when app connects
      setTimeout(() => {
        this.runTestScenario();
      }, 2000);
      
      ws.on('close', () => {
        console.log('📡 Your app disconnected');
      });
    });
  }

  broadcast(data) {
    this.wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(data));
        console.log(`📤 Sent: ${data.Plate} - Driver: ${data.DriverName || 'None'}, Fuel: ${data.fuel_probe_1_level || 'None'}`);
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
    console.log('🧪 Starting fuel fill during session test...');
    console.log('📋 Test Plan:');
    console.log('  - BRAAMFONTE: Engine session 5-60s with fuel fill 20-40s');
    console.log('  - Testing: Fill during active session + usage protection');
    console.log('  - Expected: Engine session tracks fill_events + separate fill session created');

    // Send regular updates every 3 seconds
    const updateInterval = setInterval(() => {
      this.sendRegularUpdates();
    }, 3000);

    // BRAAMFONTE engine session: 5-60 seconds
    setTimeout(() => {
      console.log('🟢 Starting engine session for BRAAMFONTE');
      this.sessions.set('BRAAMFONTE', { startLevel: 200, startTime: Date.now() });
    }, 5000);

    // BRAAMFONTE fuel fill during session: 20-40 seconds
    setTimeout(() => {
      console.log('⛽ Starting fuel fill for BRAAMFONTE during engine session');
      this.fuelFills.set('BRAAMFONTE', { startLevel: 180, targetLevel: 250, startTime: Date.now() });
    }, 20000);

    setTimeout(() => {
      console.log('⛽ Stopping fuel fill for BRAAMFONTE');
      this.fuelFills.delete('BRAAMFONTE');
    }, 40000);

    setTimeout(() => {
      console.log('🔴 Stopping engine session for BRAAMFONTE');
      this.sessions.delete('BRAAMFONTE');
    }, 60000);

    // Stop after 90 seconds
    setTimeout(() => {
      console.log('🛑 Test scenario complete');
      clearInterval(updateInterval);
    }, 90000);
  }

  sendRegularUpdates() {
    // Only send BRAAMFONTE data for focused testing
    const site = this.sites.find(s => s.plate === 'BRAAMFONTE');
    const session = this.sessions.get(site.plate);
    const fuelFill = this.fuelFills.get(site.plate);
    
    let message;

    if (fuelFill && session) {
      // Both engine session and fuel fill active
      const elapsed = (Date.now() - fuelFill.startTime) / 1000;
      const progress = Math.min(elapsed / 20, 1); // 20 second fill
      const currentLevel = fuelFill.startLevel + (fuelFill.targetLevel - fuelFill.startLevel) * progress;
      message = this.createMessage(site, "ENGINE ON + POSSIBLE FUEL FILL", currentLevel);
    } else if (fuelFill) {
      // Fuel fill only
      const elapsed = (Date.now() - fuelFill.startTime) / 1000;
      const progress = Math.min(elapsed / 20, 1);
      const currentLevel = fuelFill.startLevel + (fuelFill.targetLevel - fuelFill.startLevel) * progress;
      message = this.createMessage(site, "POSSIBLE FUEL FILL", currentLevel);
    } else if (session) {
      // Engine session only
      const elapsed = (Date.now() - session.startTime) / 60000;
      const currentFuel = Math.max(50, session.startLevel - (elapsed * 2)); // 2L/min consumption
      message = this.createMessage(site, "ENGINE ON", currentFuel);
    } else {
      // Regular update
      message = this.createMessage(site, "", 200);
    }

    this.broadcast(message);
  }

  stop() {
    if (this.wss) {
      this.wss.close();
    }
    console.log('🛑 Local WebSocket server stopped');
  }
}

// Start the local WebSocket server
const server = new LocalWebSocketServer();
server.start();

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down local WebSocket server...');
  server.stop();
  process.exit(0);
});