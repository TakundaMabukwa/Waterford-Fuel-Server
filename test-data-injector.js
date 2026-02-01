const WebSocket = require('ws');

class TestDataInjector {
  constructor() {
    this.ws = null;
    this.isRunning = false;
    this.sites = [
      {
        plate: "EBONY",
        latitude: -26.004057,
        longitude: 28.17784,
        locTime: "4057",
        quality: "60.115.1.10",
        geozone: "1031 29 September Dr, Ebony Park, Midrand, 1690, South Africa"
      },
      {
        plate: "FLORENTIA", 
        latitude: -26.26748,
        longitude: 28.12673,
        locTime: "19549",
        quality: "61.13.2.35",
        geozone: "98 2nd Avenue, Alberton, 1449, South Africa"
      },
      {
        plate: "BRAAMFONTE",
        latitude: -26.19409,
        longitude: 28.03482,
        locTime: "1075", 
        quality: "53.16.1.59",
        geozone: "Braamfontein Bus Stop, Melle Street, Johannesburg 2000, South Africa"
      },
      {
        plate: "RYNFIELD",
        latitude: -26.13608,
        longitude: 28.34938,
        locTime: "162",
        quality: "53.15.1.99", 
        geozone: "238 Pretoria Road, Benoni, 1514, South Africa"
      }
    ];
    
    this.sessions = new Map();
    this.fuelFills = new Map();
  }

  connect() {
    this.ws = new WebSocket('ws://64.227.138.235:8005');
    
    this.ws.on('open', () => {
      console.log('🔌 Connected to actual server');
      this.startTestScenario();
    });

    this.ws.on('error', (error) => {
      console.error('❌ Connection error:', error);
    });

    this.ws.on('close', () => {
      console.log('🔌 Disconnected from server');
    });
  }

  sendMessage(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
      console.log(`📤 Sent: ${data.Plate} - Speed: ${data.Speed}, Driver: ${data.DriverName || 'None'}, Fuel: ${data.fuel_probe_1_level || 'None'}`);
    }
  }

  createBaseMessage(site) {
    return {
      Plate: site.plate,
      Speed: 0,
      Latitude: site.latitude,
      Longitude: site.longitude,
      LocTime: site.locTime,
      Quality: site.quality,
      Mileage: null,
      Pocsagstr: "",
      Head: "",
      Geozone: site.geozone,
      DriverName: "",
      NameEvent: "",
      Temperature: ""
    };
  }

  startEngineSession(site) {
    console.log(`🟢 Starting engine session for ${site.plate}`);
    this.sessions.set(site.plate, {
      startTime: Date.now(),
      fuelLevel: 150 + Math.random() * 50 // 150-200L starting fuel
    });
  }

  stopEngineSession(site) {
    const session = this.sessions.get(site.plate);
    if (session) {
      console.log(`🔴 Stopping engine session for ${site.plate}`);
      this.sessions.delete(site.plate);
    }
  }

  startFuelFill(site) {
    console.log(`⛽ Starting fuel fill for ${site.plate}`);
    const startLevel = 100 + Math.random() * 30; // 100-130L starting
    this.fuelFills.set(site.plate, {
      startTime: Date.now(),
      startLevel: startLevel,
      targetLevel: startLevel + 50 + Math.random() * 30 // Add 50-80L
    });
  }

  stopFuelFill(site) {
    const fill = this.fuelFills.get(site.plate);
    if (fill) {
      console.log(`⛽ Stopping fuel fill for ${site.plate}`);
      this.fuelFills.delete(site.plate);
    }
  }

  sendRegularUpdate() {
    this.sites.forEach(site => {
      const message = this.createBaseMessage(site);
      const session = this.sessions.get(site.plate);
      const fuelFill = this.fuelFills.get(site.plate);

      // Always add basic fuel data (simulate real vehicles)
      const baseFuel = 120 + Math.random() * 80; // 120-200L base
      message.Temperature = "25,405,1004,2020,093F,2021,1404,2022,1B,2023,5E";
      message.fuel_probe_1_level = Math.round(baseFuel * 10) / 10;
      message.fuel_probe_1_volume_in_tank = Math.round((baseFuel * 3) * 10) / 10;
      message.fuel_probe_1_temperature = 25 + Math.random() * 5;
      message.fuel_probe_1_level_percentage = Math.round((baseFuel / 300) * 100);
      message.message_type = 405;

      // Fuel fill status (overrides base fuel data)
      if (fuelFill) {
        message.DriverName = "POSSIBLE FUEL FILL";
        const elapsed = (Date.now() - fuelFill.startTime) / 1000;
        const progress = Math.min(elapsed / 60, 1); // 1 minute fill duration
        const currentLevel = fuelFill.startLevel + (fuelFill.targetLevel - fuelFill.startLevel) * progress;
        
        message.fuel_probe_1_level = Math.round(currentLevel * 10) / 10;
        message.fuel_probe_1_volume_in_tank = Math.round((currentLevel * 3) * 10) / 10;
        message.fuel_probe_1_level_percentage = Math.round((currentLevel / 300) * 100);
      } else if (session) {
        // Engine status with fuel consumption
        message.DriverName = Math.random() > 0.5 ? "ENGINE ON" : "PTO ON";
        const elapsedMinutes = (Date.now() - session.startTime) / 60000;
        const currentFuel = Math.max(50, session.fuelLevel - (elapsedMinutes * 1.5)); // 1.5L per minute consumption
        
        message.fuel_probe_1_level = Math.round(currentFuel * 10) / 10;
        message.fuel_probe_1_volume_in_tank = Math.round((currentFuel * 3) * 10) / 10;
        message.fuel_probe_1_level_percentage = Math.round((currentFuel / 300) * 100);
      }

      this.sendMessage(message);
    });
  }

  startTestScenario() {
    console.log('🧪 Starting 10-minute test scenario...');
    console.log('📋 Test Plan:');
    console.log('  - 2 sites with fuel fill (1 minute each)');
    console.log('  - 1 site with engine on (3 minutes) then off');
    console.log('  - Total duration: 10 minutes');
    this.isRunning = true;

    // Send regular updates every 3 seconds
    const updateInterval = setInterval(() => {
      if (!this.isRunning) {
        clearInterval(updateInterval);
        return;
      }
      this.sendRegularUpdate();
    }, 3000);

    // Test timeline - 10 minutes total (600 seconds)
    
    // FUEL FILL SITE 1 (EBONY) - Start at 1 minute, run for 1 minute
    setTimeout(() => {
      console.log('⛽ Starting fuel fill for EBONY (1 minute duration)');
      this.startFuelFill(this.sites[0]);
    }, 60000); // 1 minute
    
    setTimeout(() => {
      console.log('⛽ Stopping fuel fill for EBONY');
      this.stopFuelFill(this.sites[0]);
    }, 120000); // 2 minutes (1 minute duration)
    
    // FUEL FILL SITE 2 (FLORENTIA) - Start at 3 minutes, run for 1 minute
    setTimeout(() => {
      console.log('⛽ Starting fuel fill for FLORENTIA (1 minute duration)');
      this.startFuelFill(this.sites[1]);
    }, 180000); // 3 minutes
    
    setTimeout(() => {
      console.log('⛽ Stopping fuel fill for FLORENTIA');
      this.stopFuelFill(this.sites[1]);
    }, 240000); // 4 minutes (1 minute duration)
    
    // ENGINE SESSION (BRAAMFONTE) - Start at 5 minutes, run for 3 minutes
    setTimeout(() => {
      console.log('🟢 Starting engine session for BRAAMFONTE (3 minutes duration)');
      this.startEngineSession(this.sites[2]);
    }, 300000); // 5 minutes
    
    setTimeout(() => {
      console.log('🔴 Stopping engine session for BRAAMFONTE');
      const session = this.sessions.get(this.sites[2].plate);
      this.stopEngineSession(this.sites[2]);
      
      // Send ENGINE OFF message with consumed fuel
      const message = this.createBaseMessage(this.sites[2]);
      message.DriverName = "ENGINE OFF";
      message.Temperature = "25,405,1004,2020,093F,2021,1404,2022,1B,2023,5E";
      
      if (session) {
        const elapsedMinutes = (Date.now() - session.startTime) / 60000;
        const finalFuel = Math.max(50, session.fuelLevel - (elapsedMinutes * 1.5));
        message.fuel_probe_1_level = Math.round(finalFuel * 10) / 10;
        message.fuel_probe_1_volume_in_tank = Math.round((finalFuel * 3) * 10) / 10;
        message.fuel_probe_1_level_percentage = Math.round((finalFuel / 300) * 100);
      } else {
        message.fuel_probe_1_level = 140;
        message.fuel_probe_1_volume_in_tank = 420;
        message.fuel_probe_1_level_percentage = 47;
      }
      
      message.fuel_probe_1_temperature = 30;
      message.message_type = 405;
      this.sendMessage(message);
    }, 480000); // 8 minutes (3 minute duration)

    // Stop after 10 minutes
    setTimeout(() => {
      console.log('🛑 Test scenario complete - 10 minutes elapsed');
      this.isRunning = false;
      clearInterval(updateInterval);
      this.ws.close();
    }, 600000); // 10 minutes
  }
}

const injector = new TestDataInjector();
injector.connect();

process.on('SIGINT', () => {
  console.log('\n🛑 Stopping test...');
  injector.isRunning = false;
  if (injector.ws) injector.ws.close();
  process.exit(0);
});