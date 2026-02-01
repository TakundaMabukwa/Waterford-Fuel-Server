const WebSocket = require('ws');

class TestWebSocketClient {
  constructor() {
    this.vehicles = [
      { plate: 'KEYWEST', fuel: 121.6, percentage: 50 },
      { plate: 'ALEX', fuel: 185.7, percentage: 84 },
      { plate: 'BLOEM2', fuel: 315, percentage: 100 },
      { plate: 'DURBANVILL', fuel: 200, percentage: 75 },
      { plate: 'BERGBRON', fuel: 150, percentage: 60 }
    ];
    this.step = 0;
  }

  connect() {
    console.log('🧪 Test WebSocket Client - Connecting to your server.js');
    console.log('📡 Connecting to: ws://localhost:4000 (your actual server)\n');
    
    this.ws = new WebSocket('ws://localhost:4000');
    
    this.ws.on('open', () => {
      console.log('✅ Connected to your server.js');
      console.log('🚀 Starting to send test data...\n');
      this.startSendingData();
    });
    
    this.ws.on('message', (data) => {
      console.log('📥 Received from server:', data.toString());
    });
    
    this.ws.on('close', () => {
      console.log('❌ Disconnected from server');
    });
    
    this.ws.on('error', (error) => {
      console.error('❌ WebSocket error:', error.message);
    });
  }

  startSendingData() {
    setInterval(() => {
      this.step++;
      
      this.vehicles.forEach((vehicle, index) => {
        let driverName = '';
        let fuelChange = 0;
        
        // Simulate fuel fill for KEYWEST every 20 steps
        if (vehicle.plate === 'KEYWEST') {
          if (this.step % 20 === 5) {
            driverName = 'POSSIBLE FUEL FILL';
            console.log(`⛽ KEYWEST: Starting fuel fill at ${vehicle.fuel}L`);
          } else if (this.step % 20 >= 6 && this.step % 20 <= 10) {
            driverName = 'POSSIBLE FUEL FILL';
            fuelChange = Math.random() * 5 + 2; // 2-7L increase during fill
          } else if (this.step % 20 === 11) {
            driverName = ''; // End fuel fill
            console.log(`⛽ KEYWEST: Fuel fill ended at ${vehicle.fuel}L`);
          } else if (this.step % 20 === 15) {
            driverName = 'ENGINE ON';
          } else if (this.step % 20 === 18) {
            driverName = 'ENGINE OFF';
          }
        }
        
        // Simulate engine operations for ALEX
        if (vehicle.plate === 'ALEX') {
          if (this.step % 25 === 3) {
            driverName = 'ENGINE ON';
          } else if (this.step % 25 === 20) {
            driverName = 'ENGINE OFF';
          } else if (this.step % 25 >= 4 && this.step % 25 <= 19) {
            driverName = 'RUNNING';
            fuelChange = -(Math.random() * 1 + 0.5); // Fuel consumption
          }
        }
        
        // Normal fuel consumption for others
        if (!driverName.includes('FILL') && driverName !== 'ENGINE ON') {
          fuelChange = -(Math.random() * 0.3); // Small consumption
        }
        
        vehicle.fuel += fuelChange;
        vehicle.fuel = Math.max(10, Math.min(350, vehicle.fuel));
        vehicle.percentage = Math.round((vehicle.fuel / 350) * 100);
        
        const message = {
          Plate: vehicle.plate,
          Speed: Math.floor(Math.random() * 60),
          Latitude: -26.088094 + (Math.random() - 0.5) * 0.1,
          Longitude: 27.782602 + (Math.random() - 0.5) * 0.1,
          LocTime: Date.now().toString(),
          Quality: "192.168.1." + (100 + index),
          Mileage: null,
          Pocsagstr: "",
          Head: "",
          Geozone: "Test Location, Johannesburg, South Africa",
          DriverName: driverName,
          NameEvent: "",
          Temperature: "25,405,1004,2020,0741,2021,05A5,2022,14,2023,54",
          fuel_probe_1_level: parseFloat(vehicle.fuel.toFixed(1)),
          fuel_probe_1_volume_in_tank: vehicle.fuel * 2,
          fuel_probe_1_temperature: 20 + Math.random() * 10,
          fuel_probe_1_level_percentage: vehicle.percentage,
          message_type: 405
        };
        
        if (this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify(message));
          
          if (driverName) {
            console.log(`📤 ${vehicle.plate}: "${driverName}" (${vehicle.fuel.toFixed(1)}L)`);
          }
        }
      });
      
    }, 2000); // Every 2 seconds
  }
}

const client = new TestWebSocketClient();
client.connect();

console.log('📋 Test Scenarios:');
console.log('- KEYWEST: Fuel fills every 20 steps');
console.log('- ALEX: Engine ON/OFF cycles');
console.log('- Others: Normal fuel consumption');
console.log('\n⏳ Make sure your server.js is running on port 4000...');