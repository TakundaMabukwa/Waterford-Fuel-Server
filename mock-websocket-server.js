const WebSocket = require('ws');

// Create WebSocket server on port 8005 (same as your .env WEBSOCKET_URL)
const wss = new WebSocket.Server({ port: 8005 });

const vehicles = [
  { plate: 'UNKNOWN1', fuel: 121.6789, percentage: 50.23, engineState: null, quality: '53.15.1.232' },
  // { plate: 'ALEX', fuel: 185.7432, percentage: 84.56, engineState: null, quality: '192.168.1.101' },
  // { plate: 'BLOEM2', fuel: 315.1234, percentage: 100.00, engineState: null, quality: '192.168.1.102' },
  { plate: 'UNKNOWN2', fuel: 200.5678, percentage: 75.89, engineState: null, quality: '61.172.2.170' },
  // { plate: 'BERGBRON', fuel: 150.9876, percentage: 60.12, engineState: null, quality: '62.138.2.35' }
];

const testScenarios = [
  { step: 5, vehicle: 'UNKNOWN1', action: 'ENGINE_ON' },
  { step: 10, vehicle: 'UNKNOWN1', action: 'FUEL_FILL_START' },
  { step: 12, vehicle: 'UNKNOWN1', action: 'FUEL_FILL_END' },
  { step: 20, vehicle: 'UNKNOWN1', action: 'ENGINE_OFF' },
  { step: 25, vehicle: 'UNKNOWN2', action: 'ENGINE_ON' },
  { step: 30, vehicle: 'UNKNOWN2', action: 'FUEL_FILL_START' },
  { step: 32, vehicle: 'UNKNOWN2', action: 'FUEL_FILL_END' },
  { step: 40, vehicle: 'UNKNOWN2', action: 'ENGINE_OFF' }
];

console.log('🧪 Mock WebSocket Server started on ws://localhost:8005');
console.log('📡 Testing precise timing and fuel measurements');
console.log('🔬 All fuel values use 4 decimal places for accuracy\n');

wss.on('connection', (ws) => {
  console.log('✅ Your server connected!');
  console.log('🚀 Starting to send test data...\n');
  
  let step = 0;
  
  const sendData = () => {
    step++;
    
    vehicles.forEach((vehicle, index) => {
      let driverName = '';
      let fuelChange = 0;
      
      // Check for scheduled test scenarios
      const scenario = testScenarios.find(s => s.step === step && s.vehicle === vehicle.plate);
      
      if (scenario) {
        switch (scenario.action) {
          case 'ENGINE_ON':
            vehicle.engineState = 'ON';
            vehicle.sessionStartTime = new Date().toISOString();
            driverName = 'ENGINE ON';
            console.log(`🟢 ${vehicle.plate}: ENGINE ON at ${new Date().toLocaleTimeString()} (${vehicle.fuel.toFixed(4)}L)`);
            break;
            
          case 'ENGINE_OFF':
            vehicle.engineState = 'OFF';
            const sessionEndTime = new Date().toISOString();
            const sessionDuration = vehicle.sessionStartTime ? 
              ((new Date(sessionEndTime) - new Date(vehicle.sessionStartTime)) / 1000 / 60).toFixed(1) : 'Unknown';
            driverName = 'ENGINE OFF';
            fuelChange = -(Math.random() * 2.5678 + 1.2345); // Precise consumption
            console.log(`🔴 ${vehicle.plate}: ENGINE OFF at ${new Date().toLocaleTimeString()} (Duration: ${sessionDuration}min, Consumed: ${Math.abs(fuelChange).toFixed(4)}L)`);
            vehicle.sessionStartTime = null;
            break;
            
          case 'FUEL_FILL_START':
            vehicle.fillStartTime = new Date().toISOString();
            driverName = 'Possible Fuel Fill';
            console.log(`⛽ ${vehicle.plate}: FUEL FILL DETECTED at ${new Date().toLocaleTimeString()} (${vehicle.fuel.toFixed(4)}L)`);
            break;
            
          case 'FUEL_FILL_END':
            const fillEndTime = new Date().toISOString();
            const fillDuration = vehicle.fillStartTime ? 
              ((new Date(fillEndTime) - new Date(vehicle.fillStartTime)) / 1000 / 60).toFixed(1) : 'Unknown';
            driverName = 'Normal Operation';
            fuelChange = 25.5 + Math.random() * 10; // Fill amount
            console.log(`⛽ ${vehicle.plate}: FUEL FILL COMPLETE at ${new Date().toLocaleTimeString()} (Duration: ${fillDuration}min, Added: ${fuelChange.toFixed(4)}L)`);
            vehicle.fillStartTime = null;
            break;
        }
      } else {
        // Normal operation
        if (vehicle.engineState === 'ON') {
          fuelChange = -(Math.random() * 0.1234); // Precise consumption while running
        } else {
          fuelChange = -(Math.random() * 0.0123); // Minimal consumption when off
        }
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
        Quality: vehicle.quality,
        Mileage: null,
        Pocsagstr: "",
        Head: "",
        Geozone: "Test Location, Johannesburg, South Africa",
        DriverName: driverName,
        NameEvent: "",
        Temperature: "25,405,1004,2020,0741,2021,05A5,2022,14,2023,54",
        fuel_probe_1_level: parseFloat(vehicle.fuel.toFixed(4)),
        fuel_probe_1_volume_in_tank: parseFloat((vehicle.fuel * 2.0123).toFixed(4)),
        fuel_probe_1_temperature: parseFloat((20 + Math.random() * 10).toFixed(2)),
        fuel_probe_1_level_percentage: parseFloat(vehicle.percentage.toFixed(2)),
        message_type: 405
      };
      
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
        
        if (driverName && !scenario) {
          console.log(`📤 ${vehicle.plate}: "${driverName}" (${vehicle.fuel.toFixed(4)}L)`);
        }
      }
    });
  };
  
  // Send data every 3 seconds
  const interval = setInterval(sendData, 3000);
  
  ws.on('close', () => {
    console.log('❌ Your server disconnected');
    clearInterval(interval);
  });
});

console.log('📋 Precision Test Scenarios:');
console.log('- KEYWEST: ENGINE ON (step 5) → ENGINE OFF (step 15) = 10 steps timing');
console.log('- ALEX: FUEL FILL (step 20-30) = precise fuel measurement');
console.log('- BLOEM2: ENGINE ON (step 35) → ENGINE OFF (step 50) = 15 steps timing');
console.log('- DURBANVILL: FUEL FILL (step 55-60) = quick fill test');
console.log('- All fuel levels: 4 decimal precision (e.g., 121.6789L)');
console.log('\n⏳ Waiting for your server to connect...');