const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8090 });

console.log('WebSocket server started on ws://localhost:8090');

wss.on('connection', (ws) => {
  console.log('Client connected');
  
  let messageCount = 0;
  const startTime = Date.now();
  
  const interval = setInterval(() => {
    const elapsed = Date.now() - startTime;
    const seconds = Math.floor(elapsed / 1000);
    
    // Stop after 5 minutes
    if (seconds >= 300) {
      clearInterval(interval);
      console.log('5 minutes completed, stopping data stream');
      return;
    }
    
    const now = new Date();
    const locTime = now.toISOString().slice(0, 19).replace('T', ' ');
    
    // Base messages for 3 vehicles
    const messages = [];
    
    // FARRAMERE - no fuel data
    messages.push({
      Plate: 'FARRAMERE',
      Speed: Math.floor(Math.random() * 60),
      Latitude: -26.14978,
      Longitude: 28.29961,
      Quality: '61.13.2.99',
      Mileage: 2462 + seconds,
      Pocsagstr: '61.13.2.99',
      Head: '',
      Geozone: '',
      DriverName: '',
      NameEvent: '',
      Temperature: '',
      LocTime: locTime,
      message_type: 405
    });
    
    // MOBILE 3 - has fuel data + PTO events
    const mobile3 = {
      Plate: 'MOBILE 3',
      Speed: Math.floor(Math.random() * 40),
      Latitude: -25.931415,
      Longitude: 27.996146,
      Quality: '60.42.2.60',
      Mileage: 758 + seconds,
      Pocsagstr: '60.42.2.60',
      Head: '',
      Geozone: '',
      DriverName: '',
      NameEvent: '',
      Temperature: '25,405,1007,2020,0320,2021,0740,2022,1F,2023,26',
      LocTime: locTime,
      message_type: 405,
      fuel_probe_1_level: '80.0',
      fuel_probe_1_volume_in_tank: (185.6 + Math.random() * 2).toFixed(1),
      fuel_probe_1_temperature: '31',
      fuel_probe_1_level_percentage: '38'
    };
    
    // PTO ON at 2 minutes (120 seconds) - no fuel data
    if (seconds === 120) {
      mobile3.DriverName = 'PTO ON';
      delete mobile3.fuel_probe_1_level;
      delete mobile3.fuel_probe_1_volume_in_tank;
      delete mobile3.fuel_probe_1_temperature;
      delete mobile3.fuel_probe_1_level_percentage;
      console.log(`[${seconds}s] PTO ON event sent`);
    }
    // PTO OFF at 4 minutes (240 seconds) - no fuel data
    else if (seconds === 240) {
      mobile3.DriverName = 'PTO OFF';
      delete mobile3.fuel_probe_1_level;
      delete mobile3.fuel_probe_1_volume_in_tank;
      delete mobile3.fuel_probe_1_temperature;
      delete mobile3.fuel_probe_1_level_percentage;
      console.log(`[${seconds}s] PTO OFF event sent`);
    }
    
    messages.push(mobile3);
    
    // RANDBURG - has fuel data + fuel fill event
    const randburg = {
      Plate: 'RANDBURG',
      Speed: 0,
      Latitude: -26.09388,
      Longitude: 28.00511,
      Quality: '62.138.2.81',
      Mileage: 2761 + seconds,
      Pocsagstr: '62.138.2.81',
      Head: '',
      Geozone: '',
      DriverName: '',
      NameEvent: '',
      Temperature: '25,405,1007,2020,0320,2021,0740,2022,1F,2023,26',
      LocTime: locTime,
      message_type: 405,
      fuel_probe_1_level: '50.0',
      fuel_probe_1_volume_in_tank: '120.5',
      fuel_probe_1_temperature: '28',
      fuel_probe_1_level_percentage: '50'
    };
    
    // Possible Fuel Fill at 1 minute (60 seconds) - no fuel data
    if (seconds === 60) {
      randburg.DriverName = 'POSSIBLE FUEL FILL';
      delete randburg.fuel_probe_1_level;
      delete randburg.fuel_probe_1_volume_in_tank;
      delete randburg.fuel_probe_1_temperature;
      delete randburg.fuel_probe_1_level_percentage;
      console.log(`[${seconds}s] POSSIBLE FUEL FILL event sent`);
    }
    // Simulate fuel increasing after fill starts
    else if (seconds > 60 && seconds <= 180) {
      const fillProgress = (seconds - 60) / 120;
      randburg.fuel_probe_1_volume_in_tank = (120.5 + fillProgress * 80).toFixed(1);
      randburg.fuel_probe_1_level_percentage = Math.min(95, 50 + Math.floor(fillProgress * 45)).toString();
    }
    
    messages.push(randburg);
    
    // Send all messages
    messages.forEach(msg => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(msg));
        messageCount++;
      }
    });
    
    if (seconds % 30 === 0) {
      console.log(`[${seconds}s] Sent ${messageCount} messages`);
    }
    
  }, 2000); // Send every 2 seconds
  
  ws.on('close', () => {
    clearInterval(interval);
    console.log('Client disconnected');
  });
});
