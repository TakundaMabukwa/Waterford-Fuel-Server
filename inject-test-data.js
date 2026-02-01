const WebSocket = require('ws');

// Test data to inject
const testData = [
  {
    "Plate": "TEST001",
    "DriverName": "ENGINE ON",
    "fuel_probe_1_level": "45.0", 
    "fuel_probe_1_level_percentage": "67",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "RUNNING",
    "fuel_probe_1_level": "43.2",
    "fuel_probe_1_level_percentage": "64", 
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001", 
    "DriverName": "POSSIBLE FUEL FILL",
    "fuel_probe_1_level": "42.8",
    "fuel_probe_1_level_percentage": "63",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "",
    "fuel_probe_1_level": "68.5",
    "fuel_probe_1_level_percentage": "95",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "RUNNING", 
    "fuel_probe_1_level": "67.8",
    "fuel_probe_1_level_percentage": "94",
    "Quality": "192.168.1.100"
  },
  {
    "Plate": "TEST001",
    "DriverName": "ENGINE OFF",
    "fuel_probe_1_level": "66.2",
    "fuel_probe_1_level_percentage": "92",
    "Quality": "192.168.1.100"
  }
];

// Create WebSocket server that your app will connect to
const wss = new WebSocket.Server({ port: 8009 });

console.log('🧪 Test Data Injection Server started on ws://localhost:8009');
console.log('📡 Your app should connect to: ws://localhost:8009\n');

wss.on('connection', (ws) => {
  console.log('✅ Client connected - starting test sequence\n');
  
  let step = 0;
  
  const sendNext = () => {
    if (step < testData.length) {
      const message = testData[step];
      console.log(`📤 Step ${step + 1}: Sending "${message.DriverName || 'Empty'}" (${message.fuel_probe_1_level}L)`);
      
      ws.send(JSON.stringify(message));
      step++;
      
      setTimeout(sendNext, 4000); // 4 second intervals
    } else {
      console.log('\n✅ All test data sent');
    }
  };
  
  // Start sending after 2 seconds
  setTimeout(sendNext, 2000);
  
  ws.on('close', () => {
    console.log('❌ Client disconnected');
  });
});

console.log('📋 Test sequence ready:');
console.log('1. ENGINE ON (45.0L)');
console.log('2. RUNNING (43.2L)');
console.log('3. POSSIBLE FUEL FILL (42.8L)');
console.log('4. Empty status (68.5L) = +25.7L');
console.log('5. RUNNING (67.8L)');
console.log('6. ENGINE OFF (66.2L)');
console.log('\n⏳ Waiting for your app to connect...');