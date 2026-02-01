const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8008 });

console.log('🧪 Dummy WebSocket Server started on ws://localhost:8008');

const testSequence = [
  // 1. Engine starts
  {
    "Plate": "TEST001",
    "DriverName": "ENGINE ON",
    "fuel_probe_1_level": "45.0",
    "fuel_probe_1_level_percentage": "67",
    "Quality": "192.168.1.100"
  },
  // 2. Normal operation
  {
    "Plate": "TEST001", 
    "DriverName": "RUNNING",
    "fuel_probe_1_level": "43.2",
    "fuel_probe_1_level_percentage": "64",
    "Quality": "192.168.1.100"
  },
  // 3. Fuel fill starts
  {
    "Plate": "TEST001",
    "DriverName": "POSSIBLE FUEL FILL",
    "fuel_probe_1_level": "42.8",
    "fuel_probe_1_level_percentage": "63",
    "Quality": "192.168.1.100"
  },
  // 4. Fuel fill ends (status becomes empty)
  {
    "Plate": "TEST001",
    "DriverName": "",
    "fuel_probe_1_level": "68.5",
    "fuel_probe_1_level_percentage": "95",
    "Quality": "192.168.1.100"
  },
  // 5. Continue operation
  {
    "Plate": "TEST001",
    "DriverName": "RUNNING", 
    "fuel_probe_1_level": "67.8",
    "fuel_probe_1_level_percentage": "94",
    "Quality": "192.168.1.100"
  },
  // 6. Engine stops
  {
    "Plate": "TEST001",
    "DriverName": "ENGINE OFF",
    "fuel_probe_1_level": "66.2",
    "fuel_probe_1_level_percentage": "92",
    "Quality": "192.168.1.100"
  }
];

wss.on('connection', (ws) => {
  console.log('✅ Client connected');
  
  let step = 0;
  
  const sendNext = () => {
    if (step < testSequence.length) {
      const message = testSequence[step];
      console.log(`📤 Sending step ${step + 1}: ${message.DriverName || 'Empty status'}`);
      ws.send(JSON.stringify(message));
      step++;
      
      setTimeout(sendNext, 3000); // 3 second intervals
    } else {
      console.log('✅ Test sequence completed');
    }
  };
  
  // Start sending after 1 second
  setTimeout(sendNext, 1000);
  
  ws.on('close', () => {
    console.log('❌ Client disconnected');
  });
});

console.log('📋 Test sequence:');
console.log('1. ENGINE ON (45.0L)');
console.log('2. RUNNING (43.2L)'); 
console.log('3. POSSIBLE FUEL FILL (42.8L)');
console.log('4. Empty status (68.5L) = +25.7L filled');
console.log('5. RUNNING (67.8L)');
console.log('6. ENGINE OFF (66.2L)');
console.log('\n🔌 Waiting for client connection...');