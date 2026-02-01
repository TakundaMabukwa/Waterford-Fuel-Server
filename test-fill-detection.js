/**
 * Test for fill detection with stabilization
 * Tests both status-based and passive fill detection
 */

require('dotenv').config();
const WebSocket = require('ws');

const PORT = 8090;

// Test data - simulates a fill during engine session
const testSequence = [
  // 1. Engine starts with 100L
  {
    delay: 0,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "ENGINE ON",
      "LocTime": getLocTime(0),
      "fuel_probe_1_level": "100.0",
      "fuel_probe_1_level_percentage": "20",
      "fuel_probe_1_volume_in_tank": "100.0",
      "Quality": "192.168.1.100"
    },
    description: "Engine ON - Starting with 100L"
  },
  // 2. Normal operation, fuel drops slightly
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(2),
      "fuel_probe_1_level": "98.0",
      "fuel_probe_1_level_percentage": "19",
      "fuel_probe_1_volume_in_tank": "98.0",
      "Quality": "192.168.1.100"
    },
    description: "Running - Fuel at 98L"
  },
  // 3. Fuel drops more (before fill)
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(4),
      "fuel_probe_1_level": "95.0",
      "fuel_probe_1_level_percentage": "18",
      "fuel_probe_1_volume_in_tank": "95.0",
      "Quality": "192.168.1.100"
    },
    description: "Running - Fuel at 95L (lowest before fill)"
  },
  // 4. FILL DETECTED - Sudden increase (passive detection should trigger)
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(6),
      "fuel_probe_1_level": "150.0",
      "fuel_probe_1_level_percentage": "30",
      "fuel_probe_1_volume_in_tank": "150.0",
      "Quality": "192.168.1.100"
    },
    description: "🔥 FILL START - Fuel jumps to 150L (+55L)"
  },
  // 5. Fill continues increasing
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(8),
      "fuel_probe_1_level": "200.0",
      "fuel_probe_1_level_percentage": "40",
      "fuel_probe_1_volume_in_tank": "200.0",
      "Quality": "192.168.1.100"
    },
    description: "📈 Fill continuing - 200L"
  },
  // 6. Fill continues
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(10),
      "fuel_probe_1_level": "250.0",
      "fuel_probe_1_level_percentage": "50",
      "fuel_probe_1_volume_in_tank": "250.0",
      "Quality": "192.168.1.100"
    },
    description: "📈 Fill continuing - 250L"
  },
  // 7. Fill stabilizes (same value)
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(12),
      "fuel_probe_1_level": "250.0",
      "fuel_probe_1_level_percentage": "50",
      "fuel_probe_1_volume_in_tank": "250.0",
      "Quality": "192.168.1.100"
    },
    description: "⏸️ Fill stabilizing - still 250L"
  },
  // 8. Still stable
  {
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(14),
      "fuel_probe_1_level": "250.0",
      "fuel_probe_1_level_percentage": "50",
      "fuel_probe_1_volume_in_tank": "250.0",
      "Quality": "192.168.1.100"
    },
    description: "⏸️ Still stable at 250L"
  },
  // 9-14: More stable readings to trigger 2-min stabilization
  ...Array.from({length: 6}, (_, i) => ({
    delay: 2000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "",
      "LocTime": getLocTime(16 + i * 20), // 20 second intervals in LocTime
      "fuel_probe_1_level": "250.0",
      "fuel_probe_1_level_percentage": "50",
      "fuel_probe_1_volume_in_tank": "250.0",
      "Quality": "192.168.1.100"
    },
    description: `⏸️ Stable reading ${i + 3} at 250L`
  })),
  // Final: Engine OFF
  {
    delay: 3000,
    data: {
      "Plate": "DURBANVILL",
      "DriverName": "ENGINE OFF",
      "LocTime": getLocTime(200),
      "fuel_probe_1_level": "248.0",
      "fuel_probe_1_level_percentage": "49",
      "fuel_probe_1_volume_in_tank": "248.0",
      "Quality": "192.168.1.100"
    },
    description: "🛑 Engine OFF - Final fuel 248L"
  }
];

function getLocTime(secondsOffset) {
  const now = new Date();
  now.setSeconds(now.getSeconds() + secondsOffset);
  return now.toISOString().replace('T', ' ').split('.')[0];
}

// Create WebSocket server
const wss = new WebSocket.Server({ port: PORT });

console.log(`🧪 Fill Detection Test Server`);
console.log(`📡 WebSocket server started on ws://localhost:${PORT}`);
console.log(`\n📋 Test Sequence:`);
testSequence.forEach((step, i) => {
  console.log(`   ${i + 1}. ${step.description}`);
});
console.log(`\n⏳ Waiting for client connection...\n`);

wss.on('connection', (ws) => {
  console.log('✅ Client connected! Starting test sequence...\n');
  console.log('=' .repeat(60));
  
  let index = 0;
  
  function sendNext() {
    if (index >= testSequence.length) {
      console.log('=' .repeat(60));
      console.log('\n✅ Test sequence complete!');
      console.log('\n📊 Expected Results:');
      console.log('   - Passive fill detected at 150L (95L → 150L = +55L initial)');
      console.log('   - Highest fuel tracked: 250L');
      console.log('   - Fill completes when stable for 2 min');
      console.log('   - Final fill amount: 250L - 95L = 155L');
      console.log('\n⏱️ Wait for stabilization checker (runs every 30s)...');
      console.log('   The fill should complete when fuel hasn\'t increased for 2+ minutes\n');
      return;
    }
    
    const step = testSequence[index];
    
    setTimeout(() => {
      console.log(`📤 [${index + 1}/${testSequence.length}] ${step.description}`);
      console.log(`   Fuel: ${step.data.fuel_probe_1_volume_in_tank}L, Status: "${step.data.DriverName || '(empty)'}"`);
      ws.send(JSON.stringify(step.data));
      index++;
      sendNext();
    }, step.delay);
  }
  
  sendNext();
  
  ws.on('close', () => {
    console.log('\n❌ Client disconnected');
    process.exit(0);
  });
});

wss.on('error', (err) => {
  console.error('Server error:', err.message);
});

process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down...');
  wss.close();
  process.exit(0);
});
