/**
 * Comprehensive WebSocket Test - Tests all fuel tracking scenarios
 * 
 * Scenarios:
 * 1. Pre-fill tracking (lowest fuel before FUEL FILL status)
 * 2. Engine ON using fuel data BEFORE the status (closest before LocTime)
 * 3. Engine OFF using fuel data AFTER the status
 * 4. Fuel fill completion (highest fuel after status disappears)
 * 
 * Run this FIRST, then start your main server in another terminal
 */

const WebSocket = require('ws');

const PORT = 8090;
const wss = new WebSocket.Server({ port: PORT });

console.log(`🚀 Dummy WebSocket Server running on ws://localhost:${PORT}`);
console.log('📋 Waiting for client connection...\n');

// Helper to format time
function formatTime(date) {
  return date.toISOString().replace('T', ' ').slice(0, 19);
}

// Create a message with fuel data
function createFuelMessage(plate, locTime, fuelVolume, fuelPercentage, driverName = '') {
  const fuelLevel = (fuelVolume * 0.305).toFixed(1); // Approximate level from volume
  return {
    Plate: plate,
    Speed: 0,
    Latitude: -25.703903,
    Longitude: 28.24175,
    Quality: "60.41.2.179",
    Mileage: 2209,
    Pocsagstr: "60.41.2.179",
    Head: "",
    Geozone: "",
    DriverName: driverName,
    NameEvent: "",
    Temperature: "25,405,1007,2020,08DC,2021,1D0B,2022,18,2023,4B",
    LocTime: locTime,
    message_type: 405,
    fuel_probe_1_level: fuelLevel,
    fuel_probe_1_volume_in_tank: fuelVolume.toString(),
    fuel_probe_1_temperature: "24",
    fuel_probe_1_level_percentage: fuelPercentage.toString()
  };
}

// Create a status message WITHOUT fuel data
function createStatusMessage(plate, locTime, driverName) {
  return {
    Plate: plate,
    Speed: 0,
    Latitude: -26.17912,
    Longitude: 28.25583,
    Quality: "60.115.1.52",
    Mileage: 282,
    Pocsagstr: "60.115.1.52",
    Head: "",
    Geozone: "",
    DriverName: driverName,
    NameEvent: "",
    Temperature: "",
    LocTime: locTime,
    message_type: 405
  };
}

wss.on('connection', (ws) => {
  console.log('✅ Client connected!\n');
  console.log('═'.repeat(60));
  console.log('STARTING COMPREHENSIVE TEST SEQUENCE');
  console.log('═'.repeat(60));
  
  const baseTime = new Date();
  let messageIndex = 0;
  
  // Test scenarios timeline (compressed to ~3 minutes for testing)
  const scenarios = [
    // ============ SITE 1: WAVERLY - Engine Session Test ============
    // Build up fuel history BEFORE engine on
    { delay: 0, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime())), 500, 68), desc: '📊 WAVERLY: Fuel reading 500L (building history)' },
    { delay: 5000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 5000)), 498, 67), desc: '📊 WAVERLY: Fuel reading 498L (building history)' },
    { delay: 10000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 10000)), 495, 67), desc: '📊 WAVERLY: Fuel reading 495L ← Should be used as OPENING fuel' },
    
    // Engine ON - no fuel data in message
    { delay: 15000, msg: () => createStatusMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 15000)), 'PTO ON'), desc: '🟢 WAVERLY: ENGINE ON (no fuel data) - Should use 495L from BEFORE' },
    
    // Fuel readings AFTER engine on (should NOT be used for opening)
    { delay: 20000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 20000)), 493, 66), desc: '📊 WAVERLY: Fuel reading 493L (after ON - consuming)' },
    { delay: 30000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 30000)), 490, 66), desc: '📊 WAVERLY: Fuel reading 490L (consuming)' },
    { delay: 45000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 45000)), 485, 65), desc: '📊 WAVERLY: Fuel reading 485L (consuming)' },
    { delay: 60000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 60000)), 480, 65), desc: '📊 WAVERLY: Fuel reading 480L (consuming)' },
    
    // Engine OFF - no fuel data in message
    { delay: 75000, msg: () => createStatusMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 75000)), 'PTO OFF'), desc: '🔴 WAVERLY: ENGINE OFF (no fuel data) - Should wait for AFTER reading' },
    
    // Fuel reading AFTER engine off (should be used for closing)
    { delay: 80000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 80000)), 478, 64), desc: '📊 WAVERLY: Fuel reading 478L ← Should be used as CLOSING fuel' },
    
    // ============ SITE 2: KEYWEST - Fuel Fill Test ============
    // Pre-fill readings (tracking lowest)
    { delay: 10000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 10000)), 200, 30), desc: '📊 KEYWEST: Fuel reading 200L (pre-fill tracking starts)' },
    { delay: 20000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 20000)), 180, 27), desc: '📊 KEYWEST: Fuel reading 180L (lower - tracking)' },
    { delay: 35000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 35000)), 150, 23), desc: '📊 KEYWEST: Fuel reading 150L (LOWEST - should be opening fuel)' },
    { delay: 45000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 45000)), 155, 24), desc: '📊 KEYWEST: Fuel reading 155L (slightly higher - 150 still lowest)' },
    
    // FUEL FILL status appears
    { delay: 50000, msg: () => createStatusMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 50000)), 'FUEL FILL'), desc: '⛽ KEYWEST: FUEL FILL STATUS - Opening should be 150L (lowest before)' },
    
    // During fill - fuel increasing
    { delay: 55000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 55000)), 250, 38, 'FUEL FILL'), desc: '📊 KEYWEST: Fuel reading 250L (filling...)' },
    { delay: 65000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 65000)), 400, 60, 'FUEL FILL'), desc: '📊 KEYWEST: Fuel reading 400L (filling...)' },
    { delay: 75000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 75000)), 550, 75, 'FUEL FILL'), desc: '📊 KEYWEST: Fuel reading 550L (filling...)' },
    
    // FUEL FILL status disappears
    { delay: 85000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 85000)), 600, 80), desc: '📊 KEYWEST: Status gone - 600L - Watcher starts tracking highest' },
    
    // Post-fill readings (tracking highest)
    { delay: 95000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 95000)), 620, 82), desc: '📊 KEYWEST: Fuel reading 620L (higher - tracking)' },
    { delay: 105000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 105000)), 650, 85), desc: '📊 KEYWEST: Fuel reading 650L (HIGHEST - should be closing fuel)' },
    { delay: 115000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 115000)), 648, 85), desc: '📊 KEYWEST: Fuel reading 648L (slightly lower - 650 still highest)' },
    
    // ============ SITE 3: DURBANVILL - Combined Session + Fill ============
    // Engine session with fuel fill in the middle
    { delay: 5000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 5000)), 400, 55), desc: '📊 DURBANVILL: Fuel reading 400L (building history)' },
    { delay: 25000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 25000)), 395, 54), desc: '📊 DURBANVILL: Fuel reading 395L ← Opening for session' },
    
    // Engine ON
    { delay: 30000, msg: () => createStatusMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 30000)), 'PTO ON'), desc: '🟢 DURBANVILL: ENGINE ON - Should use 395L from BEFORE' },
    
    // Consuming fuel
    { delay: 40000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 40000)), 380, 52), desc: '📊 DURBANVILL: Fuel reading 380L (consuming)' },
    { delay: 55000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 55000)), 350, 48), desc: '📊 DURBANVILL: Fuel reading 350L (consuming - low!)' },
    
    // Fuel fill during session
    { delay: 60000, msg: () => createStatusMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 60000)), 'FUEL FILL'), desc: '⛽ DURBANVILL: FUEL FILL during engine session!' },
    { delay: 70000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 70000)), 500, 68, 'FUEL FILL'), desc: '📊 DURBANVILL: Fuel reading 500L (filling during session)' },
    { delay: 80000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 80000)), 600, 80), desc: '📊 DURBANVILL: Fill done, 600L - Status gone' },
    
    // Continue session after fill
    { delay: 90000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 90000)), 595, 79), desc: '📊 DURBANVILL: Fuel reading 595L (consuming after fill)' },
    { delay: 100000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 100000)), 590, 78), desc: '📊 DURBANVILL: Fuel reading 590L (consuming)' },
    
    // Engine OFF
    { delay: 110000, msg: () => createStatusMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 110000)), 'PTO OFF'), desc: '🔴 DURBANVILL: ENGINE OFF' },
    { delay: 115000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 115000)), 588, 78), desc: '📊 DURBANVILL: Fuel reading 588L ← Closing fuel' },
    
    // ============ SITE 4: BAMBANANI - PASSIVE FILL DETECTION TEST ============
    // This simulates a fill WITHOUT the "FUEL FILL" status message
    // System should detect 10L+ increase within 2 minutes
    { delay: 120000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 120000)), 300, 45), desc: '📊 BAMBANANI: Fuel reading 300L (baseline)' },
    { delay: 125000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 125000)), 295, 44), desc: '📊 BAMBANANI: Fuel reading 295L (slight decrease)' },
    { delay: 130000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 130000)), 280, 42), desc: '📊 BAMBANANI: Fuel reading 280L (lowest - should be opening)' },
    // Now fuel increases rapidly WITHOUT FUEL FILL status - passive detection should trigger
    { delay: 135000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 135000)), 320, 48), desc: '📊 BAMBANANI: Fuel reading 320L (+40L in 5s - PASSIVE FILL should trigger!)' },
    { delay: 140000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 140000)), 380, 57), desc: '📊 BAMBANANI: Fuel reading 380L (filling continues...)' },
    { delay: 145000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 145000)), 450, 68), desc: '📊 BAMBANANI: Fuel reading 450L (filling continues...)' },
    { delay: 150000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 150000)), 500, 75), desc: '📊 BAMBANANI: Fuel reading 500L (HIGHEST - should be closing)' },
    { delay: 155000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 155000)), 498, 74), desc: '📊 BAMBANANI: Fuel reading 498L (slight decrease after fill)' },
    
    // ============ Continued readings to keep connection alive ============
    { delay: 165000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 165000)), 475, 64), desc: '📊 WAVERLY: Regular reading 475L' },
    { delay: 175000, msg: () => createFuelMessage('KEYWEST', formatTime(new Date(baseTime.getTime() + 175000)), 645, 85), desc: '📊 KEYWEST: Regular reading 645L' },
    { delay: 185000, msg: () => createFuelMessage('DURBANVILL', formatTime(new Date(baseTime.getTime() + 185000)), 585, 77), desc: '📊 DURBANVILL: Regular reading 585L' },
    { delay: 195000, msg: () => createFuelMessage('BAMBANANI', formatTime(new Date(baseTime.getTime() + 195000)), 495, 74), desc: '📊 BAMBANANI: Regular reading 495L' },
    { delay: 210000, msg: () => createFuelMessage('WAVERLY', formatTime(new Date(baseTime.getTime() + 210000)), 470, 63), desc: '📊 WAVERLY: Regular reading 470L' },
  ];
  
  // Sort by delay
  scenarios.sort((a, b) => a.delay - b.delay);
  
  console.log(`\n📋 Scheduled ${scenarios.length} messages over ${Math.max(...scenarios.map(s => s.delay)) / 1000}s\n`);
  console.log('Expected Results:');
  console.log('─'.repeat(60));
  console.log('WAVERLY Session:');
  console.log('  Opening Fuel: 495L (from BEFORE Engine ON)');
  console.log('  Closing Fuel: 478L (from AFTER Engine OFF)');
  console.log('  Usage: 17L');
  console.log('');
  console.log('KEYWEST Fill (with status):');
  console.log('  Opening Fuel: 150L (LOWEST before FUEL FILL status)');
  console.log('  Closing Fuel: 650L (HIGHEST after status gone)');
  console.log('  Fill Amount: 500L');
  console.log('');
  console.log('DURBANVILL Session + Fill:');
  console.log('  Session Opening: 395L (from BEFORE Engine ON)');
  console.log('  Fill during session: ~250L (350→600)');
  console.log('  Session Closing: 588L (from AFTER Engine OFF)');
  console.log('');
  console.log('BAMBANANI Passive Fill (NO status message):');
  console.log('  Opening Fuel: 280L (lowest before +10L increase)');
  console.log('  Closing Fuel: 500L (HIGHEST after increase detected)');
  console.log('  Fill Amount: 220L (detected WITHOUT FUEL FILL status!)');
  console.log('─'.repeat(60));
  console.log('');
  
  // Schedule all messages
  scenarios.forEach((scenario, index) => {
    setTimeout(() => {
      const msg = scenario.msg();
      const jsonStr = JSON.stringify(msg);
      
      console.log(`\n[${index + 1}/${scenarios.length}] ${new Date().toLocaleTimeString()} - ${scenario.desc}`);
      console.log(`    Sending: ${msg.Plate} | LocTime: ${msg.LocTime} | Driver: "${msg.DriverName || '-'}" | Fuel: ${msg.fuel_probe_1_volume_in_tank || 'N/A'}L`);
      
      ws.send(jsonStr);
      messageIndex++;
      
      if (index === scenarios.length - 1) {
        console.log('\n' + '═'.repeat(60));
        console.log('✅ ALL TEST MESSAGES SENT');
        console.log('═'.repeat(60));
        console.log('\n⏳ Waiting 10 minutes for fuel fill watcher to complete...');
        console.log('   (The fill watcher has a 10-minute timeout to find highest fuel)');
        console.log('\nCheck your server logs and database for results!');
      }
    }, scenario.delay);
  });
});

wss.on('close', () => {
  console.log('WebSocket server closed');
});

console.log('\n📝 Instructions:');
console.log('1. Keep this terminal running');
console.log('2. In another terminal, start your server: npm start');
console.log('3. Watch the logs for fuel tracking behavior');
console.log('4. Check database for created sessions and fills\n');
