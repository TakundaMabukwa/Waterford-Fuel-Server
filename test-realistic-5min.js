const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8090 });

console.log('🚀 Realistic 5-Minute Session Test - Port 8090\n');

wss.on('connection', (ws) => {
  console.log('✅ Client connected\n');
  
  const site = 'NEW ROAD';
  const baseTime = new Date();
  let currentFuel = 500;
  let messageCount = 0;
  
  console.log('📡 Starting 5-minute real-time simulation...\n');
  
  // STEP 1: ENGINE ON at 0s
  setTimeout(() => {
    const msg = {
      Plate: site,
      DriverName: 'PTO ON',
      LocTime: new Date(baseTime.getTime()).toISOString().replace('T', ' ').slice(0, -5),
      Temperature: '',
      message_type: 405
    };
    console.log(`[${++messageCount}] 00:00 - PTO ON`);
    ws.send(JSON.stringify(msg));
  }, 1000);
  
  // STEP 2: First fuel data at 5s (opening fuel)
  setTimeout(() => {
    const msg = {
      Plate: site,
      DriverName: '',
      LocTime: new Date(baseTime.getTime() + 5000).toISOString().replace('T', ' ').slice(0, -5),
      Temperature: '25,405',
      fuel_probe_1_level: '137.6',
      fuel_probe_1_volume_in_tank: currentFuel.toFixed(1),
      fuel_probe_1_temperature: '22',
      fuel_probe_1_level_percentage: '85',
      message_type: 405
    };
    console.log(`[${++messageCount}] 00:05 - Fuel: ${currentFuel}L (opening)`);
    ws.send(JSON.stringify(msg));
  }, 6000);
  
  // STEP 3-12: Regular fuel updates every 30s (consuming fuel)
  for (let i = 1; i <= 10; i++) {
    setTimeout(() => {
      const fuelAtThisTime = 500 - (i * 5); // Calculate fuel for this specific time
      const locTime = new Date(baseTime.getTime() + (5000 + i * 30000));
      
      const msg = {
        Plate: site,
        DriverName: '',
        LocTime: locTime.toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '25,405',
        fuel_probe_1_level: (fuelAtThisTime / 3.63).toFixed(1),
        fuel_probe_1_volume_in_tank: fuelAtThisTime.toFixed(1),
        fuel_probe_1_temperature: (22 + i * 0.1).toFixed(1),
        fuel_probe_1_level_percentage: Math.round((fuelAtThisTime / 585) * 100).toString(),
        message_type: 405
      };
      
      const minutes = Math.floor((5 + i * 30) / 60);
      const seconds = (5 + i * 30) % 60;
      console.log(`[${++messageCount}] ${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')} - Fuel: ${fuelAtThisTime}L`);
      ws.send(JSON.stringify(msg));
    }, 6000 + (i * 30000));
  }
  
  // STEP 13: FUEL FILL START at 3:05 (450L before fill)
  setTimeout(() => {
    const fuelBeforeFill = 450;
    const locTime = new Date(baseTime.getTime() + 185000); // 3:05
    const msg = {
      Plate: site,
      DriverName: 'POSSIBLE FUEL FILL',
      LocTime: locTime.toISOString().replace('T', ' ').slice(0, -5),
      Temperature: '25,405',
      fuel_probe_1_level: (fuelBeforeFill / 3.63).toFixed(1),
      fuel_probe_1_volume_in_tank: fuelBeforeFill.toFixed(1),
      fuel_probe_1_temperature: '22.5',
      fuel_probe_1_level_percentage: Math.round((fuelBeforeFill / 585) * 100).toString(),
      message_type: 405
    };
    console.log(`[${++messageCount}] 03:05 - ⛽ FUEL FILL START (${fuelBeforeFill}L)`);
    ws.send(JSON.stringify(msg));
  }, 150000); // Send at 2:30 (out of order!)
  
  // STEP 14: FUEL FILL END at 3:35 (500L after fill)
  setTimeout(() => {
    const fuelAfterFill = 500;
    const locTime = new Date(baseTime.getTime() + 215000); // 3:35
    const msg = {
      Plate: site,
      DriverName: '',
      LocTime: locTime.toISOString().replace('T', ' ').slice(0, -5),
      Temperature: '25,405',
      fuel_probe_1_level: (fuelAfterFill / 3.63).toFixed(1),
      fuel_probe_1_volume_in_tank: fuelAfterFill.toFixed(1),
      fuel_probe_1_temperature: '22.5',
      fuel_probe_1_level_percentage: Math.round((fuelAfterFill / 585) * 100).toString(),
      message_type: 405
    };
    console.log(`[${++messageCount}] 03:35 - ⛽ FUEL FILL END (${fuelAfterFill}L, +50L)`);
    ws.send(JSON.stringify(msg));
  }, 216000);
  
  // STEP 15: ENGINE OFF at 5:00
  setTimeout(() => {
    const locTime = new Date(baseTime.getTime() + 300000);
    const msg = {
      Plate: site,
      DriverName: 'PTO OFF',
      LocTime: locTime.toISOString().replace('T', ' ').slice(0, -5),
      Temperature: '',
      message_type: 405
    };
    console.log(`[${++messageCount}] 05:00 - PTO OFF`);
    ws.send(JSON.stringify(msg));
  }, 301000);
  
  // STEP 16: Closing fuel at 5:05 (495L - consumed 5L after fill)
  setTimeout(() => {
    const closingFuel = 495;
    const locTime = new Date(baseTime.getTime() + 305000);
    const msg = {
      Plate: site,
      DriverName: '',
      LocTime: locTime.toISOString().replace('T', ' ').slice(0, -5),
      Temperature: '25,405',
      fuel_probe_1_level: (closingFuel / 3.63).toFixed(1),
      fuel_probe_1_volume_in_tank: closingFuel.toFixed(1),
      fuel_probe_1_temperature: '23',
      fuel_probe_1_level_percentage: Math.round((closingFuel / 585) * 100).toString(),
      message_type: 405
    };
    console.log(`[${++messageCount}] 05:05 - Fuel: ${closingFuel}L (closing)\n`);
    ws.send(JSON.stringify(msg));
  }, 306000);
  
  // Summary
  setTimeout(() => {
    console.log('='.repeat(70));
    console.log('📊 TEST COMPLETE');
    console.log('='.repeat(70));
    console.log('Duration: 5 minutes of real-time data');
    console.log('Messages sent: ~16 (like real WebSocket traffic)');
    console.log('\n✅ Expected Results:');
    console.log('  ENGINE SESSION:');
    console.log('    - Opening: 500L');
    console.log('    - Closing: 495L');
    console.log('    - Consumed: 55L (with 50L fill in between)');
    console.log('    - Duration: 5 minutes');
    console.log('    - Status: COMPLETED');
    console.log('\n  FUEL FILL SESSION:');
    console.log('    - Opening: 450L');
    console.log('    - Closing: 500L');
    console.log('    - Fill amount: 50L');
    console.log('    - Duration: 30 seconds');
    console.log('    - Status: FUEL_FILL_COMPLETED');
    console.log('\n📋 Check database for 2 sessions!');
    console.log('='.repeat(70) + '\n');
  }, 307000);
});

console.log('⏳ Waiting for client connection...\n');
