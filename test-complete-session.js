const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8090 });

console.log('🚀 Complete Session Test - Port 8090');
console.log('📋 Scenario: Engine session with fuel fill in middle, all jumbled\n');

wss.on('connection', (ws) => {
  console.log('✅ Client connected\n');
  
  const site = 'NEW ROAD';
  const baseTime = new Date();
  
  // Create chronological scenario (compressed to 5 minutes)
  const messages = [
    // 1. ENGINE ON at 09:00:00
    { order: 1, time: 0, data: { Plate: site, DriverName: 'PTO ON', LocTime: new Date(baseTime.getTime()).toISOString().replace('T', ' ').slice(0, -5), Temperature: '', message_type: 405 }},
    
    // 2. Fuel data at 09:00:05 (opening fuel: 400L)
    { order: 2, time: 5000, data: { Plate: site, DriverName: '', LocTime: new Date(baseTime.getTime() + 5000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '25,405', fuel_probe_1_level: '110.0', fuel_probe_1_volume_in_tank: '400.0', fuel_probe_1_temperature: '22', fuel_probe_1_level_percentage: '68', message_type: 405 }},
    
    // 3. Fuel data at 09:01:00 (consuming: 390L)
    { order: 3, time: 60000, data: { Plate: site, DriverName: '', LocTime: new Date(baseTime.getTime() + 60000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '25,405', fuel_probe_1_level: '107.3', fuel_probe_1_volume_in_tank: '390.0', fuel_probe_1_temperature: '22', fuel_probe_1_level_percentage: '66', message_type: 405 }},
    
    // 4. FUEL FILL START at 09:02:00
    { order: 4, time: 120000, data: { Plate: site, DriverName: 'POSSIBLE FUEL FILL', LocTime: new Date(baseTime.getTime() + 120000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '25,405', fuel_probe_1_level: '104.6', fuel_probe_1_volume_in_tank: '380.0', fuel_probe_1_temperature: '22', fuel_probe_1_level_percentage: '64', message_type: 405 }},
    
    // 5. FUEL FILL END at 09:02:30 (filled to 480L)
    { order: 5, time: 150000, data: { Plate: site, DriverName: '', LocTime: new Date(baseTime.getTime() + 150000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '25,405', fuel_probe_1_level: '132.0', fuel_probe_1_volume_in_tank: '480.0', fuel_probe_1_temperature: '22', fuel_probe_1_level_percentage: '82', message_type: 405 }},
    
    // 6. Fuel data at 09:03:00 (consuming again: 470L)
    { order: 6, time: 180000, data: { Plate: site, DriverName: '', LocTime: new Date(baseTime.getTime() + 180000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '25,405', fuel_probe_1_level: '129.3', fuel_probe_1_volume_in_tank: '470.0', fuel_probe_1_temperature: '22', fuel_probe_1_level_percentage: '80', message_type: 405 }},
    
    // 7. ENGINE OFF at 09:04:00
    { order: 7, time: 240000, data: { Plate: site, DriverName: 'PTO OFF', LocTime: new Date(baseTime.getTime() + 240000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '', message_type: 405 }},
    
    // 8. Fuel data at 09:04:05 (closing fuel: 460L)
    { order: 8, time: 245000, data: { Plate: site, DriverName: '', LocTime: new Date(baseTime.getTime() + 245000).toISOString().replace('T', ' ').slice(0, -5), Temperature: '25,405', fuel_probe_1_level: '126.6', fuel_probe_1_volume_in_tank: '460.0', fuel_probe_1_temperature: '22', fuel_probe_1_level_percentage: '78', message_type: 405 }}
  ];
  
  // Send in JUMBLED order
  const jumbledOrder = [7, 3, 5, 1, 8, 4, 2, 6]; // OFF, fuel, fuel, ON, fuel, FILL, fuel, fuel
  
  console.log('📤 Sending messages in JUMBLED order:');
  console.log('   Send order: 7, 3, 5, 1, 8, 4, 2, 6');
  console.log('   Expected: System sorts to 1→2→3→4→5→6→7→8\n');
  
  setTimeout(() => {
    jumbledOrder.forEach((msgIndex, i) => {
      const msg = messages[msgIndex - 1];
      setTimeout(() => {
        console.log(`📨 Sending [${i + 1}/8]: Order ${msg.order} - ${msg.data.DriverName || 'fuel'}`);
        ws.send(JSON.stringify(msg.data));
      }, i * 100);
    });
    
    setTimeout(() => {
      console.log('\n' + '='.repeat(70));
      console.log('📊 TEST SUMMARY');
      console.log('='.repeat(70));
      console.log('Chronological order (what system should process):');
      console.log('  1. 09:00:00 - PTO ON');
      console.log('  2. 09:00:05 - Fuel 400L (opening)');
      console.log('  3. 09:01:00 - Fuel 390L (consuming)');
      console.log('  4. 09:02:00 - FUEL FILL START (380L)');
      console.log('  5. 09:02:30 - Fuel 480L (filled +100L)');
      console.log('  6. 09:03:00 - Fuel 470L (consuming)');
      console.log('  7. 09:04:00 - PTO OFF');
      console.log('  8. 09:04:05 - Fuel 460L (closing)');
      console.log('\n✅ Expected Results:');
      console.log('  ENGINE SESSION:');
      console.log('    - Opening: 400L');
      console.log('    - Closing: 460L');
      console.log('    - Net consumption: 40L (400-460+100fill)');
      console.log('    - Duration: 4 minutes');
      console.log('    - Status: COMPLETED');
      console.log('\n  FUEL FILL SESSION:');
      console.log('    - Opening: 380L');
      console.log('    - Closing: 480L');
      console.log('    - Fill amount: 100L');
      console.log('    - Duration: 30 seconds');
      console.log('    - Status: FUEL_FILL_COMPLETED');
      console.log('\n📋 Check database for 2 sessions with correct values!');
      console.log('='.repeat(70) + '\n');
    }, 1500);
  }, 2000);
});

console.log('⏳ Waiting for client connection...\n');
