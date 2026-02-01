const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8090 });

console.log('🚀 Local WebSocket Test Server started on port 8090');
console.log('📋 Testing session flow with intermittent fuel data (real format)...\n');

wss.on('connection', (ws) => {
  console.log('✅ Client connected\n');
  
  let messageCount = 0;
  let currentFuel = 500;
  let currentFuelLevel = 137.6;
  let currentPercentage = 85;
  
  // Simulate 5-minute session for NEW ROAD
  const simulateSession = () => {
    const site = 'NEW ROAD';
    const baseTime = new Date();
    
    // STEP 1: ENGINE ON without fuel data (0 seconds)
    setTimeout(() => {
      const msg1 = {
        Plate: site,
        Speed: 0,
        Latitude: -26.25708,
        Longitude: 28.26531,
        Quality: '61.13.2.85',
        Mileage: 5125,
        Pocsagstr: '61.13.2.85',
        Head: '',
        Geozone: 'KFC_NEW_ROAD',
        DriverName: 'ENGINE ON',
        NameEvent: '',
        Temperature: '', // NO FUEL DATA
        LocTime: new Date(baseTime.getTime()).toISOString().replace('T', ' ').slice(0, -5),
        message_type: 405
      };
      console.log(`📤 [${++messageCount}] ENGINE ON WITHOUT fuel data:`);
      console.log(JSON.stringify(msg1, null, 2));
      console.log('⏳ Expected: Session created with 0 fuel, marked as pending\n');
      ws.send(JSON.stringify(msg1));
    }, 1000);
    
    // STEP 2: Regular message WITH fuel data (5 seconds later)
    setTimeout(() => {
      const msg2 = {
        Plate: site,
        Speed: 0,
        Latitude: -26.25708,
        Longitude: 28.26531,
        Quality: '61.13.2.85',
        Mileage: 5125,
        Pocsagstr: '61.13.2.85',
        Head: '',
        Geozone: 'KFC_NEW_ROAD',
        DriverName: '',
        NameEvent: '',
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        LocTime: new Date(baseTime.getTime() + 5000).toISOString().replace('T', ' ').slice(0, -5),
        message_type: 405,
        fuel_probe_1_level: currentFuelLevel.toFixed(1),
        fuel_probe_1_volume_in_tank: currentFuel.toFixed(1),
        fuel_probe_1_temperature: '22',
        fuel_probe_1_level_percentage: currentPercentage.toString()
      };
      console.log(`📤 [${++messageCount}] FIRST fuel data (5s after ENGINE ON):`);
      console.log(`   Fuel: ${currentFuel}L (${currentPercentage}%)`);
      console.log('✅ Expected: Pending session updated with opening_fuel = ' + currentFuel + 'L\n');
      ws.send(JSON.stringify(msg2));
    }, 6000);
    
    // STEP 2b: Send OLDER fuel data (arrives later but has earlier LocTime)
    setTimeout(() => {
      const msg2b = {
        Plate: site,
        Speed: 0,
        Latitude: -26.25708,
        Longitude: 28.26531,
        Quality: '61.13.2.85',
        Mileage: 5125,
        Pocsagstr: '61.13.2.85',
        Head: '',
        Geozone: 'KFC_NEW_ROAD',
        DriverName: '',
        NameEvent: '',
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        LocTime: new Date(baseTime.getTime() + 2000).toISOString().replace('T', ' ').slice(0, -5), // 2s after ENGINE ON
        message_type: 405,
        fuel_probe_1_level: '138.5',
        fuel_probe_1_volume_in_tank: '505.0',
        fuel_probe_1_temperature: '22',
        fuel_probe_1_level_percentage: '86'
      };
      console.log(`📤 [${++messageCount}] OLDER fuel data (arrives now, but LocTime is 2s after ENGINE ON):`);
      console.log(`   Fuel: 505L - CLOSER to ENGINE ON time`);
      console.log('🎯 Expected: System should UPDATE to use 505L (closer LocTime)\n');
      ws.send(JSON.stringify(msg2b));
    }, 8000);
    
    // STEP 3-12: Regular updates - alternating with/without fuel data
    for (let i = 1; i <= 10; i++) {
      setTimeout(() => {
        currentFuel -= 5;
        currentFuelLevel -= 1.37;
        currentPercentage = Math.round((currentFuel / 585) * 100);
        
        // Alternate: some messages have fuel data, some don't (realistic scenario)
        const hasFuelData = i % 2 === 0; // Every other message has fuel
        
        const msg = {
          Plate: site,
          Speed: 0,
          Latitude: -26.25708,
          Longitude: 28.26531,
          Quality: '61.13.2.85',
          Mileage: 5125 + i,
          Pocsagstr: '61.13.2.85',
          Head: '',
          Geozone: 'KFC_NEW_ROAD',
          DriverName: '',
          NameEvent: '',
          Temperature: hasFuelData ? '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37' : '',
          LocTime: new Date(baseTime.getTime() + (5000 + i * 30000)).toISOString().replace('T', ' ').slice(0, -5),
          message_type: 405
        };
        
        if (hasFuelData) {
          msg.fuel_probe_1_level = currentFuelLevel.toFixed(1);
          msg.fuel_probe_1_volume_in_tank = currentFuel.toFixed(1);
          msg.fuel_probe_1_temperature = (22 + i * 0.1).toFixed(1);
          msg.fuel_probe_1_level_percentage = currentPercentage.toString();
        }
        
        console.log(`📤 [${++messageCount}] Update ${i}/10 (${i * 30}s) - ${hasFuelData ? 'WITH' : 'WITHOUT'} fuel - ${hasFuelData ? currentFuel.toFixed(1) + 'L' : 'no data'}`);
        ws.send(JSON.stringify(msg));
      }, 6000 + (i * 30000));
    }
    
    // STEP 13: ENGINE OFF without fuel data (5 minutes later)
    setTimeout(() => {
      const msg13 = {
        Plate: site,
        Speed: 0,
        Latitude: -26.25708,
        Longitude: 28.26531,
        Quality: '61.13.2.85',
        Mileage: 5135,
        Pocsagstr: '61.13.2.85',
        Head: '',
        Geozone: 'KFC_NEW_ROAD',
        DriverName: 'ENGINE OFF',
        NameEvent: '',
        Temperature: '', // NO FUEL DATA
        LocTime: new Date(baseTime.getTime() + 305000).toISOString().replace('T', ' ').slice(0, -5),
        message_type: 405
      };
      console.log(`📤 [${++messageCount}] ENGINE OFF WITHOUT fuel data:`);
      console.log(JSON.stringify(msg13, null, 2));
      console.log('⏳ Expected: Session marked for closure, waiting for fuel data\n');
      ws.send(JSON.stringify(msg13));
    }, 306000);
    
    // STEP 14: Regular message WITH fuel data (5 seconds after ENGINE OFF)
    setTimeout(() => {
      const msg14 = {
        Plate: site,
        Speed: 0,
        Latitude: -26.25708,
        Longitude: 28.26531,
        Quality: '61.13.2.85',
        Mileage: 5135,
        Pocsagstr: '61.13.2.85',
        Head: '',
        Geozone: 'KFC_NEW_ROAD',
        DriverName: '',
        NameEvent: '',
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        LocTime: new Date(baseTime.getTime() + 310000).toISOString().replace('T', ' ').slice(0, -5),
        message_type: 405,
        fuel_probe_1_level: (currentFuelLevel - 2).toFixed(1),
        fuel_probe_1_volume_in_tank: (currentFuel - 10).toFixed(1),
        fuel_probe_1_temperature: '23',
        fuel_probe_1_level_percentage: (currentPercentage - 2).toString()
      };
      console.log(`📤 [${++messageCount}] FIRST closing fuel data (5s after ENGINE OFF):`);
      console.log(`   Fuel: ${(currentFuel - 10).toFixed(1)}L`);
      ws.send(JSON.stringify(msg14));
    }, 311000);
    
    // STEP 14b: Send OLDER fuel data (arrives later but has earlier LocTime - closer to ENGINE OFF)
    setTimeout(() => {
      const msg14b = {
        Plate: site,
        Speed: 0,
        Latitude: -26.25708,
        Longitude: 28.26531,
        Quality: '61.13.2.85',
        Mileage: 5135,
        Pocsagstr: '61.13.2.85',
        Head: '',
        Geozone: 'KFC_NEW_ROAD',
        DriverName: '',
        NameEvent: '',
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        LocTime: new Date(baseTime.getTime() + 306000).toISOString().replace('T', ' ').slice(0, -5), // 1s after ENGINE OFF
        message_type: 405,
        fuel_probe_1_level: currentFuelLevel.toFixed(1),
        fuel_probe_1_volume_in_tank: currentFuel.toFixed(1),
        fuel_probe_1_temperature: '23',
        fuel_probe_1_level_percentage: currentPercentage.toString()
      };
      console.log(`📤 [${++messageCount}] OLDER closing fuel data (arrives now, but LocTime is 1s after ENGINE OFF):`);
      console.log(`   Fuel: ${currentFuel.toFixed(1)}L - CLOSER to ENGINE OFF time`);
      console.log('🎯 Expected: System should use ' + currentFuel.toFixed(1) + 'L (closer LocTime)\n');
      ws.send(JSON.stringify(msg14b));
    }, 313000);
    
    // Summary after completion
    setTimeout(() => {
      console.log('\n' + '='.repeat(60));
      console.log('📋 TEST SUMMARY');
      console.log('='.repeat(60));
      console.log('Site: NEW ROAD');
      console.log('Duration: 5 minutes');
      console.log('\n🎯 LocTime-Based Matching Test:');
      console.log('   Opening: Should use 505L (LocTime 2s after ENGINE ON)');
      console.log('   NOT 500L (LocTime 5s after ENGINE ON)');
      console.log('   Closing: Should use ' + currentFuel.toFixed(1) + 'L (LocTime 1s after ENGINE OFF)');
      console.log('   NOT ' + (currentFuel - 10).toFixed(1) + 'L (LocTime 5s after ENGINE OFF)');
      console.log('\n✅ Check your database for session with:');
      console.log('   - opening_fuel = 505 (closest to ENGINE ON LocTime)');
      console.log('   - closing_fuel = ' + currentFuel.toFixed(1) + ' (closest to ENGINE OFF LocTime)');
      console.log('   - total_usage = ' + (505 - currentFuel).toFixed(1));
      console.log('\n📊 This proves the system uses LocTime proximity, not arrival order!');
      console.log('='.repeat(60) + '\n');
    }, 314000);
  };
  
  // Start simulation after 2 seconds
  setTimeout(() => {
    console.log('🎬 Starting 5-minute session simulation...\n');
    simulateSession();
  }, 2000);
});

wss.on('error', (error) => {
  console.error('❌ WebSocket error:', error);
});

console.log('⏳ Waiting for client connection...');
console.log('💡 Make sure your server is running with WEBSOCKET_URL=ws://localhost:8090\n');
