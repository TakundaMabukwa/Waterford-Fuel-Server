const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8090 });

console.log('🚀 Jumbled Message Test Server - Port 8090\n');

wss.on('connection', (ws) => {
  console.log('✅ Client connected\n');
  
  const site = 'NEW ROAD';
  const baseTime = new Date();
  
  // Create all messages with correct LocTimes
  const messages = [
    // Message 1: PTO ON at 09:26:31
    {
      order: 1,
      locTime: new Date(baseTime.getTime()).toISOString().replace('T', ' ').slice(0, -5),
      data: {
        Plate: site,
        DriverName: 'PTO ON',
        LocTime: new Date(baseTime.getTime()).toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '',
        message_type: 405
      }
    },
    // Message 2: Fuel data at 09:26:36 (5s after)
    {
      order: 2,
      locTime: new Date(baseTime.getTime() + 5000).toISOString().replace('T', ' ').slice(0, -5),
      data: {
        Plate: site,
        DriverName: '',
        LocTime: new Date(baseTime.getTime() + 5000).toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        fuel_probe_1_level: '137.6',
        fuel_probe_1_volume_in_tank: '500.0',
        fuel_probe_1_temperature: '22',
        fuel_probe_1_level_percentage: '85',
        message_type: 405
      }
    },
    // Message 3: Fuel data at 09:27:06
    {
      order: 3,
      locTime: new Date(baseTime.getTime() + 35000).toISOString().replace('T', ' ').slice(0, -5),
      data: {
        Plate: site,
        DriverName: '',
        LocTime: new Date(baseTime.getTime() + 35000).toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        fuel_probe_1_level: '134.9',
        fuel_probe_1_volume_in_tank: '490.0',
        fuel_probe_1_temperature: '22.2',
        fuel_probe_1_level_percentage: '84',
        message_type: 405
      }
    },
    // Message 4: Fuel data at 09:27:36
    {
      order: 4,
      locTime: new Date(baseTime.getTime() + 65000).toISOString().replace('T', ' ').slice(0, -5),
      data: {
        Plate: site,
        DriverName: '',
        LocTime: new Date(baseTime.getTime() + 65000).toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        fuel_probe_1_level: '132.1',
        fuel_probe_1_volume_in_tank: '480.0',
        fuel_probe_1_temperature: '22.4',
        fuel_probe_1_level_percentage: '82',
        message_type: 405
      }
    },
    // Message 5: PTO OFF at 09:28:06
    {
      order: 5,
      locTime: new Date(baseTime.getTime() + 95000).toISOString().replace('T', ' ').slice(0, -5),
      data: {
        Plate: site,
        DriverName: 'PTO OFF',
        LocTime: new Date(baseTime.getTime() + 95000).toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '',
        message_type: 405
      }
    },
    // Message 6: Fuel data at 09:28:11 (5s after OFF)
    {
      order: 6,
      locTime: new Date(baseTime.getTime() + 100000).toISOString().replace('T', ' ').slice(0, -5),
      data: {
        Plate: site,
        DriverName: '',
        LocTime: new Date(baseTime.getTime() + 100000).toISOString().replace('T', ' ').slice(0, -5),
        Temperature: '25,405,1004,2020,0560,2021,0D2D,2022,14,2023,37',
        fuel_probe_1_level: '129.4',
        fuel_probe_1_volume_in_tank: '470.0',
        fuel_probe_1_temperature: '22.6',
        fuel_probe_1_level_percentage: '80',
        message_type: 405
      }
    }
  ];
  
  // Send messages in JUMBLED order (simulating reconnection)
  const jumbledOrder = [5, 2, 6, 1, 3, 4]; // OFF, fuel, fuel, ON, fuel, fuel
  
  console.log('📤 Sending messages in JUMBLED order:');
  console.log('   Order: OFF -> fuel -> fuel -> ON -> fuel -> fuel');
  console.log('   Expected: System sorts by LocTime and processes correctly\n');
  
  setTimeout(() => {
    jumbledOrder.forEach((msgIndex, i) => {
      const msg = messages[msgIndex - 1];
      setTimeout(() => {
        console.log(`📨 Sending message ${i + 1}/6: [Order ${msg.order}] ${msg.locTime} - ${msg.data.DriverName || 'fuel data'}`);
        ws.send(JSON.stringify(msg.data));
      }, i * 100); // Send all within 600ms (bulk)
    });
    
    // Summary
    setTimeout(() => {
      console.log('\n' + '='.repeat(60));
      console.log('📊 TEST SUMMARY');
      console.log('='.repeat(60));
      console.log('Messages sent in order: 5, 2, 6, 1, 3, 4');
      console.log('System should sort to: 1, 2, 3, 4, 5, 6');
      console.log('\n✅ Expected processing order:');
      console.log('   1. PTO ON (09:26:31)');
      console.log('   2. Fuel 500L (09:26:36) - Updates opening fuel');
      console.log('   3. Fuel 490L (09:27:06)');
      console.log('   4. Fuel 480L (09:27:36)');
      console.log('   5. PTO OFF (09:28:06)');
      console.log('   6. Fuel 470L (09:28:11) - Completes session');
      console.log('\n📋 Check database:');
      console.log('   - opening_fuel: 500L');
      console.log('   - closing_fuel: 470L');
      console.log('   - total_usage: 30L');
      console.log('   - duration: ~1.6 minutes');
      console.log('='.repeat(60) + '\n');
    }, 1000);
  }, 2000);
});

console.log('⏳ Waiting for client connection...\n');
