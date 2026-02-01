require('dotenv').config();
const WebSocket = require('ws');
const { supabase } = require('./supabase-client');

// Test timing accuracy with controlled intervals
class TimingAccuracyTest {
  constructor() {
    this.wss = new WebSocket.Server({ port: 8006 });
    this.testResults = [];
    this.testStartTime = null;
  }

  start() {
    console.log('🧪 Timing Accuracy Test Server started on ws://localhost:8006');
    console.log('📋 This test sends ENGINE ON/OFF with precise timing intervals');
    console.log('⏱️  Expected durations: 5s, 10s, 15s, 30s');
    console.log('\n🔌 Connect your server to ws://localhost:8006 to test\n');

    this.wss.on('connection', (ws) => {
      console.log('✅ Test client connected - starting timing tests...\n');
      this.runTimingTests(ws);
    });
  }

  async runTimingTests(ws) {
    const tests = [
      { duration: 5000, expectedHours: 0.001389 },   // 5 seconds = 0.001389 hours
      { duration: 10000, expectedHours: 0.002778 },  // 10 seconds = 0.002778 hours
      { duration: 15000, expectedHours: 0.004167 },  // 15 seconds = 0.004167 hours
      { duration: 30000, expectedHours: 0.008333 }   // 30 seconds = 0.008333 hours
    ];

    for (let i = 0; i < tests.length; i++) {
      const test = tests[i];
      console.log(`🔄 Test ${i + 1}: ${test.duration}ms duration (${test.expectedHours.toFixed(6)}h expected)`);
      
      // Send ENGINE ON
      const startTime = Date.now();
      this.sendMessage(ws, 'ENGINE ON', startTime);
      
      // Wait exact duration
      await this.sleep(test.duration);
      
      // Send ENGINE OFF
      const endTime = Date.now();
      this.sendMessage(ws, 'ENGINE OFF', endTime);
      
      const actualDuration = endTime - startTime;
      const actualHours = actualDuration / (1000 * 60 * 60);
      
      console.log(`   ⏱️  Actual duration: ${actualDuration}ms (${actualHours.toFixed(6)}h)`);
      console.log(`   📊 Expected: ${test.expectedHours.toFixed(6)}h, Actual: ${actualHours.toFixed(6)}h`);
      console.log(`   ✅ Difference: ${Math.abs(actualHours - test.expectedHours).toFixed(6)}h\n`);
      
      // Wait between tests and check database
      await this.sleep(3000);
      await this.checkDatabaseSession(i + 1, test.expectedHours);
    }
    
    console.log('🏁 All timing tests completed!');
    console.log('📋 Check your database for session durations matching the expected values above.');
  }

  sendMessage(ws, status, timestamp) {
    const message = {
      Plate: 'TIMING-TEST',
      Speed: 0,
      Latitude: -26.1,
      Longitude: 27.8,
      LocTime: timestamp.toString(),
      Quality: '192.168.1.200',
      Mileage: null,
      Pocsagstr: '',
      Head: '',
      Geozone: 'Test Location',
      DriverName: status,
      NameEvent: '',
      Temperature: '25,405,1004,2020,0741,2021,05A5,2022,14,2023,54',
      fuel_probe_1_level: 200.0,
      fuel_probe_1_volume_in_tank: 400.0,
      fuel_probe_1_temperature: 25.0,
      fuel_probe_1_level_percentage: 60,
      message_type: 405
    };

    ws.send(JSON.stringify(message));
    console.log(`📤 Sent: ${status} at ${new Date(timestamp).toISOString()}`);
  }

  async checkDatabaseSession(testNumber, expectedHours) {
    try {
      const { data: sessions } = await supabase
        .from('energy_rite_operating_sessions')
        .select('*')
        .eq('branch', 'TIMING-TEST')
        .eq('session_status', 'COMPLETED')
        .order('session_start_time', { ascending: false })
        .limit(1);
        
      if (sessions && sessions.length > 0) {
        const session = sessions[0];
        const dbHours = session.operating_hours;
        const difference = Math.abs(dbHours - expectedHours);
        const accuracy = ((1 - (difference / expectedHours)) * 100).toFixed(2);
        
        console.log(`   🗄️  Database Result for Test ${testNumber}:`);
        console.log(`   📊 Expected: ${expectedHours.toFixed(6)}h`);
        console.log(`   💾 Database: ${dbHours.toFixed(6)}h`);
        console.log(`   📈 Accuracy: ${accuracy}% (${difference.toFixed(6)}h difference)`);
        console.log(`   ${accuracy > 99.9 ? '✅' : '❌'} ${accuracy > 99.9 ? 'PASS' : 'FAIL'}\n`);
      } else {
        console.log(`   ❌ No session found in database for Test ${testNumber}\n`);
      }
    } catch (error) {
      console.log(`   ❌ Database check failed: ${error.message}\n`);
    }
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Start the test
const test = new TimingAccuracyTest();
test.start();