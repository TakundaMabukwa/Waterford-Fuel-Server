const WebSocket = require('ws');

const port = Number(process.env.DUMMY_WS_PORT || 8099);
const plate = process.env.DUMMY_TEST_PLATE || 'ZZDUALTANK01';
const host = '0.0.0.0';
const wss = new WebSocket.Server({ port, host });

const baseTime = new Date('2026-04-13T10:00:00Z');
const messages = [];

function loc(offsetSeconds) {
  const d = new Date(baseTime.getTime() + offsetSeconds * 1000);
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

function buildFuelPayload(tank1Volume, tank1Pct, tank2Volume, tank2Pct) {
  return {
    fuel_probe_1_level: String(tank1Volume.toFixed(1)),
    fuel_probe_1_volume_in_tank: String(tank1Volume.toFixed(1)),
    fuel_probe_1_temperature: '24',
    fuel_probe_1_level_percentage: String(Math.round(tank1Pct)),
    fuel_probe_2_level: String(tank2Volume.toFixed(1)),
    fuel_probe_2_volume_in_tank: String(tank2Volume.toFixed(1)),
    fuel_probe_2_temperature: '25',
    fuel_probe_2_level_percentage: String(Math.round(tank2Pct))
  };
}

function message(offsetSeconds, options = {}) {
  const payload = {
    Plate: plate,
    Speed: options.speed ?? 0,
    Latitude: options.lat ?? -26.2041,
    Longitude: options.lng ?? 28.0473,
    Quality: '54.125.1.247',
    Mileage: options.mileage ?? 500000,
    Pocsagstr: '54.125.1.247',
    Head: '',
    Geozone: options.geozone ?? '',
    DriverName: options.driverName ?? '',
    NameEvent: '',
    Temperature: options.temperature ?? '',
    LocTime: loc(offsetSeconds),
    message_type: 405
  };

  if (options.includeFuel !== false) {
    Object.assign(
      payload,
      buildFuelPayload(
        options.tank1Volume ?? 0,
        options.tank1Pct ?? 0,
        options.tank2Volume ?? 0,
        options.tank2Pct ?? 0
      )
    );
  }

  return payload;
}

function add(payload, delayMs) {
  messages.push({ payload, delayMs });
}

// Scenario 1: engine ON status without fuel. Opening must come from the closest earlier fuel packet.
add(message(0, { tank1Volume: 300.0, tank1Pct: 60, tank2Volume: 200.0, tank2Pct: 50 }), 1000);
add(message(8, { tank1Volume: 299.4, tank1Pct: 60, tank2Volume: 199.8, tank2Pct: 50, speed: 2 }), 1000);
add(message(11, { includeFuel: false, driverName: 'ENGINE ON' }), 1000);

// Scenario 2: engine ON movement and slosh. Should not affect event capture.
add(message(25, { tank1Volume: 297.1, tank1Pct: 59, tank2Volume: 198.9, tank2Pct: 50, speed: 40 }), 1000);
add(message(45, { tank1Volume: 296.4, tank1Pct: 59, tank2Volume: 198.4, tank2Pct: 50, speed: 38 }), 1000);

// Scenario 3: engine OFF status without fuel. Closing must come from the closest later packet.
add(message(60, { includeFuel: false, driverName: 'ENGINE OFF' }), 1000);
add(message(72, { tank1Volume: 295.8, tank1Pct: 59, tank2Volume: 197.9, tank2Pct: 49, speed: 0 }), 1000);

// Scenario 4: off-engine fill on both tanks, then stabilize.
add(message(95, { tank1Volume: 296.0, tank1Pct: 59, tank2Volume: 198.0, tank2Pct: 49, geozone: 'DEPOT' }), 1000);
add(message(120, { tank1Volume: 308.0, tank1Pct: 62, tank2Volume: 212.0, tank2Pct: 53, geozone: 'DEPOT' }), 1000);
add(message(145, { tank1Volume: 314.2, tank1Pct: 63, tank2Volume: 220.5, tank2Pct: 55, geozone: 'DEPOT' }), 1000);
add(message(170, { tank1Volume: 314.0, tank1Pct: 63, tank2Volume: 220.3, tank2Pct: 55, geozone: 'DEPOT' }), 1000);

// Scenario 5: engine ON again with no fuel in status packet. Opening should use closest earlier stabilized reading.
add(message(190, { includeFuel: false, driverName: 'ENGINE ON' }), 1000);
add(message(215, { tank1Volume: 311.0, tank1Pct: 62, tank2Volume: 218.0, tank2Pct: 55, speed: 32 }), 1000);

// Scenario 6: engine OFF again with no fuel, later closing snapshot.
add(message(245, { includeFuel: false, driverName: 'ENGINE OFF' }), 1000);
add(message(258, { tank1Volume: 309.8, tank1Pct: 62, tank2Volume: 216.7, tank2Pct: 54, speed: 0 }), 1000);

// Scenario 7: off-engine theft on both tanks, then stabilize.
add(message(285, { tank1Volume: 309.5, tank1Pct: 62, tank2Volume: 216.4, tank2Pct: 54, geozone: 'YARD' }), 1000);
add(message(310, { tank1Volume: 270.0, tank1Pct: 54, tank2Volume: 185.0, tank2Pct: 46, geozone: 'YARD' }), 1000);
add(message(335, { tank1Volume: 255.0, tank1Pct: 51, tank2Volume: 175.0, tank2Pct: 44, geozone: 'YARD' }), 1000);
add(message(360, { tank1Volume: 255.2, tank1Pct: 51, tank2Volume: 175.1, tank2Pct: 44, geozone: 'YARD' }), 1000);

// Quiet tail to keep socket alive while watcher stabilization elapses.
add(message(450, { tank1Volume: 255.1, tank1Pct: 51, tank2Volume: 175.0, tank2Pct: 44, geozone: 'YARD' }), 1000);
add(message(540, { tank1Volume: 255.0, tank1Pct: 51, tank2Volume: 175.0, tank2Pct: 44, geozone: 'YARD' }), 1000);

console.log(`Dummy dual-tank Waterford websocket listening on ws://localhost:${port}`);
console.log(`Plate under test: ${plate}`);
console.log(`Messages queued: ${messages.length}`);

wss.on('connection', (ws) => {
  console.log('Client connected to dummy websocket');
  let index = 0;

  const sendNext = () => {
    if (index >= messages.length) {
      console.log('Scenario complete, keeping socket open for stabilization checks');
      return;
    }

    const { payload, delayMs } = messages[index++];
    ws.send(JSON.stringify(payload));
    console.log(
      `[${index}/${messages.length}] ${payload.LocTime} ${payload.DriverName || 'FUEL'} ` +
      `tank1=${payload.fuel_probe_1_volume_in_tank ?? 'n/a'} tank2=${payload.fuel_probe_2_volume_in_tank ?? 'n/a'} speed=${payload.Speed}`
    );
    setTimeout(sendNext, delayMs);
  };

  sendNext();
});

process.on('SIGINT', () => {
  console.log('Shutting down dummy websocket');
  wss.close(() => process.exit(0));
});
