const WebSocket = require('ws');

const port = Number(process.env.DUMMY_WS_PORT || 8099);
const plate = process.env.DUMMY_TEST_PLATE || 'ZZPREFILL01';
const host = '0.0.0.0';
const wss = new WebSocket.Server({ port, host });

const baseTime = new Date(process.env.DUMMY_BASE_TIME || '2026-04-13T12:00:00Z');
const messages = [];

function loc(offsetSeconds) {
  const d = new Date(baseTime.getTime() + offsetSeconds * 1000);
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

function fuelPayload(tank1Volume, tank1Pct, tank2Volume, tank2Pct, options = {}) {
  return {
    Plate: plate,
    Speed: options.speed ?? 0,
    Latitude: -26.2041,
    Longitude: 28.0473,
    Quality: '54.125.1.247',
    Mileage: 500000,
    Pocsagstr: '54.125.1.247',
    Head: '',
    Geozone: options.geozone ?? 'DEPOT',
    DriverName: options.driverName ?? '',
    NameEvent: '',
    Temperature: '',
    LocTime: loc(options.offsetSeconds ?? 0),
    message_type: 405,
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

function statusPayload(driverName, offsetSeconds) {
  return {
    Plate: plate,
    Speed: 0,
    Latitude: -26.2041,
    Longitude: 28.0473,
    Quality: '54.125.1.247',
    Mileage: 500000,
    Pocsagstr: '54.125.1.247',
    Head: '',
    Geozone: 'DEPOT',
    DriverName: driverName,
    NameEvent: '',
    Temperature: '',
    LocTime: loc(offsetSeconds),
    message_type: 405
  };
}

function add(delayMs, payload) {
  messages.push({ delayMs, payload });
}

// Off-engine pre-fill baseline drifting down slightly. The lowest here should be used as opening.
add(1000, fuelPayload(290.4, 58, 205.2, 51, { offsetSeconds: 0 }));
add(1000, fuelPayload(289.8, 58, 204.9, 51, { offsetSeconds: 20 }));
add(1000, fuelPayload(289.1, 58, 204.2, 51, { offsetSeconds: 40 }));
add(1000, fuelPayload(288.6, 57, 203.8, 51, { offsetSeconds: 60 }));
add(1000, statusPayload('Possible Fuel Fill', 75));

// Fill begins after the status and rises on both tanks.
add(1000, fuelPayload(296.0, 59, 210.3, 53, { offsetSeconds: 95 }));
add(1000, fuelPayload(304.4, 61, 219.6, 55, { offsetSeconds: 120 }));
add(1000, fuelPayload(311.2, 62, 227.8, 57, { offsetSeconds: 145 }));
add(1000, fuelPayload(311.0, 62, 227.6, 57, { offsetSeconds: 170 }));

// Keep socket alive long enough for stabilization checks and completion.
add(1000, fuelPayload(311.1, 62, 227.7, 57, { offsetSeconds: 240 }));
add(1000, fuelPayload(311.0, 62, 227.6, 57, { offsetSeconds: 300 }));

console.log(`Dummy pre-fill status websocket listening on ws://localhost:${port}`);
console.log(`Plate under test: ${plate}`);

wss.on('connection', (ws) => {
  console.log('Client connected to dummy websocket');
  let index = 0;

  const sendNext = () => {
    if (index >= messages.length) {
      console.log('Scenario complete, socket left open for stabilization');
      return;
    }

    const { delayMs, payload } = messages[index++];
    ws.send(JSON.stringify(payload));
    console.log(
      `[${index}/${messages.length}] ${payload.LocTime} ${payload.DriverName || 'FUEL'} ` +
      `tank1=${payload.fuel_probe_1_volume_in_tank ?? 'n/a'} tank2=${payload.fuel_probe_2_volume_in_tank ?? 'n/a'}`
    );
    setTimeout(sendNext, delayMs);
  };

  sendNext();
});

process.on('SIGINT', () => {
  console.log('Shutting down dummy websocket');
  wss.close(() => process.exit(0));
});
