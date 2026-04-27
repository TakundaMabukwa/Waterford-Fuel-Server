const WebSocket = require('ws');

const port = Number(process.env.DUMMY_WS_PORT || 8099);
const plate = process.env.DUMMY_TEST_PLATE || 'ZZLATESTART01';
const host = '0.0.0.0';
const wss = new WebSocket.Server({ port, host });
const baseTime = new Date(process.env.DUMMY_BASE_TIME || '2026-04-27T10:00:00Z');

function loc(offsetSeconds) {
  const d = new Date(baseTime.getTime() + offsetSeconds * 1000);
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

function fuelPayload({ offsetSeconds, fuel, speed = 0, driverName = '' }) {
  const pct = Math.round((fuel / 600) * 100);
  return {
    Plate: plate,
    Speed: speed,
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
    message_type: 405,
    fuel_probe_1_level: String(fuel.toFixed(1)),
    fuel_probe_1_volume_in_tank: String(fuel.toFixed(1)),
    fuel_probe_1_temperature: '24',
    fuel_probe_1_level_percentage: String(pct),
    fuel_probe_2_level: '0.0',
    fuel_probe_2_volume_in_tank: '0.0',
    fuel_probe_2_temperature: '25',
    fuel_probe_2_level_percentage: '0'
  };
}

function statusPayload({ offsetSeconds, driverName, speed = 0 }) {
  return {
    Plate: plate,
    Speed: speed,
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

const sequence = [
  fuelPayload({ offsetSeconds: 0, fuel: 500.0, speed: 15 }),
  fuelPayload({ offsetSeconds: 15, fuel: 505.0, speed: 15 }),
  fuelPayload({ offsetSeconds: 30, fuel: 510.0, speed: 15 }),
  statusPayload({ offsetSeconds: 45, driverName: 'Possible Fuel Fill', speed: 0 }),
  fuelPayload({ offsetSeconds: 60, fuel: 520.0, speed: 0 }),
  fuelPayload({ offsetSeconds: 80, fuel: 535.0, speed: 0 }),
  fuelPayload({ offsetSeconds: 100, fuel: 540.0, speed: 0 }),
  fuelPayload({ offsetSeconds: 140, fuel: 540.0, speed: 0 }),
  fuelPayload({ offsetSeconds: 220, fuel: 540.0, speed: 0 })
];

console.log(`Late-start fill dummy websocket listening on ws://localhost:${port}`);
console.log(`Plate under test: ${plate}`);
console.log('Scenario: fuel rises before fill status; expected opening should be LOWEST pre-status (500L), not closest pre-status (510L).');

wss.on('connection', (ws) => {
  console.log('Client connected to late-start dummy websocket');
  let index = 0;

  const sendNext = () => {
    if (index >= sequence.length) {
      console.log('Scenario payloads sent; waiting for server-side stabilization completion');
      return;
    }

    const payload = sequence[index++];
    ws.send(JSON.stringify(payload));
    console.log(`[${index}/${sequence.length}] ${payload.LocTime} ${payload.DriverName || 'FUEL'} speed=${payload.Speed} fuel=${payload.fuel_probe_1_volume_in_tank ?? 'n/a'}`);
    setTimeout(sendNext, 1000);
  };

  sendNext();
});

process.on('SIGINT', () => {
  console.log('Shutting down late-start dummy websocket');
  wss.close(() => process.exit(0));
});
