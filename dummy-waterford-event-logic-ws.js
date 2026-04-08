const WebSocket = require('ws');

const port = Number(process.env.DUMMY_WS_PORT || 8099);
const plate = process.env.DUMMY_TEST_PLATE || 'ZZEVENTTEST1';
const host = '0.0.0.0';
const wss = new WebSocket.Server({ port, host });

const baseTime = new Date('2026-04-08T06:00:00Z');
const messages = [];

function loc(offsetSeconds) {
  const d = new Date(baseTime.getTime() + offsetSeconds * 1000);
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

function fuelMessage(offsetSeconds, volume, percentage, options = {}) {
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
    payload.fuel_probe_1_level = String(volume.toFixed(1));
    payload.fuel_probe_1_volume_in_tank = String(volume.toFixed(1));
    payload.fuel_probe_1_temperature = '24';
    payload.fuel_probe_1_level_percentage = String(Math.max(0, percentage).toFixed(0));
  }

  return payload;
}

function add(payload, delayMs = 250) {
  messages.push({ payload, delayMs });
}

// 1. Pre-ON fuel, then ON status without fuel. Opening should come from the earlier fuel-bearing packet.
add(fuelMessage(0, 120.4, 31));
add(fuelMessage(8, 120.1, 31, { speed: 2 }));
add(fuelMessage(12, 0, 0, { includeFuel: false, driverName: 'ENGINE ON' }));

// 2. Moving/sloshing while engine ON. These should not create fills or thefts.
add(fuelMessage(25, 119.3, 30, { speed: 38, lat: -26.2045, lng: 28.0481 }));
add(fuelMessage(40, 118.4, 30, { speed: 42, lat: -26.2051, lng: 28.0490 }));
add(fuelMessage(55, 121.0, 31, { speed: 35, lat: -26.2060, lng: 28.0500 }));
add(fuelMessage(70, 117.9, 30, { speed: 28, lat: -26.2068, lng: 28.0510 }));

// 3. OFF status without fuel, then the first later fuel packet. Closing should come from the later packet.
add(fuelMessage(90, 0, 0, { includeFuel: false, driverName: 'ENGINE OFF' }));
add(fuelMessage(102, 117.8, 30, { speed: 0, lat: -26.2070, lng: 28.0512 }));

// 4. Resting on an incline with small slosh while OFF. Should not trigger events.
add(fuelMessage(118, 118.2, 30, { speed: 0, geozone: 'Depot Incline' }));
add(fuelMessage(134, 117.6, 30, { speed: 0, geozone: 'Depot Incline' }));
add(fuelMessage(150, 118.5, 30, { speed: 0, geozone: 'Depot Incline' }));

// 5. Real engine-OFF fill: +12L in under 2 minutes, with slosh on top while parked.
add(fuelMessage(165, 118.1, 30, { speed: 0, geozone: 'Depot Fill Bay' }));
add(fuelMessage(190, 123.8, 32, { speed: 0, geozone: 'Depot Fill Bay' }));
add(fuelMessage(215, 129.6, 34, { speed: 0, geozone: 'Depot Fill Bay' }));
add(fuelMessage(240, 130.4, 34, { speed: 0, geozone: 'Depot Fill Bay' }));
add(fuelMessage(265, 129.9, 34, { speed: 0, geozone: 'Depot Fill Bay' }));
add(fuelMessage(290, 130.2, 34, { speed: 0, geozone: 'Depot Fill Bay' }));

// 6. Start engine again with no fuel in the status packet.
add(fuelMessage(320, 0, 0, { includeFuel: false, driverName: 'ENGINE ON' }));

// 7. Large engine-ON increase that must be ignored for fill detection.
add(fuelMessage(340, 129.7, 34, { speed: 24 }));
add(fuelMessage(360, 142.5, 37, { speed: 18 }));
add(fuelMessage(380, 141.2, 36, { speed: 20 }));

// 8. Stop engine again, with later closing fuel.
add(fuelMessage(405, 0, 0, { includeFuel: false, driverName: 'ENGINE OFF' }));
add(fuelMessage(420, 141.0, 36, { speed: 0, geozone: 'Hilltop Yard' }));

// 9. Engine-OFF theft: >50L drop while stationary, then stabilize.
add(fuelMessage(450, 140.7, 36, { speed: 0, geozone: 'Hilltop Yard' }));
add(fuelMessage(475, 111.0, 28, { speed: 0, geozone: 'Hilltop Yard' }));
add(fuelMessage(500, 88.4, 22, { speed: 0, geozone: 'Hilltop Yard' }));
add(fuelMessage(525, 86.8, 22, { speed: 0, geozone: 'Hilltop Yard' }));
add(fuelMessage(550, 87.2, 22, { speed: 0, geozone: 'Hilltop Yard' }));

// 10. Long quiet tail so the server keeps receiving packets while watcher stabilization time elapses.
add(fuelMessage(620, 87.0, 22, { speed: 0, geozone: 'Hilltop Yard' }), 500);
add(fuelMessage(680, 87.1, 22, { speed: 0, geozone: 'Hilltop Yard' }), 500);
add(fuelMessage(740, 87.0, 22, { speed: 0, geozone: 'Hilltop Yard' }), 500);

console.log(`Dummy Waterford event websocket listening on ws://localhost:${port}`);
console.log(`Plate under test: ${plate}`);
console.log(`Messages queued: ${messages.length}`);

wss.on('connection', (ws) => {
  console.log('Client connected to dummy websocket');
  let index = 0;

  const sendNext = () => {
    if (index >= messages.length) {
      console.log('Scenario complete, keeping socket open for watcher stabilization checks');
      return;
    }

    const { payload, delayMs } = messages[index++];
    ws.send(JSON.stringify(payload));
    console.log(`[${index}/${messages.length}] ${payload.LocTime} ${payload.DriverName || 'FUEL'} fuel=${payload.fuel_probe_1_volume_in_tank ?? 'n/a'} speed=${payload.Speed}`);
    setTimeout(sendNext, delayMs);
  };

  sendNext();
});

process.on('SIGINT', () => {
  console.log('Shutting down dummy websocket');
  wss.close(() => process.exit(0));
});
