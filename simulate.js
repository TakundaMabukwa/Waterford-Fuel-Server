const WebSocket = require('ws');

const PORT = 8093;
const wss = new WebSocket.Server({ port: PORT });

const PLATE = 'JM39BBGP';

const toHex = (val, bytes) => Math.round(val).toString(16).toUpperCase().padStart(bytes * 2, '0');

const createFuelHex = (level1, vol1, temp1, pct1, level2, vol2, temp2, pct2) => {
  return [
    '25', '405', '1004',
    '2020', toHex(level1, 4),
    '2021', toHex(vol1, 4),
    '2022', toHex(temp1, 2),
    '2023', toHex(pct1, 2),
    '2024', toHex(level2, 4),
    '2025', toHex(vol2, 4),
    '2026', toHex(temp2, 2),
    '2027', toHex(pct2, 2)
  ].join(',');
};

const createMessage = (overrides = {}) => {
  const defaults = {
    plate: PLATE,
    speed: 0,
    lat: -26.2041,
    lon: 28.0473,
    locTime: '2025-07-01 08:00:00',
    mileage: 135000,
    pocsagstr: '53.75.1.61',
    status: '',
    fuelDataRaw: '',
    itemInstalled: `${PLATE} - BUZZER AND CAN`,
    geozone: 'DEPOT',
    driverName: 'Driver JM39'
  };
  const m = { ...defaults, ...overrides };
  return `^${m.plate}|${m.speed}|${m.lat}|${m.lon}|${m.locTime}|${m.mileage}|${m.pocsagstr}|${m.status}|${m.fuelDataRaw}|${m.itemInstalled}|${m.geozone}|${m.driverName}^`;
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const seqTime = (baseMin) => {
  const t = new Date('2025-07-01T06:00:00Z');
  t.setMinutes(t.getMinutes() + baseMin);
  return t.toISOString().replace('T', ' ').replace('Z', '').substring(0, 19);
};

wss.on('connection', async (ws) => {
  console.log(`[sim] Client connected`);

  const send = (msg) => {
    console.log(`[sim] SEND: ${msg.substring(0, 180)}`);
    ws.send(msg);
  };

  await sleep(2000);

  console.log('\n[sim] === PHASE 1: Stationary — engine off, fuel stable ~402.5L / ~464.5L ===');
  for (let i = 0; i < 3; i++) {
    send(createMessage({
      speed: 0,
      locTime: seqTime(i * 2),
      mileage: 135000,
      fuelDataRaw: createFuelHex(6440, 402, 18, 100, 7432, 464, 18, 96)
    }));
    await sleep(400);
  }

  console.log('\n[sim] === PHASE 2: ENGINE ON — opening fuel ~402L / ~464L ===');
  send(createMessage({
    status: 'Engine On',
    speed: 0,
    locTime: seqTime(6),
    mileage: 135000,
    fuelDataRaw: createFuelHex(6440, 402, 18, 100, 7432, 464, 18, 96)
  }));
  await sleep(800);

  console.log('\n[sim] === PHASE 3: Driving — fuel decreasing ===');
  const driveData = [
    { min: 8,  spd: 31,  lvl1: 6432, vol1: 401, lvl2: 7424, vol2: 463, spd2: 31 },
    { min: 10, spd: 60,  lvl1: 6420, vol1: 400, lvl2: 7412, vol2: 462, spd2: 60 },
    { min: 12, spd: 84,  lvl1: 6408, vol1: 399, lvl2: 7400, vol2: 461, spd2: 84 },
    { min: 14, spd: 72,  lvl1: 6396, vol1: 398, lvl2: 7388, vol2: 460, spd2: 72 },
    { min: 16, spd: 45,  lvl1: 6384, vol1: 397, lvl2: 7376, vol2: 459, spd2: 45 },
    { min: 18, spd: 60,  lvl1: 6372, vol1: 396, lvl2: 7364, vol2: 458, spd2: 60 },
    { min: 20, spd: 55,  lvl1: 6360, vol1: 395, lvl2: 7352, vol2: 457, spd2: 55 },
  ];
  for (const d of driveData) {
    send(createMessage({
      speed: d.spd,
      locTime: seqTime(d.min),
      mileage: 135000 + d.min * 5,
      fuelDataRaw: createFuelHex(d.lvl1, d.vol1, 19, 99, d.lvl2, d.vol2, 19, 95)
    }));
    await sleep(300);
  }

  console.log('\n[sim] === PHASE 4: ENGINE OFF — closing fuel ~395L / ~457L ===');
  send(createMessage({
    status: 'Engine Off',
    speed: 0,
    locTime: seqTime(22),
    mileage: 135110,
    fuelDataRaw: createFuelHex(6320, 395, 20, 98, 7340, 457, 20, 94)
  }));
  await sleep(800);

  console.log('\n[sim] === PHASE 5: Engine off — fuel stable ===');
  for (let i = 0; i < 2; i++) {
    send(createMessage({
      speed: 0,
      locTime: seqTime(24 + i * 2),
      mileage: 135110,
      fuelDataRaw: createFuelHex(6320, 395, 20, 98, 7340, 457, 20, 94)
    }));
    await sleep(400);
  }

  console.log('\n[sim] === PHASE 6: FUEL FILL — 12L increase in 1.5 minutes (462→457 probe2) ===');
  send(createMessage({
    speed: 0,
    locTime: seqTime(28),
    mileage: 135110,
    fuelDataRaw: createFuelHex(6528, 408, 21, 100, 7568, 471, 21, 98)
  }));
  await sleep(300);
  send(createMessage({
    speed: 0,
    locTime: seqTime(29),
    mileage: 135110,
    fuelDataRaw: createFuelHex(6544, 409, 21, 100, 7584, 472, 21, 98)
  }));
  await sleep(300);

  console.log('\n[sim] === PHASE 7: Post-fill stable — fuel ~409L / ~472L ===');
  send(createMessage({
    speed: 0,
    locTime: seqTime(30),
    mileage: 135110,
    fuelDataRaw: createFuelHex(6544, 409, 21, 100, 7584, 472, 21, 98)
  }));
  await sleep(800);

  console.log('\n[sim] === PHASE 8: ENGINE ON — opening fuel ~409L / ~472L ===');
  send(createMessage({
    status: 'Engine On',
    speed: 0,
    locTime: seqTime(32),
    mileage: 135110,
    fuelDataRaw: createFuelHex(6544, 409, 21, 100, 7584, 472, 21, 98)
  }));
  await sleep(800);

  console.log('\n[sim] === PHASE 9: Driving after fill ===');
  const driveData2 = [
    { min: 34, spd: 40,  lvl1: 6528, vol1: 408, lvl2: 7568, vol2: 471 },
    { min: 36, spd: 70,  lvl1: 6512, vol1: 407, lvl2: 7552, vol2: 470 },
    { min: 38, spd: 85,  lvl1: 6496, vol1: 406, lvl2: 7536, vol2: 469 },
    { min: 40, spd: 65,  lvl1: 6480, vol1: 405, lvl2: 7520, vol2: 468 },
  ];
  for (const d of driveData2) {
    send(createMessage({
      speed: d.spd,
      locTime: seqTime(d.min),
      mileage: 135110 + (d.min - 32) * 5,
      fuelDataRaw: createFuelHex(d.lvl1, d.vol1, 22, 99, d.lvl2, d.vol2, 22, 97)
    }));
    await sleep(300);
  }

  console.log('\n[sim] === PHASE 10: ENGINE OFF — closing ~405L / ~468L ===');
  send(createMessage({
    status: 'Engine Off',
    speed: 0,
    locTime: seqTime(42),
    mileage: 135130,
    fuelDataRaw: createFuelHex(6480, 405, 22, 99, 7488, 468, 22, 97)
  }));

  console.log('\n[sim] === SIMULATION COMPLETE ===');
  console.log('[sim] Expected sessions:');
  console.log('  Session 1: Engine ON at 06:06, Engine OFF at 06:22 → usage ~7L probe1 + 7L probe2');
  console.log('  Session 2: Engine ON at 06:32, Engine OFF at 06:42 → usage ~4L probe1 + 4L probe2');
  console.log('[sim] Expected fills:');
  console.log('  Fill 1: ~13L increase at 06:28 (SESSION_COMPARISON on session 2 start)');
  console.log('  Fill 2: ~13L detected by OPERATION_10L_2MIN between 06:28-06:29');
  await sleep(3000);
  process.exit(0);
});

console.log(`[sim] WebSocket server listening on ws://localhost:${PORT}`);
