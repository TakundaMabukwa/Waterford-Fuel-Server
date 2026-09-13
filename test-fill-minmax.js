// E2E test for simplified geozone fill detection:
//   preFill  = lowest of last 5 fuel messages before/at zone entry
//   postFill = first fuel value after zone exit
//   fill     = postFill - preFill (always stored)
// Uses the REAL waterford-ws-client + decoder against a mock WS server,
// with stubbed db + geozone modules (no Postgres / Supabase needed).
const assert = require('assert');
const WebSocket = require('ws');

const state = { history: [], fills: [], reviewActions: [], geoEvents: [], thefts: [] };

const combinedOf = (h) => (h.fuel_probe_1_volume_in_tank || 0) + (h.fuel_probe_2_volume_in_tank || 0);

// ---- stub waterford-db BEFORE the client loads it ----
const dbPath = require.resolve('./waterford-db');
require.cache[dbPath] = {
  id: dbPath, filename: dbPath, loaded: true,
  exports: {
    isKnownVehicle: () => true,
    getCostCode: () => 'WATE-0001',
    insertHistory: async (row) => { state.history.push(row); },
    upsertLatest: async () => {},
    getLatestFuelBefore: async (plate, beforeTime) => {
      const fuelRows = state.history.filter((h) => h.plate === plate && combinedOf(h) > 0 && h.loc_time < beforeTime);
      return fuelRows.length > 0 ? fuelRows[fuelRows.length - 1] : null;
    },
    insertFillSession: async (s) => { state.fills.push(s); },
    insertTheftSession: async (s) => { state.thefts.push(s); },
    insertGeozoneEvent: async (e) => { state.geoEvents.push(e); },
    getLatestFuelReading: async () => null,
  },
};

// ---- stub waterford-geozone: inside zone only at lat 10 / lon 10 ----
const geoPath = require.resolve('./waterford-geozone');
require.cache[geoPath] = {
  id: geoPath, filename: geoPath, loaded: true,
  exports: {
    findFuelStop: async (lat, lon) =>
      (lat === 10 && lon === 10 ? { id: 7, name: 'TEST STOP', geozone_name: 'TEST STOP' } : null),
    insertFuelReviewAction: async (plate, type, amount, locTime) => {
      state.reviewActions.push({ plate, type, amount, locTime });
    },
    syncFuelStops: async () => 0,
  },
};

const { createClient } = require('./waterford-ws-client');

const PORT = 18099;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fuelRaw = (vol) => `WF,405,8,2021,${Math.round(vol * 10).toString(16).toUpperCase()}`;
const T = (m) => `2026-09-09T10:${String(m).padStart(2, '0')}:00.000+00:00`;
const msg = (plate, lat, lon, t, fuelVol) =>
  `^${plate}|0|${lat}|${lon}|${T(t)}|12345|||${fuelVol === null ? '' : fuelRaw(fuelVol)}|Installed||TestDriver^`;

async function sendAll(socket, list) {
  for (const m of list) {
    socket.send(m);
    await sleep(120);
  }
  await sleep(500);
}

async function run() {
  const wss = new WebSocket.Server({ port: PORT });
  const client = createClient(`ws://127.0.0.1:${PORT}`);
  const connected = new Promise((resolve) => wss.on('connection', (s) => resolve(s)));
  client.connect();
  const socket = await connected;
  await sleep(300);

  // Scenario 1: exit message carries fuel -> immediate capture. Expect fill 700-480=220.
  await sendAll(socket, [
    msg('S1', 5, 5, 0, 500), msg('S1', 5, 5, 1, 500), msg('S1', 5, 5, 2, 500),
    msg('S1', 5, 5, 3, 500), msg('S1', 5, 5, 4, 500),
    msg('S1', 10, 10, 5, 480),
    msg('S1', 10, 10, 6, 470),
    msg('S1', 5, 5, 7, 700),
  ]);

  // Scenario 2: exit without fuel -> DB fallback gets last reading before exit = entry reading. No fill.
  await sendAll(socket, [
    msg('S2', 5, 5, 10, 300), msg('S2', 5, 5, 11, 300), msg('S2', 5, 5, 12, 300),
    msg('S2', 5, 5, 13, 300), msg('S2', 5, 5, 14, 300),
    msg('S2', 10, 10, 15, 290),
    msg('S2', 5, 5, 16, null),
    msg('S2', 5, 5, 17, null),
    msg('S2', 5, 5, 18, 600),
  ]);

  // Scenario 3: no fuel at entry -> preFill retry in zone. Expect fill 500-200=300.
  await sendAll(socket, [
    msg('S3', 5, 5, 20, null), msg('S3', 5, 5, 21, null), msg('S3', 5, 5, 22, null),
    msg('S3', 10, 10, 23, null),
    msg('S3', 10, 10, 24, 200),
    msg('S3', 5, 5, 25, 500),
  ]);

  // Scenario 4: re-enter while waiting -> finalize previous + start new. Expect 310 then 410.
  await sendAll(socket, [
    msg('S4', 5, 5, 30, 100), msg('S4', 5, 5, 31, 100), msg('S4', 5, 5, 32, 100),
    msg('S4', 5, 5, 33, 100), msg('S4', 5, 5, 34, 100),
    msg('S4', 10, 10, 35, 90),
    msg('S4', 5, 5, 36, null),
    msg('S4', 10, 10, 37, 400),
    msg('S4', 5, 5, 38, 500),
  ]);

  // Scenario 5: never any pre-fill fuel -> discard, no fill stored.
  await sendAll(socket, [
    msg('S5', 5, 5, 40, null),
    msg('S5', 10, 10, 41, null),
    msg('S5', 5, 5, 42, 500),
  ]);

  const fillsFor = (p) => state.fills.filter((f) => f.branch === p);

  assert.strictEqual(fillsFor('S1').length, 1, 'S1 one fill');
  assert.strictEqual(fillsFor('S1')[0].opening_fuel, 480, 'S1 preFill = lowest of last 5');
  assert.strictEqual(fillsFor('S1')[0].closing_fuel, 700, 'S1 postFill = exit msg fuel');
  assert.strictEqual(fillsFor('S1')[0].total_fill, 220, 'S1 fill = 220');

  assert.strictEqual(fillsFor('S2').length, 0, 'S2 no fill - exit without fuel, DB fallback returns entry reading');

  assert.strictEqual(fillsFor('S3').length, 1, 'S3 one fill');
  assert.strictEqual(fillsFor('S3')[0].opening_fuel, 200, 'S3 preFill via retry');
  assert.strictEqual(fillsFor('S3')[0].total_fill, 300, 'S3 fill = 300');

  assert.strictEqual(fillsFor('S4').length, 1, 'S4 one fill (first exit discarded, second captured)');
  assert.strictEqual(fillsFor('S4')[0].opening_fuel, 400, 'S4 preFill = entry fuel at re-enter');
  assert.strictEqual(fillsFor('S4')[0].closing_fuel, 500, 'S4 postFill = exit fuel');
  assert.strictEqual(fillsFor('S4')[0].total_fill, 100, 'S4 fill = 500-400 = 100');

  assert.strictEqual(fillsFor('S5').length, 0, 'S5 no fill without preFill');

  assert.strictEqual(state.fills.length, state.reviewActions.length, 'every fill has a review action');
  assert.strictEqual(state.thefts.length, 0, 'no thefts triggered (statuses empty)');
  const enters = state.geoEvents.filter((e) => e.event_type === 'ZONE_ENTER').length;
  const exits = state.geoEvents.filter((e) => e.event_type === 'ZONE_EXIT').length;
  const detected = state.geoEvents.filter((e) => e.event_type === 'FILL_DETECTED').length;
  assert.strictEqual(enters, 6, 'six zone enters (S4 enters twice)');
  assert.strictEqual(exits, 6, 'six zone exits (S4 exits twice)');
  assert.strictEqual(detected, 3, 'three FILL_DETECTED events (S1, S3, S4)');

  console.log('ALL FILL-MINMAX TESTS PASSED');
  console.log(`fills=${state.fills.length} reviewActions=${state.reviewActions.length} enters=${enters} exits=${exits} detected=${detected}`);

  client.close();
  wss.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('TEST FAILED:', err.message);
  process.exit(1);
});
