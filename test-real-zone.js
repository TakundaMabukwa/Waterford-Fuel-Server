// E2E test using REAL Oilco Musina polygon coordinates and realistic fuel values.
// Simulates a vehicle entering, filling up, and exiting a real fuel zone.
// Some messages intentionally have NO fuel data to test the DB fallback path.
const assert = require('assert');
const WebSocket = require('ws');

const state = { history: [], fills: [], reviewActions: [], geoEvents: [], thefts: [] };
const combinedOf = (h) => (h.fuel_probe_1_volume_in_tank || 0) + (h.fuel_probe_2_volume_in_tank || 0);

// ---- stub waterford-db ----
const dbPath = require.resolve('./waterford-db');
require.cache[dbPath] = {
  id: dbPath, filename: dbPath, loaded: true,
  exports: {
    isKnownVehicle: () => true,
    getCostCode: () => 'WATE-0001',
    insertHistory: async (row) => { state.history.push(row); },
    upsertLatest: async () => {},
    getLatestFuelBefore: async (plate, beforeTime) => {
      const fuelRows = state.history.filter(
        (h) => h.plate === plate && combinedOf(h) > 0 && h.loc_time < beforeTime
      );
      return fuelRows.length > 0 ? fuelRows[fuelRows.length - 1] : null;
    },
    insertFillSession: async (s) => { state.fills.push(s); },
    insertTheftSession: async (s) => { state.thefts.push(s); },
    insertGeozoneEvent: async (e) => { state.geoEvents.push(e); },
    getLatestFuelReading: async () => null,
  },
};

// ---- stub waterford-geozone: real Oilco Musina polygon ----
// Polygon from fuel_stops id=20: [[30.007981,-22.297224],[30.009864,-22.296447],[30.009137,-22.295265],[30.007341,-22.295621]]
const OILCO_MUSINA = {
  id: 20,
  name: 'Oilco Musina',
  geozone_name: 'Oilco Musina',
  coordinates: [[30.007981,-22.297224],[30.009864,-22.296447],[30.009137,-22.295265],[30.007341,-22.295621]],
};

function pointInPolygon(lat, lon, polygon) {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i][0], yi = polygon[i][1];
    const xj = polygon[j][0], yj = polygon[j][1];
    if ((yi > lat) !== (yj > lat) && lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

const geoPath = require.resolve('./waterford-geozone');
require.cache[geoPath] = {
  id: geoPath, filename: geoPath, loaded: true,
  exports: {
    findFuelStop: async (lat, lon) =>
      pointInPolygon(lat, lon, OILCO_MUSINA.coordinates) ? OILCO_MUSINA : null,
    insertFuelReviewAction: async (plate, type, amount, locTime) => {
      state.reviewActions.push({ plate, type, amount, locTime });
    },
    syncFuelStops: async () => 0,
  },
};

const { createClient } = require('./waterford-ws-client');

const PORT = 18099;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Real coordinates
const INSIDE = { lat: -22.2961, lon: 30.0086 };   // centroid of Oilco Musina (verified inside)
const OUTSIDE = { lat: -22.294, lon: 30.007 };     // north of polygon (verified outside)

const fuelRaw = (vol) => `WF,405,8,2021,${Math.round(vol * 10).toString(16).toUpperCase()}`;
const T = (s) => `2026-09-13T08:${String(s).padStart(2, '0')}:00.000+00:00`;

const msg = (plate, lat, lon, t, fuelVol) =>
  `^${plate}|0|${lat}|${lon}|${T(t)}|12345|||${fuelVol === null ? '' : fuelRaw(fuelVol)}|Installed||TestDriver^`;

async function sendAll(socket, list) {
  for (const m of list) {
    socket.send(m);
    await sleep(100);
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

  const fillsFor = (p) => state.fills.filter((f) => f.branch === p);
  const eventsFor = (p) => state.geoEvents.filter((e) => e.plate === p);

  // =========================================================
  // TEST 1: Normal fill — entry with fuel, exit with fuel
  // Vehicle approaches at 750L, enters zone, fills to 965L, exits
  // =========================================================
  console.log('\n--- TEST 1: Normal fill (entry fuel + exit fuel) ---');
  await sendAll(socket, [
    msg('T1PLATE', OUTSIDE.lat, OUTSIDE.lon, 0, 752),   // approach
    msg('T1PLATE', OUTSIDE.lat, OUTSIDE.lon, 1, 751),   // approaching
    msg('T1PLATE', INSIDE.lat, INSIDE.lon, 2, 750),     // ENTER zone, fuel=750
    msg('T1PLATE', INSIDE.lat, INSIDE.lon, 3, 749),     // inside, burning fuel
    msg('T1PLATE', INSIDE.lat, INSIDE.lon, 4, 748),     // inside, burning
    msg('T1PLATE', INSIDE.lat, INSIDE.lon, 5, 820),     // FILLING
    msg('T1PLATE', INSIDE.lat, INSIDE.lon, 6, 920),     // FILLING
    msg('T1PLATE', OUTSIDE.lat, OUTSIDE.lon, 7, 965),   // EXIT, fuel=965
  ]);

  assert.strictEqual(fillsFor('T1PLATE').length, 1, 'T1: one fill recorded');
  assert.strictEqual(fillsFor('T1PLATE')[0].opening_fuel, 750, 'T1: preFill = entry fuel');
  assert.strictEqual(fillsFor('T1PLATE')[0].closing_fuel, 965, 'T1: postFill = exit fuel');
  assert.strictEqual(fillsFor('T1PLATE')[0].total_fill, 215, 'T1: fill = 965-750 = 215');
  console.log('  PASS: pre=750, post=965, fill=215');

  // =========================================================
  // TEST 2: Entry without fuel -> DB fallback
  // Vehicle enters with no fuel data, next message has fuel
  // =========================================================
  console.log('\n--- TEST 2: Entry without fuel -> DB fallback ---');
  await sendAll(socket, [
    msg('T2PLATE', OUTSIDE.lat, OUTSIDE.lon, 10, 600),  // approach with fuel
    msg('T2PLATE', OUTSIDE.lat, OUTSIDE.lon, 11, 599),  // approaching
    msg('T2PLATE', INSIDE.lat, INSIDE.lon, 12, null),   // ENTER zone, NO fuel
    msg('T2PLATE', INSIDE.lat, INSIDE.lon, 13, 598),    // in zone, fuel=598 (DB fallback gets 599 from before entry)
    msg('T2PLATE', OUTSIDE.lat, OUTSIDE.lon, 14, 700),  // EXIT, fuel=700
  ]);

  assert.strictEqual(fillsFor('T2PLATE').length, 1, 'T2: one fill recorded');
  assert.strictEqual(fillsFor('T2PLATE')[0].opening_fuel, 599, 'T2: preFill = last fuel before entry (DB fallback)');
  assert.strictEqual(fillsFor('T2PLATE')[0].closing_fuel, 700, 'T2: postFill = exit fuel');
  assert.strictEqual(fillsFor('T2PLATE')[0].total_fill, 101, 'T2: fill = 700-599 = 101');
  console.log('  PASS: pre=599 (fallback), post=700, fill=101');

  // =========================================================
  // TEST 3: Exit without fuel -> DB fallback
  // Vehicle fills inside zone, exit message has no fuel
  // =========================================================
  console.log('\n--- TEST 3: Exit without fuel -> DB fallback ---');
  await sendAll(socket, [
    msg('T3PLATE', OUTSIDE.lat, OUTSIDE.lon, 20, 400),  // approach
    msg('T3PLATE', INSIDE.lat, INSIDE.lon, 21, 399),    // ENTER, fuel=399
    msg('T3PLATE', INSIDE.lat, INSIDE.lon, 22, 398),    // inside
    msg('T3PLATE', INSIDE.lat, INSIDE.lon, 23, 550),    // FILLING
    msg('T3PLATE', OUTSIDE.lat, OUTSIDE.lon, 24, null), // EXIT, NO fuel
    msg('T3PLATE', OUTSIDE.lat, OUTSIDE.lon, 25, 600),  // after exit, fuel=600
  ]);

  assert.strictEqual(fillsFor('T3PLATE').length, 1, 'T3: one fill recorded');
  assert.strictEqual(fillsFor('T3PLATE')[0].opening_fuel, 399, 'T3: preFill = entry fuel');
  assert.strictEqual(fillsFor('T3PLATE')[0].closing_fuel, 550, 'T3: postFill = DB fallback (last fuel before exit)');
  assert.strictEqual(fillsFor('T3PLATE')[0].total_fill, 151, 'T3: fill = 550-399 = 151');
  console.log('  PASS: pre=399, post=550 (fallback), fill=151');

  // =========================================================
  // TEST 4: No fill (vehicle passes through)
  // Vehicle enters, fuel goes DOWN, exits with less fuel
  // =========================================================
  console.log('\n--- TEST 4: No fill (fuel decreased) ---');
  await sendAll(socket, [
    msg('T4PLATE', OUTSIDE.lat, OUTSIDE.lon, 30, 500),  // approach
    msg('T4PLATE', INSIDE.lat, INSIDE.lon, 31, 498),    // ENTER, fuel=498
    msg('T4PLATE', INSIDE.lat, INSIDE.lon, 32, 496),    // burning
    msg('T4PLATE', INSIDE.lat, INSIDE.lon, 33, 494),    // burning
    msg('T4PLATE', OUTSIDE.lat, OUTSIDE.lon, 34, 490),  // EXIT, fuel=490 (burned 8L)
  ]);

  assert.strictEqual(fillsFor('T4PLATE').length, 0, 'T4: no fill (fuel decreased)');
  console.log('  PASS: fill discarded (negative)');

  // =========================================================
  // TEST 5: Below threshold (tiny fill < 10L)
  // =========================================================
  console.log('\n--- TEST 5: Below threshold (fill < 10L) ---');
  await sendAll(socket, [
    msg('T5PLATE', OUTSIDE.lat, OUTSIDE.lon, 40, 600),  // approach
    msg('T5PLATE', INSIDE.lat, INSIDE.lon, 41, 598),    // ENTER, fuel=598
    msg('T5PLATE', INSIDE.lat, INSIDE.lon, 42, 603),    // tiny increase (probe noise)
    msg('T5PLATE', OUTSIDE.lat, OUTSIDE.lon, 43, 605),  // EXIT, fuel=605
  ]);

  assert.strictEqual(fillsFor('T5PLATE').length, 0, 'T5: no fill (below 10L threshold)');
  console.log('  PASS: fill discarded (< 10L)');

  // =========================================================
  // TEST 6: Messages without fuel data inside zone
  // Vehicle enters with fuel, several no-fuel messages, then fill, exit with fuel
  // =========================================================
  console.log('\n--- TEST 6: Mixed fuel/no-fuel messages inside zone ---');
  const INSIDE_B = { lat: -22.2965, lon: 30.0088 }; // second point inside polygon
  await sendAll(socket, [
    msg('T6PLATE', OUTSIDE.lat, OUTSIDE.lon, 50, 300),  // approach
    msg('T6PLATE', INSIDE.lat, INSIDE.lon, 51, 298),    // ENTER, fuel=298
    msg('T6PLATE', INSIDE_B.lat, INSIDE_B.lon, 52, null),  // inside, no fuel
    msg('T6PLATE', INSIDE_B.lat, INSIDE_B.lon, 53, null),  // inside, no fuel
    msg('T6PLATE', INSIDE_B.lat, INSIDE_B.lon, 54, null),  // inside, no fuel
    msg('T6PLATE', OUTSIDE.lat, OUTSIDE.lon, 55, 500),  // EXIT, fuel=500
  ]);

  assert.strictEqual(fillsFor('T6PLATE').length, 1, 'T6: fill recorded despite no-fuel messages');
  assert.strictEqual(fillsFor('T6PLATE')[0].opening_fuel, 298, 'T6: preFill = entry fuel');
  assert.strictEqual(fillsFor('T6PLATE')[0].closing_fuel, 500, 'T6: postFill = exit fuel');
  assert.strictEqual(fillsFor('T6PLATE')[0].total_fill, 202, 'T6: fill = 500-298 = 202');
  console.log('  PASS: pre=298, post=500, fill=202');

  // =========================================================
  // SUMMARY
  // =========================================================
  const enters = state.geoEvents.filter((e) => e.event_type === 'ZONE_ENTER').length;
  const exits = state.geoEvents.filter((e) => e.event_type === 'ZONE_EXIT').length;
  const detected = state.geoEvents.filter((e) => e.event_type === 'FILL_DETECTED').length;

  console.log('\n=== RESULTS ===');
  console.log(`fills=${state.fills.length} reviewActions=${state.reviewActions.length} enters=${enters} exits=${exits} detected=${detected}`);

  assert.strictEqual(state.fills.length, state.reviewActions.length, 'every fill has a review action');

  console.log('\nALL REAL-ZONE TESTS PASSED');

  client.close();
  wss.close();
  process.exit(0);
}

run().catch((err) => {
  console.error('TEST FAILED:', err.message);
  process.exit(1);
});
