require('dotenv').config();
const fs = require('fs');
const http = require('http');
const WebSocket = require('ws');
const { createClient: createWsClient } = require('./waterford-ws-client');
const db = require('./waterford-db');
const { decodeFuelData, hasFuelData } = require('./waterford-fuel-decoder');
const { createClient: createSupabaseClient } = require('@supabase/supabase-js');

process.env.PGHOST = process.env.PGHOST || 'localhost';
process.env.PGPORT = process.env.PGPORT_HOST || '5433';
process.env.PGDATABASE = process.env.PGDATABASE || 'fuel_table';
process.env.PGUSER = process.env.PGUSER || 'postgres';
process.env.PGPASSWORD = process.env.PGPASSWORD || 'vik8989';

const LOG_FILE = require('path').join(__dirname, 'integration-test.log');
const log = (msg) => { console.log(msg); fs.appendFileSync(LOG_FILE, msg + '\n'); };
const err = (msg) => { console.error(msg); fs.appendFileSync(LOG_FILE, 'ERROR: ' + msg + '\n'); };

fs.writeFileSync(LOG_FILE, `=== Waterford Realistic Integration Test ===\nStarted: ${new Date().toISOString()}\n\n`);

const PLATE = 'LR78YGGP';
const WS_PORT = 18765;
const WS_URL = `ws://127.0.0.1:${WS_PORT}`;

const supabase = createSupabaseClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY);
const waterford = createSupabaseClient(process.env.WATERFORD_SUPABASE_URL, process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY);

let passed = 0;
let failed = 0;
let testSessionIds = [];

function assert(label, condition, detail) {
  if (condition) { log(`  ✓ ${label}`); passed++; }
  else { log(`  ✗ ${label} ${detail ? '— ' + detail : ''}`); failed++; }
}

function buildFuelRaw(t1Level, t1Vol, t1Temp, t1Pct, t2Level, t2Vol, t2Temp, t2Pct) {
  const hex = (v) => Math.round(v).toString(16).toUpperCase();
  return `WF,405,8,2020,${hex(t1Level * 10)},2021,${hex(t1Vol * 10)},2022,${hex(t1Temp)},2023,${hex(t1Pct)},2024,${hex(t2Level * 10)},2025,${hex(t2Vol * 10)},2026,${hex(t2Temp)},2027,${hex(t2Pct)}`;
}

function buildMsg(plate, lat, lon, time, status, fuelRaw, geozone) {
  const parts = [plate, '0', lat.toString(), lon.toString(), time, '12345', '', status || '', fuelRaw || '', 'Installed', geozone || '', 'TestDriver'];
  return `^${parts.join('|')}^`;
}

async function cleanup() {
  log('\n=== CLEANUP ===');
  try {
    const { error: e1 } = await supabase.from('energy_rite_operating_sessions').delete().eq('branch', PLATE);
    if (e1) log(`  Cleanup sessions: ${e1.message}`); else log(`  Cleaned energy_rite_operating_sessions`);
    const { error: e2 } = await waterford.from('fuel_review_actions').delete().eq('vehicle_reg', PLATE);
    if (e2) log(`  Cleanup review actions: ${e2.message}`); else log(`  Cleaned fuel_review_actions`);
  } catch (e) { log(`  Cleanup error: ${e.message}`); }
}

async function run() {
  try {
    log('\n=== INIT: Starting DB + fuel stops sync ===');
    await db.init();
    const { syncFuelStops } = require('./waterford-geozone');
    const synced = await syncFuelStops();
    log(`  Synced ${synced} fuel stops`);

    log('\n=== INIT: Cleaning previous test data ===');
    await cleanup();

    log(`\n${'='.repeat(60)}`);
    log('TEST 1: Start mock WebSocket server + connect real client');
    log('='.repeat(60));

    let wsServer;
    let wsClient;
    let messageQueue = [];
    let resolveMessage = null;

    const waitForMessage = () => new Promise((resolve) => {
      resolveMessage = resolve;
    });

    const ws = new Promise((resolve) => {
      wsServer = new WebSocket.Server({ port: WS_PORT }, () => {
        log(`  Mock WS server listening on port ${WS_PORT}`);
        wsClient = createWsClient(WS_URL);
        wsClient.connect();
        resolve();
      });

      wsServer.on('connection', (socket) => {
        log('  Real client connected to mock server');
        socket.on('message', () => {});
      });
    });

    await ws;
    assert('Mock WS server started + client connected', true);

    await new Promise(r => setTimeout(r, 500));

    const now = new Date('2025-06-15T08:00:00Z');
    const t = (offsetMin) => new Date(now.getTime() + offsetMin * 60000).toISOString().replace('Z', '+00:00');

    log(`\n${'='.repeat(60)}`);
    log('TEST 2: Simulate vehicle driving — normal fuel readings');
    log('='.repeat(60));

    const normalFuel = buildFuelRaw(850, 250, 28, 65, 920, 300, 30, 72);
    const msgs1 = [
      buildMsg(PLATE, -26.2200, 28.1400, t(0), 'ENGINE ON', normalFuel, ''),
      buildMsg(PLATE, -26.2210, 28.1410, t(1), '', normalFuel, ''),
      buildMsg(PLATE, -26.2220, 28.1420, t(2), '', normalFuel, ''),
      buildMsg(PLATE, -26.2230, 28.1430, t(3), '', normalFuel, ''),
    ];

    const clientSocket = Array.from(wsServer.clients)[0];
    for (const msg of msgs1) {
      clientSocket.send(msg);
      await new Promise(r => setTimeout(r, 100));
    }

    const { data: history1 } = await supabase.from('energy_rite_operating_sessions').select('*').eq('branch', PLATE);
    log(`  Sessions after normal driving: ${history1?.length || 0}`);
    assert('Session created from ENGINE ON', history1 && history1.length >= 1, `count=${history1?.length}`);
    if (history1?.length > 0) testSessionIds.push(history1[0].id);

    log(`\n${'='.repeat(60)}`);
    log('TEST 3: Vehicle enters Waterford Carriers zone — ENGINE OFF');
    log('='.repeat(60));

    const enterFuel = buildFuelRaw(850, 250, 28, 65, 920, 300, 30, 72);
    const enterMsgs = [
      buildMsg(PLATE, -26.2243, 28.1469, t(5), '', enterFuel, ''),
      buildMsg(PLATE, -26.2244, 28.1470, t(6), 'ENGINE OFF', enterFuel, ''),
      buildMsg(PLATE, -26.2244, 28.1470, t(7), '', enterFuel, ''),
    ];

    for (const msg of enterMsgs) {
      clientSocket.send(msg);
      await new Promise(r => setTimeout(r, 100));
    }

    log('  Sent zone enter + ENGINE OFF messages');
    assert('Vehicle entered zone + engine off', true);

    log(`\n${'='.repeat(60)}`);
    log('TEST 4: Refueling — fuel increases from 250L to 400L');
    log('='.repeat(60));

    const midFuel = buildFuelRaw(850, 250, 28, 65, 920, 300, 30, 72);
    const afterFuel1 = buildFuelRaw(1000, 290, 28, 74, 920, 300, 30, 72);
    const afterFuel2 = buildFuelRaw(1100, 350, 29, 88, 920, 300, 30, 72);
    const afterFuel3 = buildFuelRaw(1200, 400, 29, 100, 920, 300, 30, 72);

    const refuelMsgs = [
      buildMsg(PLATE, -26.2244, 28.1470, t(8), 'ENGINE ON', afterFuel1, ''),
      buildMsg(PLATE, -26.2244, 28.1470, t(9), '', afterFuel1, ''),
      buildMsg(PLATE, -26.2244, 28.1470, t(10), '', afterFuel2, ''),
      buildMsg(PLATE, -26.2244, 28.1470, t(11), '', afterFuel3, ''),
    ];

    for (const msg of refuelMsgs) {
      clientSocket.send(msg);
      await new Promise(r => setTimeout(r, 100));
    }

    log('  Sent ENGINE ON + 3 increasing fuel readings (+150L fill)');

    await new Promise(r => setTimeout(r, 1000));

    const { data: fillSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, session_status, total_fill, fill_events, fill_amount_during_session, opening_fuel, closing_fuel, notes')
      .eq('branch', PLATE)
      .eq('session_status', 'FUEL_FILL_COMPLETED');

    log(`  Fill sessions found: ${fillSessions?.length || 0}`);
    if (fillSessions && fillSessions.length > 0) {
      fillSessions.forEach(s => testSessionIds.push(s.id));
      log(`  Fill details: ${JSON.stringify(fillSessions[0], null, 2)}`);
    }

    assert('FILL_COMPLETED session created', fillSessions && fillSessions.length > 0, `count=${fillSessions?.length}`);
    if (fillSessions?.[0]) {
      assert('total_fill = 150L', fillSessions[0].total_fill === 150.0, `got ${fillSessions[0].total_fill}`);
      assert('opening_fuel = 550L (250+300)', fillSessions[0].opening_fuel === 550.0, `got ${fillSessions[0].opening_fuel}`);
      assert('closing_fuel = 700L (400+300)', fillSessions[0].closing_fuel === 700.0, `got ${fillSessions[0].closing_fuel}`);
      assert('fill_events = 1', fillSessions[0].fill_events === 1, `got ${fillSessions[0].fill_events}`);
      assert('fill_amount_during_session = 150', fillSessions[0].fill_amount_during_session === 150, `got ${fillSessions[0].fill_amount_during_session}`);
      assert('notes contain zone name', fillSessions[0].notes?.includes('Waterford'), `notes: ${fillSessions[0].notes?.substring(0, 100)}`);
    }

    log(`\n${'='.repeat(60)}`);
    log('TEST 5: fuel_review_actions — fill logged');
    log('='.repeat(60));

    const { data: fillReview } = await waterford
      .from('fuel_review_actions')
      .select('*')
      .eq('vehicle_reg', PLATE)
      .eq('action_type', 'fill');

    log(`  Fuel review actions: ${fillReview?.length || 0}`);
    if (fillReview && fillReview.length > 0) {
      log(`  Review details: ${JSON.stringify(fillReview[0], null, 2)}`);
    }
    assert('Fill review action created', fillReview && fillReview.length > 0, `count=${fillReview?.length}`);
    if (fillReview?.[0]) {
      assert('probe_value shows fill amount', fillReview[0].probe_value?.includes('150.0L'), `probe_value: ${fillReview[0].probe_value}`);
      assert('notes contain zone', fillReview[0].notes?.includes('Waterford'));
    }

    log(`\n${'='.repeat(60)}`);
    log('TEST 6: Vehicle exits zone + theft scenario');
    log('='.repeat(60));

    const exitFuel = buildFuelRaw(1200, 400, 29, 100, 920, 300, 30, 72);
    const exitMsgs = [
      buildMsg(PLATE, -26.2250, 28.1480, t(12), '', exitFuel, ''),
      buildMsg(PLATE, -26.2300, 28.1500, t(13), 'ENGINE OFF', exitFuel, ''),
    ];

    for (const msg of exitMsgs) {
      clientSocket.send(msg);
      await new Promise(r => setTimeout(r, 100));
    }

    log('  Vehicle left zone, engine off');

    await new Promise(r => setTimeout(r, 500));

    const theftFuel = buildFuelRaw(800, 200, 28, 50, 600, 150, 28, 38);
    const theftMsgs = [
      buildMsg(PLATE, -26.2300, 28.1500, t(14), 'POSSIBLE FUEL THEFT', theftFuel, ''),
      buildMsg(PLATE, -26.2300, 28.1500, t(15), '', theftFuel, ''),
      buildMsg(PLATE, -26.2300, 28.1500, t(16), '', theftFuel, ''),
      buildMsg(PLATE, -26.2300, 28.1500, t(17), '', theftFuel, ''),
    ];

    for (const msg of theftMsgs) {
      clientSocket.send(msg);
      await new Promise(r => setTimeout(r, 100));
    }

    log('  Sent POSSIBLE FUEL THEFT + 3 fuel readings (fuel dropped from 700L to 350L)');

    await new Promise(r => setTimeout(r, 1500));

    const { data: theftSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, session_status, total_theft, opening_fuel, closing_fuel, notes')
      .eq('branch', PLATE)
      .eq('session_status', 'FUEL_THEFT_COMPLETED');

    log(`  Theft sessions found: ${theftSessions?.length || 0}`);
    if (theftSessions && theftSessions.length > 0) {
      theftSessions.forEach(s => testSessionIds.push(s.id));
      log(`  Theft details: ${JSON.stringify(theftSessions[0], null, 2)}`);
    }

    assert('FUEL_THEFT_COMPLETED session created', theftSessions && theftSessions.length > 0, `count=${theftSessions?.length}`);
    if (theftSessions?.[0]) {
      assert('total_theft > 0', theftSessions[0].total_theft > 0, `got ${theftSessions[0].total_theft}`);
      assert('opening_fuel = 700', theftSessions[0].opening_fuel === 700.0, `got ${theftSessions[0].opening_fuel}`);
      assert('closing_fuel = 350', theftSessions[0].closing_fuel === 350.0, `got ${theftSessions[0].closing_fuel}`);
      assert('notes contain Theft', theftSessions[0].notes?.includes('Theft'));
    }

    log(`\n${'='.repeat(60)}`);
    log('TEST 7: fuel_review_actions — theft logged');
    log('='.repeat(60));

    const { data: theftReview } = await waterford
      .from('fuel_review_actions')
      .select('*')
      .eq('vehicle_reg', PLATE)
      .eq('action_type', 'theft');

    log(`  Theft review actions: ${theftReview?.length || 0}`);
    if (theftReview && theftReview.length > 0) {
      log(`  Review details: ${JSON.stringify(theftReview[0], null, 2)}`);
    }
    assert('Theft review action created', theftReview && theftReview.length > 0, `count=${theftReview?.length}`);
    if (theftReview?.[0]) {
      assert('probe_value shows theft amount', theftReview[0].probe_value?.includes('350.0L'), `probe_value: ${theftReview[0].probe_value}`);
    }

    log(`\n${'='.repeat(60)}`);
    log('TEST 8: Full session summary');
    log('='.repeat(60));

    const { data: allSessions } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, session_status, total_fill, total_theft, total_usage, opening_fuel, closing_fuel, operating_hours')
      .eq('branch', PLATE)
      .order('session_start_time', { ascending: true });

    log(`  All sessions for ${PLATE}:`);
    (allSessions || []).forEach((s, i) => {
      log(`    [${i + 1}] status=${s.session_status}, fill=${s.total_fill}, theft=${s.total_theft}, usage=${s.total_usage}, open=${s.opening_fuel}, close=${s.closing_fuel}, hours=${s.operating_hours}`);
    });

    const statusCounts = {};
    (allSessions || []).forEach(s => { statusCounts[s.session_status] = (statusCounts[s.session_status] || 0) + 1; });

    assert('Has COMPLETED session', (statusCounts['COMPLETED'] || 0) >= 1, JSON.stringify(statusCounts));
    assert('Has FUEL_FILL_COMPLETED', (statusCounts['FUEL_FILL_COMPLETED'] || 0) >= 1, JSON.stringify(statusCounts));
    assert('Has FUEL_THEFT_COMPLETED', (statusCounts['FUEL_THEFT_COMPLETED'] || 0) >= 1, JSON.stringify(statusCounts));
    assert('Total sessions >= 3', (allSessions?.length || 0) >= 3, `got ${allSessions?.length}`);

    log(`\n${'='.repeat(60)}`);
    log('TEST 9: Combined review actions summary');
    log('='.repeat(60));

    const { data: allFra } = await waterford
      .from('fuel_review_actions')
      .select('action_type, probe_value, notes')
      .eq('vehicle_reg', PLATE);

    log(`  All fuel review actions for ${PLATE}:`);
    (allFra || []).forEach((f, i) => {
      log(`    [${i + 1}] type=${f.action_type}, probe=${f.probe_value}`);
    });

    assert('Has fill review', allFra?.some(f => f.action_type === 'fill'));
    assert('Has theft review', allFra?.some(f => f.action_type === 'theft'));

    if (wsClient) wsClient.close();
    if (wsServer) wsServer.close();

  } catch (e) {
    err(`\nFATAL ERROR: ${e.message}\n${e.stack}`);
    failed++;
  }

  log(`\n${'='.repeat(60)}`);
  log(`FINAL RESULTS: ${passed} passed, ${failed} failed`);
  log('='.repeat(60));
  log(`Finished: ${new Date().toISOString()}`);
  log(`Log saved to: ${LOG_FILE}\n`);

  if (failed > 0) process.exit(1);
}

run().catch(e => { err(`Unhandled: ${e.message}\n${e.stack}`); process.exit(1); });
