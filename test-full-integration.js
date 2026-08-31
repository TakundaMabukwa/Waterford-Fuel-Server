require('dotenv').config();
const fs = require('fs');
const { createClient } = require('@supabase/supabase-js');
const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;

const LOG_FILE = process.env.LOG_FILE || require('path').join(__dirname, 'integration-test.log');
const log = (msg) => { console.log(msg); fs.appendFileSync(LOG_FILE, msg + '\n'); };
const err = (msg) => { console.error(msg); fs.appendFileSync(LOG_FILE, 'ERROR: ' + msg + '\n'); };

fs.writeFileSync(LOG_FILE, `=== Waterford Integration Test ===\nStarted: ${new Date().toISOString()}\n\n`);

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
const waterfordUrl = process.env.WATERFORD_SUPABASE_URL;
const waterfordKey = process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY;

log(`SUPABASE_URL: ${supabaseUrl || 'MISSING'}`);
log(`WATERFORD_SUPABASE_URL: ${waterfordUrl || 'MISSING'}`);

if (!supabaseUrl || !supabaseKey) { err('Missing SUPABASE_URL or key'); process.exit(1); }
if (!waterfordUrl || !waterfordKey) { err('Missing WATERFORD env vars'); process.exit(1); }

const supabase = createClient(supabaseUrl, supabaseKey);
const waterford = createClient(waterfordUrl, waterfordKey);

const PLATE = 'TESTZone99';
let passed = 0;
let failed = 0;

function assert(label, condition, detail) {
  if (condition) { log(`  ✓ ${label}`); passed++; }
  else { log(`  ✗ ${label} ${detail ? '— ' + detail : ''}`); failed++; }
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
  log(`\n${'='.repeat(50)}`);
  log('TEST 1: Waterford Supabase — Fuel Stops');
  log('='.repeat(50));
  try {
    const { data: stops, error: stopsErr } = await waterford.from('fuel_stops').select('id, name, coordinates');
    assert('Fuel stops readable', !stopsErr && stops && stops.length > 0, stopsErr ? stopsErr.message : `count=${stops?.length}`);
    const waterfordStop = stops?.find(s => s.name === 'Waterford Carriers');
    assert('Waterford Carriers exists', waterfordStop !== undefined, waterfordStop ? `id=${waterfordStop.id}` : 'NOT FOUND');

    let polygon = waterfordStop?.coordinates;
    if (typeof polygon === 'string') { try { polygon = JSON.parse(polygon); } catch { polygon = null; } }
    if (!Array.isArray(polygon) || polygon.length < 3) { err(`Invalid polygon: ${JSON.stringify(polygon)}`); return; }

    const closedRing = [...polygon, polygon[0]];
    const turfPoly = { type: 'Feature', geometry: { type: 'Polygon', coordinates: [closedRing] }, properties: {} };

    const inside = booleanPointInPolygon([28.1469, -26.2243], turfPoly);
    assert('Center of Waterford Carriers INSIDE polygon', inside);

    const outside = booleanPointInPolygon([28.0, -29.0], turfPoly);
    assert('Far point OUTSIDE polygon', !outside);
  } catch (e) { err(`TEST 1 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 2: Session Open (ENGINE ON) → energy_rite_operating_sessions');
  log('='.repeat(50));
  let openId = null;
  try {
    const startTime = new Date('2025-01-15T08:00:00Z');
    const { data, error } = await supabase.from('energy_rite_operating_sessions').insert({
      branch: PLATE, company: 'WATERFORD', cost_code: 'WATE-0001',
      session_date: '2025-01-15', session_start_time: startTime.toISOString(),
      opening_fuel: 525.8, opening_fuel_probe_1: 240.7, opening_fuel_probe_2: 285.1,
      opening_percentage: 0, opening_percentage_probe_1: 65.2, opening_percentage_probe_2: 72.8,
      session_status: 'ONGOING', notes: 'Test: Engine started. Opening: 525.8L'
    }).select('id');
    assert('ONGOING session inserted', !error && data && data.length > 0, error ? error.message : '');
    openId = data?.[0]?.id;

    const { data: v } = await supabase.from('energy_rite_operating_sessions').select('id, session_status, opening_fuel').eq('id', openId).single();
    assert('Session queryable', v && v.session_status === 'ONGOING');
    assert('opening_fuel = 525.8', v && v.opening_fuel === 525.8, `got ${v?.opening_fuel}`);
  } catch (e) { err(`TEST 2 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 3: Session Close (ENGINE OFF) → energy_rite_operating_sessions');
  log('='.repeat(50));
  try {
    const endTime = new Date('2025-01-15T16:00:00Z');
    const { error } = await supabase.from('energy_rite_operating_sessions').update({
      session_end_time: endTime.toISOString(), operating_hours: 8.0,
      closing_fuel: 480.2, closing_fuel_probe_1: 220.1, closing_fuel_probe_2: 260.1,
      total_usage: 45.6, session_status: 'COMPLETED',
      notes: 'Test: Engine off. Closing: 480.2L. Usage: 45.6L'
    }).eq('id', openId);
    assert('Session updated to COMPLETED', !error, error ? error.message : '');

    const { data: c } = await supabase.from('energy_rite_operating_sessions').select('*').eq('id', openId).single();
    assert('closing_fuel = 480.2', c && c.closing_fuel === 480.2, `got ${c?.closing_fuel}`);
    assert('operating_hours = 8.0', c && c.operating_hours === 8.0, `got ${c?.operating_hours}`);
    assert('session_status = COMPLETED', c && c.session_status === 'COMPLETED');
    assert('total_usage = 45.6', c && c.total_usage === 45.6, `got ${c?.total_usage}`);
  } catch (e) { err(`TEST 3 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 4: Fill Detection (geozone) → energy_rite_operating_sessions');
  log('='.repeat(50));
  let fillId = null;
  try {
    const fillTime = new Date('2025-01-15T12:00:00Z');
    const { data, error } = await supabase.from('energy_rite_operating_sessions').insert({
      branch: PLATE, company: 'WATERFORD', cost_code: 'WATE-0001',
      session_date: '2025-01-15', session_start_time: fillTime.toISOString(), session_end_time: fillTime.toISOString(),
      opening_fuel: 400.0, opening_fuel_probe_1: 180.0, opening_fuel_probe_2: 220.0,
      closing_fuel: 525.8, closing_fuel_probe_1: 240.7, closing_fuel_probe_2: 285.1,
      total_fill: 125.8, fill_events: 1, fill_amount_during_session: 125.8,
      session_status: 'FUEL_FILL_COMPLETED',
      notes: 'Test: Geozone fill. 400L -> 525.8L = +125.8L | zone: Waterford Carriers | detection: geozone'
    }).select('id');
    assert('FILL_COMPLETED inserted', !error && data && data.length > 0, error ? error.message : '');
    fillId = data?.[0]?.id;

    const { data: fv } = await supabase.from('energy_rite_operating_sessions').select('*').eq('id', fillId).single();
    assert('total_fill = 125.8', fv && fv.total_fill === 125.8, `got ${fv?.total_fill}`);
    assert('session_status = FUEL_FILL_COMPLETED', fv && fv.session_status === 'FUEL_FILL_COMPLETED');
    assert('fill_events = 1', fv && fv.fill_events === 1, `got ${fv?.fill_events}`);
    assert('notes contain zone', fv?.notes?.includes('Waterford Carriers'));
  } catch (e) { err(`TEST 4 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 5: Theft Detection (status) → energy_rite_operating_sessions');
  log('='.repeat(50));
  let theftId = null;
  try {
    const theftTime = new Date('2025-01-15T14:00:00Z');
    const { data, error } = await supabase.from('energy_rite_operating_sessions').insert({
      branch: PLATE, company: 'WATERFORD', cost_code: 'WATE-0001',
      session_date: '2025-01-15', session_start_time: theftTime.toISOString(), session_end_time: theftTime.toISOString(),
      opening_fuel: 600.0, opening_fuel_probe_1: 300.0, opening_fuel_probe_2: 300.0,
      closing_fuel: 550.0, closing_fuel_probe_1: 275.0, closing_fuel_probe_2: 275.0,
      total_theft: 50.0, session_status: 'FUEL_THEFT_COMPLETED',
      notes: 'Test: Theft. 600L -> 550L = -50L | detection: status trigger'
    }).select('id');
    assert('THEFT_COMPLETED inserted', !error && data && data.length > 0, error ? error.message : '');
    theftId = data?.[0]?.id;

    const { data: tv } = await supabase.from('energy_rite_operating_sessions').select('*').eq('id', theftId).single();
    assert('total_theft = 50', tv && tv.total_theft === 50.0, `got ${tv?.total_theft}`);
    assert('session_status = FUEL_THEFT_COMPLETED', tv && tv.session_status === 'FUEL_THEFT_COMPLETED');
  } catch (e) { err(`TEST 5 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 6: fuel_review_actions — Fill');
  log('='.repeat(50));
  try {
    const { error } = await waterford.from('fuel_review_actions').upsert({
      vehicle_reg: PLATE, review_date: '2025-01-15', action_type: 'fill',
      probe_value: '125.8L (400.0L -> 525.8L)',
      notes: 'loc_time: 2025-01-15T12:00:00Z | zone: Waterford Carriers | detection: geozone'
    }, { onConflict: 'vehicle_reg,review_date,action_type' });
    assert('Fill upserted', !error, error ? error.message : '');

    const { data, error: qErr } = await waterford.from('fuel_review_actions').select('*').eq('vehicle_reg', PLATE).eq('action_type', 'fill').limit(1);
    assert('Fill queryable', !qErr && data && data.length > 0, qErr ? qErr.message : '');
    assert('probe_value correct', data?.[0]?.probe_value === '125.8L (400.0L -> 525.8L)', `got "${data?.[0]?.probe_value}"`);
  } catch (e) { err(`TEST 6 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 7: fuel_review_actions — Theft');
  log('='.repeat(50));
  try {
    const { error } = await waterford.from('fuel_review_actions').upsert({
      vehicle_reg: PLATE, review_date: '2025-01-16', action_type: 'theft',
      probe_value: '50.0L (600.0L -> 550.0L)',
      notes: 'loc_time: 2025-01-15T14:00:00Z | detection: status trigger'
    }, { onConflict: 'vehicle_reg,review_date,action_type' });
    assert('Theft upserted', !error, error ? error.message : '');

    const { data, error: qErr } = await waterford.from('fuel_review_actions').select('*').eq('vehicle_reg', PLATE).eq('action_type', 'theft').limit(1);
    assert('Theft queryable', !qErr && data && data.length > 0, qErr ? qErr.message : '');
    assert('probe_value correct', data?.[0]?.probe_value === '50.0L (600.0L -> 550.0L)');
  } catch (e) { err(`TEST 7 error: ${e.message}`); }

  log(`\n${'='.repeat(50)}`);
  log('TEST 8: Combined Verification');
  log('='.repeat(50));
  try {
    const { data: sessions } = await supabase.from('energy_rite_operating_sessions').select('session_status, total_fill, total_theft, total_usage, closing_fuel, opening_fuel').eq('branch', PLATE);
    const sc = {};
    (sessions || []).forEach(s => { sc[s.session_status] = (sc[s.session_status] || 0) + 1; });
    log(`  Sessions found: ${JSON.stringify(sc)}`);
    assert('Has ONGOING → COMPLETED session', (sc['COMPLETED'] || 0) >= 1);
    assert('Has FUEL_FILL_COMPLETED', (sc['FUEL_FILL_COMPLETED'] || 0) >= 1);
    assert('Has FUEL_THEFT_COMPLETED', (sc['FUEL_THEFT_COMPLETED'] || 0) >= 1);
    assert('Total sessions >= 3', (sessions?.length || 0) >= 3, `got ${sessions?.length}`);

    const { data: fra } = await waterford.from('fuel_review_actions').select('action_type, probe_value').eq('vehicle_reg', PLATE);
    log(`  Review actions found: ${JSON.stringify(fra)}`);
    assert('Has fill review', fra?.some(f => f.action_type === 'fill'));
    assert('Has theft review', fra?.some(f => f.action_type === 'theft'));
  } catch (e) { err(`TEST 8 error: ${e.message}`); }

  await cleanup();

  log(`\n${'='.repeat(50)}`);
  log(`FINAL RESULTS: ${passed} passed, ${failed} failed`);
  log('='.repeat(50));
  log(`Finished: ${new Date().toISOString()}`);
  log(`Log saved to: ${LOG_FILE}\n`);

  if (failed > 0) process.exit(1);
}

run().catch(e => { err(`Unhandled: ${e.message}\n${e.stack}`); process.exit(1); });
