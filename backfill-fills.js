require('dotenv').config();

const { Pool } = require('pg');
const { createClient } = require('@supabase/supabase-js');

const DRY_RUN = !process.argv.includes('--execute');
const DATE_FROM = '2026-07-01';
const DATE_TO = '2026-08-31';

process.env.PGHOST = 'localhost';
process.env.PGPORT = process.env.PGPORT_HOST || '5433';

const pool = new Pool({
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'fuel_table',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD
});

const firstSupabase = (process.env.SUPABASE_URL && (process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY))
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY)
  : null;

const waterfordSupabase = (process.env.WATERFORD_SUPABASE_URL && (process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY))
  ? createClient(process.env.WATERFORD_SUPABASE_URL, process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY)
  : null;

const VEHICLES = {
  'KL33HWGP': 'WATE-0001', 'FV70YVGP': 'WATE-0001', 'FM23CWGP': 'WATE-0001',
  'CP09PGGP': 'WATE-0001', 'YWX933GP': 'WATE-0001', 'JW59WDGP': 'WATE-0001',
  'LR78ZBGP': 'WATE-0001', 'MF56SKGP': 'WATE-0001', 'LD08STGP': 'WATE-0001',
  'LR78XJGP': 'WATE-0001', 'KP48MNGP': 'WATE-0001', 'KP48NFGP': 'WATE-0001',
  'LC62WSGP': 'WATE-0001', 'MD69KRGP': 'WATE-0001', 'MD69KJGP': 'WATE-0001',
  'MG45YNGP': 'WATE-0001', 'LV75GCGP': 'WATE-0001', 'HW65MMGP': 'WATE-0001',
  'JW59WJGP': 'WATE-0001', 'LD08SSGP': 'WATE-0001', 'JP29YVGP': 'WATE-0001',
  'JW59VYGP': 'WATE-0001', 'LF60RGGP': 'WATE-0001', 'JP29YTGP': 'WATE-0001',
  'LV75FKGP': 'WATE-0001', 'JM39BBGP': 'WATE-0001', 'KP48NCGP': 'WATE-0001',
  'KP48MWGP': 'WATE-0001', 'KZ89MRGP': 'WATE-0001', 'LD08SLGP': 'WATE-0001',
  'LR78YGGP': 'WATE-0001', 'KN41XSGP': 'WATE-0001', 'KC31RGGP': 'WATE-0001',
  'LD08SWGP': 'WATE-0001', 'LS34PRGP': 'WATE-0001', 'LR81ZZGP': 'WATE-0001',
  'LS34PMGP': 'WATE-0001', 'KD57TSGP': 'WATE-0001', 'LS34PGGP': 'WATE-0001',
  'FV26GTGP': 'WATE-0001', 'FW28SMGP': 'WATE-0001', 'JP88KFGP': 'WATE-0001',
  'MK84KSGP': 'WATE-0001', 'KC93JKGP': 'WATE-0001', 'LF60WPGP': 'WATE-0001',
  'LS38WYGP': 'WATE-0001', 'LD13PHGP': 'WATE-0001'
};

const combinedFuel = (row) => (row.fuel_probe_1_volume_in_tank || 0) + (row.fuel_probe_2_volume_in_tank || 0);

const isFillStatus = (status) => {
  const s = (status || '').toUpperCase();
  return s.includes('FUEL FILL');
};

const isTheftStatus = (status) => {
  const s = (status || '').toUpperCase();
  return s.includes('FUEL THEFT');
};

const hasFuel = (row) => (row.fuel_probe_1_volume_in_tank > 0 || row.fuel_probe_2_volume_in_tank > 0);

async function loadVehicleHistory() {
  console.log(`[backfill] Loading vehicle history from ${DATE_FROM} to ${DATE_TO}...`);
  const { rows } = await pool.query(`
    SELECT id, plate, loc_time, status,
      fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank,
      latitude, longitude, created_at
    FROM vehicle_history
    WHERE created_at >= $1 AND created_at <= $2
      AND (fuel_probe_1_volume_in_tank > 0 OR fuel_probe_2_volume_in_tank > 0)
    ORDER BY plate, created_at ASC
  `, [DATE_FROM, DATE_TO + ' 23:59:59']);
  console.log(`[backfill] Loaded ${rows.length} rows`);
  return rows;
}

function detectEvents(rows) {
  const fills = [];
  const thefts = [];

  const byPlate = {};
  for (const row of rows) {
    if (!byPlate[row.plate]) byPlate[row.plate] = [];
    byPlate[row.plate].push(row);
  }

  for (const [plate, readings] of Object.entries(byPlate)) {
    let i = 0;
    while (i < readings.length) {
      const row = readings[i];

      if (isFillStatus(row.status)) {
        const event = extractEvent(readings, i, 'fill');
        if (event) fills.push(event);
        if (event) i = event.endIndex + 1; else i++;
        continue;
      }

      if (isTheftStatus(row.status)) {
        const event = extractEvent(readings, i, 'theft');
        if (event) thefts.push(event);
        if (event) i = event.endIndex + 1; else i++;
        continue;
      }

      i++;
    }
  }

  return { fills, thefts };
}

function extractEvent(readings, fillIndex, type) {
  const fillRow = readings[fillIndex];
  const fillTime = fillRow.created_at;

  let baselineIndex = -1;
  for (let j = fillIndex - 1; j >= 0; j--) {
    const fuel = combinedFuel(readings[j]);
    if (fuel > 0) {
      baselineIndex = j;
      break;
    }
  }

  if (baselineIndex === -1) return null;

  const baselineFuel = combinedFuel(readings[baselineIndex]);
  if (baselineFuel <= 0) return null;

  let maxFuel = baselineFuel;
  let maxIndex = baselineIndex;
  let minFuel = baselineFuel;
  let minIndex = baselineIndex;

  let endIndex = fillIndex;
  let lastClimbing = baselineFuel;
  let lastDropping = baselineFuel;

  for (let k = fillIndex; k < readings.length; k++) {
    const currentFuel = combinedFuel(readings[k]);
    if (currentFuel <= 0) continue;

    if (type === 'fill') {
      if (currentFuel > lastClimbing) {
        maxFuel = currentFuel;
        maxIndex = k;
        lastClimbing = currentFuel;
        endIndex = k;
      } else if (currentFuel < lastClimbing * 0.95) {
        break;
      }
    } else {
      if (currentFuel < lastDropping) {
        minFuel = currentFuel;
        minIndex = k;
        lastDropping = currentFuel;
        endIndex = k;
      } else if (currentFuel > lastDropping * 1.05) {
        break;
      }
    }
  }

  const amount = type === 'fill' ? maxFuel - baselineFuel : baselineFuel - minFuel;
  if (amount <= 0) return null;

  const baselineRow = readings[baselineIndex];
  const resultRow = type === 'fill' ? readings[maxIndex] : readings[minIndex];

  return {
    plate: fillRow.plate,
    type,
    amount,
    baselineFuel,
    baselineTime: baselineRow.loc_time || baselineRow.created_at,
    resultFuel: type === 'fill' ? maxFuel : minFuel,
    resultTime: resultRow.loc_time || resultRow.created_at,
    fillTime: fillRow.loc_time || fillRow.created_at,
    lat: fillRow.latitude,
    lon: fillRow.longitude,
    endIndex,
    baselineP1: baselineRow.fuel_probe_1_volume_in_tank || 0,
    baselineP2: baselineRow.fuel_probe_2_volume_in_tank || 0,
    resultP1: type === 'fill' ? (readings[maxIndex].fuel_probe_1_volume_in_tank || 0) : (readings[minIndex].fuel_probe_1_volume_in_tank || 0),
    resultP2: type === 'fill' ? (readings[maxIndex].fuel_probe_2_volume_in_tank || 0) : (readings[minIndex].fuel_probe_2_volume_in_tank || 0),
  };
}

async function checkDuplicate(plate, sessionDate, sessionStatus) {
  if (!firstSupabase) return false;
  const { data } = await firstSupabase
    .from('energy_rite_operating_sessions')
    .select('id')
    .eq('branch', plate)
    .eq('session_date', sessionDate)
    .eq('session_status', sessionStatus)
    .limit(1);
  return data && data.length > 0;
}

async function insertToSupabase(event) {
  const sessionDate = event.fillTime instanceof Date
    ? event.fillTime.toISOString().split('T')[0]
    : String(event.fillTime).split('T')[0];

  const isFill = event.type === 'fill';
  const sessionStatus = isFill ? 'FUEL_FILL_COMPLETED' : 'FUEL_THEFT_COMPLETED';
  const costCode = VEHICLES[event.plate] || 'WATE-0001';

  const session = {
    branch: event.plate,
    company: 'WATERFORD',
    cost_code: costCode,
    session_date: sessionDate,
    session_start_time: event.baselineTime instanceof Date ? event.baselineTime.toISOString() : event.baselineTime,
    session_end_time: event.resultTime instanceof Date ? event.resultTime.toISOString() : event.resultTime,
    operating_hours: 0,
    opening_fuel: event.baselineFuel,
    opening_fuel_probe_1: event.baselineP1,
    opening_fuel_probe_2: event.baselineP2,
    opening_percentage: 0,
    opening_percentage_probe_1: 0,
    opening_percentage_probe_2: 0,
    closing_fuel: event.resultFuel,
    closing_fuel_probe_1: event.resultP1,
    closing_fuel_probe_2: event.resultP2,
    closing_percentage: 0,
    closing_percentage_probe_1: 0,
    closing_percentage_probe_2: 0,
    session_status: sessionStatus,
    notes: `Backfill: ${isFill ? 'Fill' : 'Theft'} detected. Last reading at ${event.baselineTime}, ${sessionStatus} at ${event.fillTime}. Baseline: ${event.baselineFuel}L, ${isFill ? 'Max' : 'Min'}: ${event.resultFuel}L, ${isFill ? 'Filled' : 'Lost'}: ${event.amount.toFixed(1)}L`,
  };

  if (isFill) {
    session.total_fill = event.amount;
    session.total_usage = 0;
    session.fill_events = 1;
    session.fill_amount_during_session = event.amount;
  } else {
    session.total_theft = event.amount;
    session.total_usage = 0;
  }

  const dup = await checkDuplicate(event.plate, sessionDate, sessionStatus);
  if (dup) {
    console.log(`  [skip] Duplicate: ${event.plate} ${sessionDate} ${sessionStatus}`);
    return;
  }

  if (DRY_RUN) return;

  if (firstSupabase) {
    const { error } = await firstSupabase.from('energy_rite_operating_sessions').insert(session);
    if (error) console.error(`  [error] First Supabase insert: ${error.message}`);
  }

  if (waterfordSupabase) {
    const { error } = await waterfordSupabase.from('energy_rite_operating_sessions').insert(session);
    if (error) console.error(`  [error] WATERFORD Supabase insert: ${error.message}`);
  }

  if (waterfordSupabase && isFill) {
    const { error } = await waterfordSupabase.from('fuel_review_actions').upsert({
      vehicle_reg: event.plate,
      review_date: sessionDate,
      action_type: 'fill',
      type: 'fill',
      probe_value: `${event.amount.toFixed(1)}L (${event.baselineFuel.toFixed(1)}L -> ${event.resultFuel.toFixed(1)}L)`,
      notes: `Backfill: last reading at ${event.baselineTime} | fill detected at ${event.fillTime}`,
    }, { onConflict: 'vehicle_reg,review_date,action_type' });
    if (error) console.error(`  [error] WATERFORD fuel_review_actions fill: ${error.message}`);
  }

  if (waterfordSupabase && !isFill) {
    const { error } = await waterfordSupabase.from('fuel_review_actions').upsert({
      vehicle_reg: event.plate,
      review_date: sessionDate,
      action_type: 'theft',
      type: 'theft',
      probe_value: `${event.amount.toFixed(1)}L (${event.baselineFuel.toFixed(1)}L -> ${event.resultFuel.toFixed(1)}L)`,
      notes: `Backfill: last reading at ${event.baselineTime} | theft detected at ${event.fillTime}`,
    }, { onConflict: 'vehicle_reg,review_date,action_type' });
    if (error) console.error(`  [error] WATERFORD fuel_review_actions theft: ${error.message}`);
  }
}

async function run() {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`BACKFILL ${DRY_RUN ? 'DRY RUN' : 'EXECUTE'} — ${DATE_FROM} to ${DATE_TO}`);
  console.log('='.repeat(70));
  console.log(`First Supabase: ${firstSupabase ? 'connected' : 'NOT configured'}`);
  console.log(`WATERFORD Supabase: ${waterfordSupabase ? 'connected' : 'NOT configured'}`);

  const rows = await loadVehicleHistory();
  const { fills, thefts } = detectEvents(rows);

  console.log(`\nDetected: ${fills.length} fills, ${thefts.length} thefts\n`);

  console.log('-'.repeat(70));
  console.log('FILLS');
  console.log('-'.repeat(70));
  console.log('Plate      | Fill Time         | Engine Off Time   | Baseline | Max     | Filled');
  console.log('-'.repeat(70));

  let totalFilled = 0;
  for (const f of fills) {
    const ft = String(f.fillTime).substring(0, 16).replace('T', ' ');
    const bt = String(f.baselineTime).substring(0, 16).replace('T', ' ');
    console.log(
      `${f.plate.padEnd(10)} | ${ft.padEnd(17)} | ${bt.padEnd(17)} | ${(f.baselineFuel.toFixed(1) + 'L').padStart(8)} | ${(f.resultFuel.toFixed(1) + 'L').padStart(7)} | ${(f.amount.toFixed(1) + 'L').padStart(7)}`
    );
    totalFilled += f.amount;
    await insertToSupabase(f);
  }

  console.log('-'.repeat(70));
  console.log('THEFTS');
  console.log('-'.repeat(70));
  console.log('Plate      | Theft Time        | Engine Off Time   | Baseline | Min     | Lost');
  console.log('-'.repeat(70));

  let totalLost = 0;
  for (const t of thefts) {
    const ft = String(t.fillTime).substring(0, 16).replace('T', ' ');
    const bt = String(t.baselineTime).substring(0, 16).replace('T', ' ');
    console.log(
      `${t.plate.padEnd(10)} | ${ft.padEnd(17)} | ${bt.padEnd(17)} | ${(t.baselineFuel.toFixed(1) + 'L').padStart(8)} | ${(t.resultFuel.toFixed(1) + 'L').padStart(7)} | ${(t.amount.toFixed(1) + 'L').padStart(7)}`
    );
    totalLost += t.amount;
    await insertToSupabase(t);
  }

  console.log('\n' + '='.repeat(70));
  console.log('SUMMARY');
  console.log('='.repeat(70));
  console.log(`Fills:  ${fills.length} events, ${totalFilled.toFixed(1)}L total fuel added`);
  console.log(`Thefts: ${thefts.length} events, ${totalLost.toFixed(1)}L total fuel lost`);
  console.log(`Mode:   ${DRY_RUN ? 'DRY RUN (no inserts)' : 'EXECUTED (inserted to Supabase)'}`);
  console.log('='.repeat(70) + '\n');

  await pool.end();
}

run().catch(e => { console.error('[backfill] Fatal:', e.message); process.exit(1); });
