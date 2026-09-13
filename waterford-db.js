const { Pool } = require('pg');
const { createClient } = require('@supabase/supabase-js');

const supabase = process.env.SUPABASE_URL && (process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY)
  : null;

if (!supabase) {
  console.error('[db] *** CRITICAL: Supabase credentials not set - fill/theft sessions will NOT be recorded ***');
} else {
  console.log('[db] Supabase client ready (URL: ' + (process.env.SUPABASE_URL || 'missing') + ')');
}

let pool = null;
let initialized = false;
const vehicleCache = new Set();

const VEHICLES = [
  { plate: 'KL33HWGP', cost_code: 'WATE-0001' },
  { plate: 'FV70YVGP', cost_code: 'WATE-0001' },
  { plate: 'FM23CWGP', cost_code: 'WATE-0001' },
  { plate: 'CP09PGGP', cost_code: 'WATE-0001' },
  { plate: 'YWX933GP', cost_code: 'WATE-0001' },
  { plate: 'JW59WDGP', cost_code: 'WATE-0001' },
  { plate: 'LR78ZBGP', cost_code: 'WATE-0001' },
  { plate: 'MF56SKGP', cost_code: 'WATE-0001' },
  { plate: 'LD08STGP', cost_code: 'WATE-0001' },
  { plate: 'LR78XJGP', cost_code: 'WATE-0001' },
  { plate: 'KP48MNGP', cost_code: 'WATE-0001' },
  { plate: 'KP48NFGP', cost_code: 'WATE-0001' },
  { plate: 'LC62WSGP', cost_code: 'WATE-0001' },
  { plate: 'MD69KRGP', cost_code: 'WATE-0001' },
  { plate: 'MD69KJGP', cost_code: 'WATE-0001' },
  { plate: 'MG45YNGP', cost_code: 'WATE-0001' },
  { plate: 'LV75GCGP', cost_code: 'WATE-0001' },
  { plate: 'HW65MMGP', cost_code: 'WATE-0001' },
  { plate: 'JW59WJGP', cost_code: 'WATE-0001' },
  { plate: 'LD08SSGP', cost_code: 'WATE-0001' },
  { plate: 'JP29YVGP', cost_code: 'WATE-0001' },
  { plate: 'JW59VYGP', cost_code: 'WATE-0001' },
  { plate: 'LF60RGGP', cost_code: 'WATE-0001' },
  { plate: 'JP29YTGP', cost_code: 'WATE-0001' },
  { plate: 'LV75FKGP', cost_code: 'WATE-0001' },
  { plate: 'JM39BBGP', cost_code: 'WATE-0001' },
  { plate: 'KP48NCGP', cost_code: 'WATE-0001' },
  { plate: 'KP48MWGP', cost_code: 'WATE-0001' },
  { plate: 'KZ89MRGP', cost_code: 'WATE-0001' },
  { plate: 'LD08SLGP', cost_code: 'WATE-0001' },
  { plate: 'LR78YGGP', cost_code: 'WATE-0001' },
  { plate: 'KN41XSGP', cost_code: 'WATE-0001' },
  { plate: 'KC31RGGP', cost_code: 'WATE-0001' },
  { plate: 'LD08SWGP', cost_code: 'WATE-0001' },
  { plate: 'LS34PRGP', cost_code: 'WATE-0001' },
  { plate: 'LR81ZZGP', cost_code: 'WATE-0001' },
  { plate: 'LS34PMGP', cost_code: 'WATE-0001' },
  { plate: 'KD57TSGP', cost_code: 'WATE-0001' },
  { plate: 'LS34PGGP', cost_code: 'WATE-0001' },
  { plate: 'FV26GTGP', cost_code: 'WATE-0001' },
  { plate: 'FW28SMGP', cost_code: 'WATE-0001' },
  { plate: 'JP88KFGP', cost_code: 'WATE-0001' },
  { plate: 'MK84KSGP', cost_code: 'WATE-0001' },
  { plate: 'KC93JKGP', cost_code: 'WATE-0001' },
  { plate: 'LF60WPGP', cost_code: 'WATE-0001' },
  { plate: 'LS38WYGP', cost_code: 'WATE-0001' },
  { plate: 'LD13PHGP', cost_code: 'WATE-0001' }
];

const COST_CODES = Object.fromEntries(VEHICLES.map(v => [v.plate, v.cost_code]));

const createPool = () => {
  const p = new Pool({
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432', 10),
    database: process.env.PGDATABASE || 'fuel_table',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD
  });
  p.on('error', (err) => console.error('[db] Pool error:', err.message));
  return p;
};

const getPool = () => {
  if (!pool) pool = createPool();
  return pool;
};

const query = (text, values = []) => getPool().query(text, values);

const waitForDatabase = async (retries = 30, delayMs = 2000) => {
  for (let i = 1; i <= retries; i++) {
    try {
      await query('SELECT 1');
      console.log('[db] PostgreSQL connected');
      return;
    } catch {
      console.log(`[db] Waiting for PostgreSQL... ${i}/${retries}`);
      if (i === retries) throw new Error('PostgreSQL connection timeout');
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
};

const createTables = async () => {
  await query(`
    CREATE TABLE IF NOT EXISTS vehicles (
      id SERIAL PRIMARY KEY,
      plate VARCHAR(50) UNIQUE NOT NULL,
      cost_code VARCHAR(50),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS vehicle_latest (
      plate VARCHAR(50) PRIMARY KEY,
      cost_code VARCHAR(50),
      speed DOUBLE PRECISION,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      loc_time TEXT,
      mileage BIGINT,
      pocsagstr TEXT,
      status TEXT,
      message_type INTEGER,
      fuel_probe_1_level DOUBLE PRECISION,
      fuel_probe_1_volume_in_tank DOUBLE PRECISION,
      fuel_probe_1_temperature DOUBLE PRECISION,
      fuel_probe_1_level_percentage DOUBLE PRECISION,
      fuel_probe_2_level DOUBLE PRECISION,
      fuel_probe_2_volume_in_tank DOUBLE PRECISION,
      fuel_probe_2_temperature DOUBLE PRECISION,
      fuel_probe_2_level_percentage DOUBLE PRECISION,
      item_installed TEXT,
      geozone TEXT,
      driver_name TEXT,
      raw_fuel_data TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS vehicle_history (
      id BIGSERIAL PRIMARY KEY,
      plate VARCHAR(50) NOT NULL,
      cost_code VARCHAR(50),
      speed DOUBLE PRECISION,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      loc_time TEXT,
      mileage BIGINT,
      pocsagstr TEXT,
      status TEXT,
      message_type INTEGER,
      fuel_probe_1_level DOUBLE PRECISION,
      fuel_probe_1_volume_in_tank DOUBLE PRECISION,
      fuel_probe_1_temperature DOUBLE PRECISION,
      fuel_probe_1_level_percentage DOUBLE PRECISION,
      fuel_probe_2_level DOUBLE PRECISION,
      fuel_probe_2_volume_in_tank DOUBLE PRECISION,
      fuel_probe_2_temperature DOUBLE PRECISION,
      fuel_probe_2_level_percentage DOUBLE PRECISION,
      item_installed TEXT,
      geozone TEXT,
      driver_name TEXT,
      raw_fuel_data TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_vehicle_history_plate_time
      ON vehicle_history (plate, created_at DESC)
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS energy_rite_operating_sessions (
      id BIGSERIAL PRIMARY KEY,
      branch VARCHAR(50),
      company VARCHAR(100),
      cost_code VARCHAR(50),
      session_date DATE,
      session_start_time TIMESTAMPTZ,
      session_end_time TIMESTAMPTZ,
      operating_hours DOUBLE PRECISION DEFAULT 0,
      opening_fuel DOUBLE PRECISION,
      opening_percentage DOUBLE PRECISION,
      opening_fuel_probe_1 DOUBLE PRECISION,
      opening_fuel_probe_2 DOUBLE PRECISION,
      opening_percentage_probe_1 DOUBLE PRECISION,
      opening_percentage_probe_2 DOUBLE PRECISION,
      closing_fuel DOUBLE PRECISION,
      closing_percentage DOUBLE PRECISION,
      closing_fuel_probe_1 DOUBLE PRECISION,
      closing_fuel_probe_2 DOUBLE PRECISION,
      closing_percentage_probe_1 DOUBLE PRECISION,
      closing_percentage_probe_2 DOUBLE PRECISION,
      total_fill DOUBLE PRECISION DEFAULT 0,
      total_theft DOUBLE PRECISION DEFAULT 0,
      total_usage DOUBLE PRECISION DEFAULT 0,
      session_status VARCHAR(50) DEFAULT 'ONGOING',
      notes TEXT,
      fill_events INTEGER DEFAULT 0,
      fill_amount_during_session DOUBLE PRECISION DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS fuel_stops (
      id BIGINT PRIMARY KEY,
      name TEXT,
      coordinates JSONB,
      geozone_name TEXT,
      geozone_coordinates JSONB,
      location_coordinates JSONB,
      radius NUMERIC(10,2) DEFAULT 100,
      type TEXT DEFAULT 'warehouse',
      address TEXT,
      city TEXT,
      state TEXT,
      country TEXT,
      contact_person TEXT,
      contact_phone TEXT,
      operating_hours TEXT,
      capacity TEXT,
      notes TEXT,
      prescribed_value NUMERIC,
      fuel_type TEXT,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS fuel_type TEXT
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS geozone_events (
      id BIGSERIAL PRIMARY KEY,
      plate VARCHAR(50) NOT NULL,
      fuel_stop_id BIGINT REFERENCES fuel_stops(id),
      geozone_name TEXT,
      event_type VARCHAR(20) NOT NULL,
      loc_time TEXT,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      fuel_before DOUBLE PRECISION,
      fuel_after DOUBLE PRECISION,
      fill_amount DOUBLE PRECISION,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_geozone_events_plate
      ON geozone_events (plate, created_at DESC)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_geozone_events_type
      ON geozone_events (event_type)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_fuel_stops_name
      ON fuel_stops (name)
  `);

  console.log('[db] Tables ready');
};

const seedVehicles = async () => {
  for (const { plate, cost_code } of VEHICLES) {
    await query(
      'INSERT INTO vehicles (plate, cost_code) VALUES ($1, $2) ON CONFLICT (plate) DO NOTHING',
      [plate, cost_code]
    );
  }
  console.log(`[db] Seeded ${VEHICLES.length} vehicles`);
};

const loadVehicleCache = async () => {
  const { rows } = await query('SELECT plate FROM vehicles');
  rows.forEach(r => vehicleCache.add(r.plate));
  console.log(`[db] Cache loaded: ${vehicleCache.size} plates`);
};

const isKnownVehicle = (plate) => vehicleCache.has(plate);
const getCostCode = (plate) => COST_CODES[plate] || null;

const HISTORY_COLUMNS = [
  'plate', 'cost_code', 'speed', 'latitude', 'longitude', 'loc_time', 'mileage',
  'pocsagstr', 'status', 'message_type',
  'fuel_probe_1_level', 'fuel_probe_1_volume_in_tank',
  'fuel_probe_1_temperature', 'fuel_probe_1_level_percentage',
  'fuel_probe_2_level', 'fuel_probe_2_volume_in_tank',
  'fuel_probe_2_temperature', 'fuel_probe_2_level_percentage',
  'item_installed', 'geozone', 'driver_name', 'raw_fuel_data'
];

const placeholders = (cols) => cols.map((_, i) => `$${i + 1}`).join(', ');

const insertHistory = async (row) => {
  const sql = `
    INSERT INTO vehicle_history (${HISTORY_COLUMNS.join(', ')})
    VALUES (${placeholders(HISTORY_COLUMNS)})
  `;
  await query(sql, HISTORY_COLUMNS.map(c => row[c]));
};

const upsertLatest = async (row) => {
  const updates = HISTORY_COLUMNS.filter(c => c !== 'plate')
    .map(c => `${c} = EXCLUDED.${c}`)
    .join(', ');

  const sql = `
    INSERT INTO vehicle_latest (${HISTORY_COLUMNS.join(', ')}, updated_at)
    VALUES (${placeholders(HISTORY_COLUMNS)}, NOW())
    ON CONFLICT (plate) DO UPDATE SET ${updates}, updated_at = NOW()
  `;
  await query(sql, HISTORY_COLUMNS.map(c => row[c]));
};

const insertFillSession = async (session) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot insert fill session ***');
    return;
  }
  const { data, error } = await supabase.from('energy_rite_operating_sessions').insert(session).select();
  if (error) {
    console.error(`[db] Supabase fill insert FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
  } else {
    console.log(`[db] Fill session inserted to Supabase: ${session.branch} ${session.session_date}`);
  }
};

const insertTheftSession = async (session) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot insert theft session ***');
    return;
  }
  const { data, error } = await supabase.from('energy_rite_operating_sessions').insert(session).select();
  if (error) {
    console.error(`[db] Supabase theft insert FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
  } else {
    console.log(`[db] Theft session inserted to Supabase: ${session.branch} ${session.session_date}`);
  }
};

const upsertFuelStop = async (stop) => {
  const sql = `
    INSERT INTO fuel_stops (id, name, coordinates, geozone_name, geozone_coordinates,
      location_coordinates, radius, type, address, city, state, country,
      contact_person, contact_phone, operating_hours, capacity, notes, prescribed_value, fuel_type, synced_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19, NOW())
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      coordinates = EXCLUDED.coordinates,
      geozone_name = EXCLUDED.geozone_name,
      geozone_coordinates = EXCLUDED.geozone_coordinates,
      location_coordinates = EXCLUDED.location_coordinates,
      radius = EXCLUDED.radius,
      type = EXCLUDED.type,
      address = EXCLUDED.address,
      city = EXCLUDED.city,
      state = EXCLUDED.state,
      country = EXCLUDED.country,
      contact_person = EXCLUDED.contact_person,
      contact_phone = EXCLUDED.contact_phone,
      operating_hours = EXCLUDED.operating_hours,
      capacity = EXCLUDED.capacity,
      notes = EXCLUDED.notes,
      prescribed_value = EXCLUDED.prescribed_value,
      fuel_type = EXCLUDED.fuel_type,
      synced_at = NOW()
  `;
  await query(sql, [
    stop.id, stop.name, JSON.stringify(stop.coordinates),
    stop.geozone_name, JSON.stringify(stop.geozone_coordinates),
    JSON.stringify(stop.location_coordinates), stop.radius, stop.type,
    stop.address, stop.city, stop.state, stop.country,
    stop.contact_person, stop.contact_phone, stop.operating_hours,
    stop.capacity, stop.notes, stop.prescribed_value, stop.fuel_type
  ]);
};

const insertGeozoneEvent = async (event) => {
  const sql = `
    INSERT INTO geozone_events (plate, fuel_stop_id, geozone_name, event_type,
      loc_time, latitude, longitude, fuel_before, fuel_after, fill_amount)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
  `;
  await query(sql, [
    event.plate, event.fuel_stop_id, event.geozone_name, event.event_type,
    event.loc_time, event.latitude, event.longitude,
    event.fuel_before, event.fuel_after, event.fill_amount
  ]);
};

const getLatestFuelReading = async (plate) => {
  const sql = `
    SELECT fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank, loc_time, created_at
    FROM vehicle_history
    WHERE plate = $1
      AND (fuel_probe_1_volume_in_tank > 0 OR fuel_probe_2_volume_in_tank > 0)
    ORDER BY created_at DESC
    LIMIT 1
  `;
  const { rows } = await query(sql, [plate]);
  return rows.length > 0 ? rows[0] : null;
};

const getLatestFuelBefore = async (plate, beforeTime) => {
  const sql = `
    SELECT fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank, loc_time, created_at
    FROM vehicle_history
    WHERE plate = $1
      AND loc_time::timestamptz < $2::timestamptz
      AND (fuel_probe_1_volume_in_tank > 0 OR fuel_probe_2_volume_in_tank > 0)
    ORDER BY loc_time::timestamptz DESC
    LIMIT 1
  `;
  const { rows } = await query(sql, [plate, beforeTime]);
  return rows.length > 0 ? rows[0] : null;
};

const getOngoingSession = async (plate) => {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('energy_rite_operating_sessions')
    .select('id, session_start_time, opening_fuel')
    .eq('branch', plate)
    .eq('session_status', 'ONGOING')
    .order('session_start_time', { ascending: false })
    .limit(1);
  if (error) {
    console.error(`[db] getOngoingSession error: ${error.message}`);
    return null;
  }
  return data && data.length > 0 ? data[0] : null;
};

const insertOperatingSession = async (session) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot insert session ***');
    return null;
  }
  const { data, error } = await supabase.from('energy_rite_operating_sessions').insert(session).select('id');
  if (error) {
    console.error(`[db] Supabase session insert FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
    return null;
  }
  return data && data.length > 0 ? data[0].id : null;
};

const closeOperatingSession = async (sessionId, closingData) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot close session ***');
    return;
  }
  const { error } = await supabase.from('energy_rite_operating_sessions')
    .update(closingData)
    .eq('id', sessionId);
  if (error) {
    console.error(`[db] Supabase session close FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
  }
};

const init = async () => {
  if (initialized) return;
  await waitForDatabase();
  await createTables();
  await seedVehicles();
  await loadVehicleCache();
  initialized = true;
};

const close = async () => {
  if (pool) {
    await pool.end();
    pool = null;
    initialized = false;
  }
};

module.exports = {
  init, close, query, isKnownVehicle, getCostCode, insertHistory, upsertLatest,
  insertFillSession, insertTheftSession,
  upsertFuelStop, insertGeozoneEvent, getLatestFuelBefore,
  getOngoingSession, insertOperatingSession, closeOperatingSession,
  getLatestFuelReading
};
