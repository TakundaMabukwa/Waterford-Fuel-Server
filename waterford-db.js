const { Pool } = require('pg');

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
    CREATE INDEX IF NOT EXISTS idx_vehicle_history_plate_loc_time
      ON vehicle_history (plate, loc_time)
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

const FUEL_PROBE_COLUMNS = [
  'fuel_probe_1_level', 'fuel_probe_1_volume_in_tank',
  'fuel_probe_1_temperature', 'fuel_probe_1_level_percentage',
  'fuel_probe_2_level', 'fuel_probe_2_volume_in_tank',
  'fuel_probe_2_temperature', 'fuel_probe_2_level_percentage'
];

const getLowestFuelBefore = async (plate, locTime) => {
  const { rows } = await query(
    `SELECT ${FUEL_PROBE_COLUMNS.join(', ')}
     FROM vehicle_history
     WHERE plate = $1 AND loc_time < $2
       AND fuel_probe_1_volume_in_tank IS NOT NULL
       AND fuel_probe_1_volume_in_tank > 0
     ORDER BY loc_time DESC
     LIMIT 3`,
    [plate, locTime]
  );
  if (!rows.length) return null;
  return rows.reduce((min, r) =>
    (r.fuel_probe_1_volume_in_tank ?? Infinity) < (min.fuel_probe_1_volume_in_tank ?? Infinity) ? r : min
  );
};

const getLowestFuelAfter = async (plate, locTime) => {
  const { rows } = await query(
    `SELECT ${FUEL_PROBE_COLUMNS.join(', ')}
     FROM vehicle_history
     WHERE plate = $1 AND loc_time >= $2
     ORDER BY loc_time ASC
     LIMIT 3`,
    [plate, locTime]
  );
  if (!rows.length) return null;
  return rows.reduce((min, r) =>
    (r.fuel_probe_1_volume_in_tank ?? Infinity) < (min.fuel_probe_1_volume_in_tank ?? Infinity) ? r : min
  );
};

const getLastEngineEventBefore = async (plate, locTime) => {
  const { rows } = await query(
    `SELECT status, loc_time, fuel_probe_1_volume_in_tank, fuel_probe_1_level_percentage
     FROM vehicle_history
     WHERE plate = $1 AND loc_time < $2
       AND (
         UPPER(status) LIKE '%ENGINE OFF%'
         OR UPPER(status) LIKE '%IGNITION OFF%'
       )
     ORDER BY loc_time DESC
     LIMIT 1`,
    [plate, locTime]
  );
  return rows[0] || null;
};

const getFuelFillBetween = async (plate, startTime, endTime) => {
  const { rows } = await query(
    `SELECT status, loc_time
     FROM vehicle_history
     WHERE plate = $1 AND loc_time > $2 AND loc_time < $3
       AND UPPER(status) LIKE '%FUEL FILL%'
     ORDER BY loc_time ASC
     LIMIT 1`,
    [plate, startTime, endTime]
  );
  return rows[0] || null;
};

const getLastFuelBefore = async (plate, locTime) => {
  const { rows } = await query(
    `SELECT ${FUEL_PROBE_COLUMNS.join(', ')}
     FROM vehicle_history
     WHERE plate = $1 AND loc_time <= $2
       AND fuel_probe_1_volume_in_tank IS NOT NULL
     ORDER BY loc_time DESC
     LIMIT 1`,
    [plate, locTime]
  );
  return rows[0] || null;
};

const getHighestFuelBefore = async (plate, locTime) => {
  const { rows } = await query(
    `SELECT ${FUEL_PROBE_COLUMNS.join(', ')}
     FROM vehicle_history
     WHERE plate = $1 AND loc_time <= $2
       AND fuel_probe_1_volume_in_tank IS NOT NULL
       AND fuel_probe_1_volume_in_tank > 0
     ORDER BY loc_time DESC
     LIMIT 5`,
    [plate, locTime]
  );
  if (!rows.length) return null;
  return rows.reduce((max, r) =>
    (r.fuel_probe_1_volume_in_tank ?? -1) > (max.fuel_probe_1_volume_in_tank ?? -1) ? r : max
  );
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
  getLowestFuelBefore, getLowestFuelAfter, getLastEngineEventBefore, getFuelFillBetween, getLastFuelBefore, getHighestFuelBefore
};
