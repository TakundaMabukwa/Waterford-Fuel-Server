const { Pool } = require('pg');

const tableName = process.env.POSTGRES_MESSAGE_TABLE || 'energy_rite_realtime_messages';
const safeIdentifier = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

if (!safeIdentifier.test(tableName)) {
  throw new Error(`Invalid POSTGRES_MESSAGE_TABLE value: "${tableName}"`);
}

const TABLE_NAME = tableName;
const TABLE_REF = `"${TABLE_NAME}"`;

const poolConfig = {
  host: process.env.PGHOST || '127.0.0.1',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'fuel',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD
};

function createPool() {
  const nextPool = new Pool(poolConfig);
  nextPool.on('error', (error) => {
    console.error(`[postgres] Pool error: ${error.message}`);
  });
  return nextPool;
}

let pool = createPool();
let initialized = false;
let initPromise = null;
let resetPoolPromise = null;

const QUERY_MAX_RETRIES = (() => {
  const parsed = parseInt(process.env.POSTGRES_QUERY_MAX_RETRIES || '5', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 5;
})();

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toNumber(value) {
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toTimestamp(value) {
  if (!value) return null;
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
}

function normalizeMessage(row) {
  if (!row) return null;

  const timestamp = toTimestamp(row.message_time);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return {
    plate: row.plate,
    locTime: row.loc_time,
    timestamp,
    driver_name: row.driver_name || null,
    fuel_probe_1_volume_in_tank: toNumber(row.fuel_probe_1_volume_in_tank) ?? 0,
    fuel_probe_2_volume_in_tank: toNumber(row.fuel_probe_2_volume_in_tank) ?? 0,
    fuel_probe_1_level_percentage: toNumber(row.fuel_probe_1_level_percentage) ?? 0,
    fuel_probe_2_level_percentage: toNumber(row.fuel_probe_2_level_percentage) ?? 0,
    combined_fuel_volume_in_tank: toNumber(row.combined_fuel_volume_in_tank) ?? 0,
    combined_fuel_percentage: toNumber(row.combined_fuel_percentage) ?? 0,
    speed: toNumber(row.speed) ?? 0
  };
}

function normalizeStatusMessage(row) {
  if (!row) return null;

  const timestamp = toTimestamp(row.message_time);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return {
    plate: row.plate,
    locTime: row.loc_time,
    timestamp,
    driver_name: row.driver_name || null,
    speed: toNumber(row.speed) ?? 0
  };
}

function isTransientError(error) {
  if (!error) return false;

  const transientCodes = new Set([
    '40001', // serialization_failure
    '40P01', // deadlock_detected
    '53300', // too_many_connections
    '57P01', // admin_shutdown
    '57P02', // crash_shutdown
    '57P03', // cannot_connect_now
    '08000', // connection_exception
    '08001', // sqlclient_unable_to_establish_sqlconnection
    '08003', // connection_does_not_exist
    '08004', // sqlserver_rejected_establishment_of_sqlconnection
    '08006' // connection_failure
  ]);

  if (error.code && transientCodes.has(error.code)) {
    return true;
  }

  const message = String(error.message || '').toLowerCase();
  return (
    message.includes('connection terminated unexpectedly') ||
    message.includes('client has encountered a connection error') ||
    message.includes('socket hang up') ||
    message.includes('econnreset') ||
    message.includes('etimedout') ||
    message.includes('ehostunreach') ||
    message.includes('econnrefused')
  );
}

async function resetPool(reason = 'unknown') {
  if (resetPoolPromise) {
    await resetPoolPromise;
    return;
  }

  const currentPool = pool;
  resetPoolPromise = (async () => {
    initialized = false;
    pool = createPool();

    try {
      await currentPool.end();
    } catch (error) {
      console.error(`[postgres] Error closing old pool during reset: ${error.message}`);
    }

    console.warn(`[postgres] Pool reset (${reason})`);
  })();

  try {
    await resetPoolPromise;
  } finally {
    resetPoolPromise = null;
  }
}

async function runQueryWithRetry(query, values = [], context = 'query') {
  let attempt = 0;
  let delayMs = 200;

  while (attempt < QUERY_MAX_RETRIES) {
    attempt += 1;
    try {
      return await pool.query(query, values);
    } catch (error) {
      const transient = isTransientError(error);
      const isLastAttempt = attempt >= QUERY_MAX_RETRIES;

      if (!transient || isLastAttempt) {
        if (transient) {
          console.error(
            `[postgres] ${context} failed after ${attempt} attempts: ${error.message}`
          );
        }
        throw error;
      }

      console.warn(
        `[postgres] ${context} transient failure on attempt ${attempt}/${QUERY_MAX_RETRIES}: ` +
        `${error.message}. Retrying in ${delayMs}ms`
      );

      await resetPool(`${context}:${error.code || 'transient_error'}`);
      await delay(delayMs);
      delayMs = Math.min(delayMs * 2, 5000);
    }
  }
}

async function init() {
  if (initialized) return;
  if (initPromise) {
    await initPromise;
    return;
  }

  initPromise = (async () => {
    await runQueryWithRetry(
      `
      CREATE TABLE IF NOT EXISTS ${TABLE_REF} (
        id BIGSERIAL PRIMARY KEY,
        plate TEXT NOT NULL,
        loc_time TEXT NOT NULL,
        message_time TIMESTAMPTZ NOT NULL,
        driver_name TEXT,
        speed DOUBLE PRECISION,
        fuel_probe_1_level DOUBLE PRECISION,
        fuel_probe_2_level DOUBLE PRECISION,
        fuel_probe_1_level_percentage DOUBLE PRECISION,
        fuel_probe_2_level_percentage DOUBLE PRECISION,
        fuel_probe_1_volume_in_tank DOUBLE PRECISION,
        fuel_probe_2_volume_in_tank DOUBLE PRECISION,
        combined_fuel_volume_in_tank DOUBLE PRECISION,
        combined_fuel_percentage DOUBLE PRECISION,
        raw_message JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `,
      [],
      'init.create_table'
    );

    await runQueryWithRetry(
      `
      CREATE INDEX IF NOT EXISTS idx_${TABLE_NAME}_plate_time
        ON ${TABLE_REF} (plate, message_time DESC)
    `,
      [],
      'init.create_index'
    );

    initialized = true;
  })();

  try {
    await initPromise;
  } finally {
    initPromise = null;
  }
}

async function storeMessage(message, fuelData, messageTimeIso) {
  await init();

  const hasFuelData = !!fuelData?.hasFuelData;

  const query = `
    INSERT INTO ${TABLE_REF} (
      plate,
      loc_time,
      message_time,
      driver_name,
      speed,
      fuel_probe_1_level,
      fuel_probe_2_level,
      fuel_probe_1_level_percentage,
      fuel_probe_2_level_percentage,
      fuel_probe_1_volume_in_tank,
      fuel_probe_2_volume_in_tank,
      combined_fuel_volume_in_tank,
      combined_fuel_percentage,
      raw_message
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7,
      $8, $9, $10, $11, $12, $13, $14
    )
  `;

  const values = [
    message.Plate,
    message.LocTime,
    messageTimeIso,
    message.DriverName || null,
    toNumber(message.Speed),
    toNumber(message.fuel_probe_1_level),
    toNumber(message.fuel_probe_2_level),
    hasFuelData ? toNumber(fuelData.fuel_probe_1_level_percentage) : null,
    hasFuelData ? toNumber(fuelData.fuel_probe_2_level_percentage) : null,
    hasFuelData ? toNumber(fuelData.fuel_probe_1_volume_in_tank) : null,
    hasFuelData ? toNumber(fuelData.fuel_probe_2_volume_in_tank) : null,
    hasFuelData ? toNumber(fuelData.combined_fuel_volume_in_tank) : null,
    hasFuelData ? toNumber(fuelData.combined_fuel_percentage) : null,
    JSON.stringify(message)
  ];

  await runQueryWithRetry(query, values, 'storeMessage.insert');
}

async function getRecentFuelMessages(plate, limit = 10, options = {}) {
  await init();

  const values = [plate];
  let index = values.length;
  const whereParts = ['plate = $1', 'combined_fuel_volume_in_tank IS NOT NULL'];

  if (options.beforeTimeIso) {
    index += 1;
    whereParts.push(`message_time < $${index}`);
    values.push(options.beforeTimeIso);
  }

  if (options.afterTimeIso) {
    index += 1;
    whereParts.push(`message_time > $${index}`);
    values.push(options.afterTimeIso);
  }

  index += 1;
  values.push(limit);

  const query = `
    SELECT
      plate,
      loc_time,
      message_time,
      driver_name,
      speed,
      fuel_probe_1_level_percentage,
      fuel_probe_2_level_percentage,
      fuel_probe_1_volume_in_tank,
      fuel_probe_2_volume_in_tank,
      combined_fuel_volume_in_tank,
      combined_fuel_percentage
    FROM ${TABLE_REF}
    WHERE ${whereParts.join(' AND ')}
    ORDER BY message_time DESC
    LIMIT $${index}
  `;

  const { rows } = await runQueryWithRetry(query, values, 'getRecentFuelMessages.select');
  return rows.map(normalizeMessage).filter(Boolean);
}

async function getFuelMessagesInRange(plate, startTimeIso, endTimeIso, limit = 500) {
  await init();

  const query = `
    SELECT
      plate,
      loc_time,
      message_time,
      driver_name,
      speed,
      fuel_probe_1_level_percentage,
      fuel_probe_2_level_percentage,
      fuel_probe_1_volume_in_tank,
      fuel_probe_2_volume_in_tank,
      combined_fuel_volume_in_tank,
      combined_fuel_percentage
    FROM ${TABLE_REF}
    WHERE plate = $1
      AND message_time >= $2
      AND message_time <= $3
      AND combined_fuel_volume_in_tank IS NOT NULL
    ORDER BY message_time ASC
    LIMIT $4
  `;

  const { rows } = await runQueryWithRetry(
    query,
    [plate, startTimeIso, endTimeIso, limit],
    'getFuelMessagesInRange.select'
  );
  return rows.map(normalizeMessage).filter(Boolean);
}

async function getRecentStatusMessages(plate, limit = 20, options = {}) {
  await init();

  const values = [plate];
  let index = values.length;
  const whereParts = ['plate = $1', "driver_name IS NOT NULL", "TRIM(driver_name) <> ''"];

  if (options.beforeTimeIso) {
    index += 1;
    whereParts.push(`message_time < $${index}`);
    values.push(options.beforeTimeIso);
  }

  if (options.afterTimeIso) {
    index += 1;
    whereParts.push(`message_time > $${index}`);
    values.push(options.afterTimeIso);
  }

  index += 1;
  values.push(limit);

  const query = `
    SELECT
      plate,
      loc_time,
      message_time,
      driver_name,
      speed
    FROM ${TABLE_REF}
    WHERE ${whereParts.join(' AND ')}
    ORDER BY message_time DESC
    LIMIT $${index}
  `;

  const { rows } = await runQueryWithRetry(query, values, 'getRecentStatusMessages.select');
  return rows.map(normalizeStatusMessage).filter(Boolean);
}

module.exports = {
  TABLE_NAME,
  init,
  storeMessage,
  getRecentFuelMessages,
  getFuelMessagesInRange,
  getRecentStatusMessages
};
