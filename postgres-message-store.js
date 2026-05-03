const { Pool } = require('pg');

const tableName = process.env.POSTGRES_MESSAGE_TABLE || 'energy_rite_realtime_messages';
const safeIdentifier = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

if (!safeIdentifier.test(tableName)) {
  throw new Error(`Invalid POSTGRES_MESSAGE_TABLE value: "${tableName}"`);
}

const TABLE_NAME = tableName;
const TABLE_REF = `"${TABLE_NAME}"`;

const pool = new Pool({
  host: process.env.PGHOST || '127.0.0.1',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'fuel',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD
});

let initialized = false;

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
    fuel_probe_1_volume_in_tank: toNumber(row.fuel_probe_1_volume_in_tank) ?? 0,
    fuel_probe_2_volume_in_tank: toNumber(row.fuel_probe_2_volume_in_tank) ?? 0,
    fuel_probe_1_level_percentage: toNumber(row.fuel_probe_1_level_percentage) ?? 0,
    fuel_probe_2_level_percentage: toNumber(row.fuel_probe_2_level_percentage) ?? 0,
    combined_fuel_volume_in_tank: toNumber(row.combined_fuel_volume_in_tank) ?? 0,
    combined_fuel_percentage: toNumber(row.combined_fuel_percentage) ?? 0,
    speed: toNumber(row.speed) ?? 0
  };
}

async function init() {
  if (initialized) return;

  await pool.query(`
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
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_${TABLE_NAME}_plate_time
      ON ${TABLE_REF} (plate, message_time DESC)
  `);

  initialized = true;
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

  await pool.query(query, values);
}

async function getRecentFuelMessages(plate, limit = 10, options = {}) {
  await init();

  const values = [plate];
  let index = values.length;
  const whereParts = [
    'plate = $1',
    'combined_fuel_volume_in_tank IS NOT NULL'
  ];

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

  const { rows } = await pool.query(query, values);
  return rows.map(normalizeMessage).filter(Boolean);
}

async function getFuelMessagesInRange(plate, startTimeIso, endTimeIso, limit = 500) {
  await init();

  const query = `
    SELECT
      plate,
      loc_time,
      message_time,
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

  const { rows } = await pool.query(query, [plate, startTimeIso, endTimeIso, limit]);
  return rows.map(normalizeMessage).filter(Boolean);
}

module.exports = {
  TABLE_NAME,
  init,
  storeMessage,
  getRecentFuelMessages,
  getFuelMessagesInRange
};
