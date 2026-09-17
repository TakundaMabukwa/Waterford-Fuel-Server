const { query } = require('./index');

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

module.exports = { insertHistory, upsertLatest, insertGeozoneEvent, getLatestFuelReading, getLatestFuelBefore, HISTORY_COLUMNS };