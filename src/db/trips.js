const { query } = require('./index');

const upsertTrip = async (trip) => {
  const sql = `
    INSERT INTO trips (trip_id, vehicle_reg, status, selected_stop_points,
      driver_info, trailer_info, origin, destination, synced_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8, NOW())
    ON CONFLICT (trip_id) DO UPDATE SET
      vehicle_reg = EXCLUDED.vehicle_reg,
      status = EXCLUDED.status,
      selected_stop_points = EXCLUDED.selected_stop_points,
      driver_info = EXCLUDED.driver_info,
      trailer_info = EXCLUDED.trailer_info,
      origin = EXCLUDED.origin,
      destination = EXCLUDED.destination,
      synced_at = NOW()
  `;
  await query(sql, [
    trip.trip_id,
    trip.vehicle_reg,
    trip.status,
    JSON.stringify(trip.selected_stop_points),
    JSON.stringify(trip.driver_info),
    JSON.stringify(trip.trailer_info),
    trip.origin,
    trip.destination
  ]);
};

const getActiveTripForVehicle = async (vehicleReg) => {
  const { rows } = await query(
    `SELECT * FROM trips
     WHERE vehicle_reg = $1
       AND status NOT IN ('delivered', 'completed', 'cancelled')
     ORDER BY synced_at DESC
     LIMIT 1`,
    [vehicleReg.toUpperCase()]
  );
  return rows[0] || null;
};

const getTripById = async (tripId) => {
  const { rows } = await query('SELECT * FROM trips WHERE trip_id = $1', [tripId]);
  return rows[0] || null;
};

const updateTripStatus = async (tripId, status) => {
  await query(
    'UPDATE trips SET status = $1 WHERE trip_id = $2',
    [status, tripId]
  );
};

const getAllActiveTrips = async () => {
  const { rows } = await query(
    `SELECT * FROM trips WHERE status NOT IN ('delivered', 'completed', 'cancelled')`
  );
  return rows;
};

const deleteStaleTrips = async (syncedTripIds) => {
  if (!syncedTripIds.length) return 0;
  const { rowCount } = await query(
    `DELETE FROM trips WHERE trip_id <> ALL($1::varchar[]) `,
    [syncedTripIds]
  );
  return rowCount;
};

module.exports = { upsertTrip, getActiveTripForVehicle, getTripById, updateTripStatus, getAllActiveTrips, deleteStaleTrips };