const { query } = require('./index');

const insertTripZoneEvent = async (event) => {
  const sql = `
    INSERT INTO trip_zone_events (trip_id, plate, zone_id, zone_name, event_type,
      loc_time, latitude, longitude, sequence_order)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
  `;
  await query(sql, [
    event.trip_id,
    event.plate,
    event.zone_id,
    event.zone_name,
    event.event_type,
    event.loc_time,
    event.latitude,
    event.longitude,
    event.sequence_order
  ]);
};

const getTripZoneEvents = async (tripId) => {
  const { rows } = await query(
    `SELECT * FROM trip_zone_events WHERE trip_id = $1 ORDER BY sequence_order, loc_time`,
    [tripId]
  );
  return rows;
};

const getLatestEventPerZone = async (tripId) => {
  const { rows } = await query(
    `SELECT DISTINCT ON (zone_id) * FROM trip_zone_events
     WHERE trip_id = $1
     ORDER BY zone_id, loc_time DESC`,
    [tripId]
  );
  return rows;
};

module.exports = { insertTripZoneEvent, getTripZoneEvents, getLatestEventPerZone };