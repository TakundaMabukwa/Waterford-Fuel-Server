const { query } = require('./index');

const upsertZone = async (zone) => {
  const sql = `
    INSERT INTO zones (id, name, coordinates, geozone_name, type, source_type,
      location_lat, location_lng, radius, synced_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW())
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      coordinates = EXCLUDED.coordinates,
      geozone_name = EXCLUDED.geozone_name,
      type = EXCLUDED.type,
      source_type = EXCLUDED.source_type,
      location_lat = EXCLUDED.location_lat,
      location_lng = EXCLUDED.location_lng,
      radius = EXCLUDED.radius,
      synced_at = NOW()
  `;
  await query(sql, [
    zone.id, zone.name, JSON.stringify(zone.coordinates),
    zone.geozone_name, zone.type, zone.source_type,
    zone.location_lat, zone.location_lng, zone.radius
  ]);
};

const getAllZones = async () => {
  const { rows } = await query('SELECT * FROM zones WHERE coordinates IS NOT NULL');
  return rows;
};

const getZoneById = async (id) => {
  const { rows } = await query('SELECT * FROM zones WHERE id = $1', [id]);
  return rows[0] || null;
};

const deleteStaleZones = async (syncedIds) => {
  if (!syncedIds.length) return 0;
  const { rowCount } = await query(
    `DELETE FROM zones WHERE id <> ALL($1::varchar[]) `,
    [syncedIds]
  );
  return rowCount;
};

module.exports = { upsertZone, getAllZones, getZoneById, deleteStaleZones };