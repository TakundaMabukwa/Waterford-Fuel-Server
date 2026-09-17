const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;
const db = require('../../waterford-db');

let cachedZones = null;

const findZone = async (lat, lon) => {
  if (!lat || !lon) return null;

  try {
    if (!cachedZones) {
      const { rows } = await db.getAllZones();
      cachedZones = rows;
      console.log(`[geozone] Cached ${rows.length} zones from local DB`);
    }

    for (const zone of cachedZones) {
      let polygon = zone.coordinates;

      if (typeof polygon === 'string') {
        try { polygon = JSON.parse(polygon); } catch { continue; }
      }

      if (!Array.isArray(polygon) || polygon.length < 3) continue;

      const closedRing = [...polygon, polygon[0]];
      const turfPolygon = {
        type: 'Feature',
        geometry: { type: 'Polygon', coordinates: [closedRing] },
        properties: {}
      };

      const point = [lon, lat];

      if (booleanPointInPolygon(point, turfPolygon)) {
        return zone;
      }
    }

    return null;
  } catch (err) {
    console.error(`[geozone] findZone error: ${err.message}`);
    return null;
  }
};

const clearZoneCache = () => {
  cachedZones = null;
};

module.exports = { findZone, clearZoneCache };