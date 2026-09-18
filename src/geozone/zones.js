const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;
const db = require('../../waterford-db');

let cachedZones = null;

const extractCoords = (coordinates) => {
  if (!coordinates) return null;
  let c = coordinates;
  if (typeof c === 'string') {
    try { c = JSON.parse(c); } catch { return null; }
  }
  if (Array.isArray(c)) return c;
  if (c.type === 'Polygon' && Array.isArray(c.coordinates) && Array.isArray(c.coordinates[0])) {
    return c.coordinates[0];
  }
  return null;
};

const findZone = async (lat, lon) => {
  if (!lat || !lon) return null;

  try {
    if (!cachedZones) {
      const { rows } = await db.getAllZones();
      cachedZones = rows.filter(z => {
        const coords = extractCoords(z.coordinates);
        return coords && coords.length >= 3;
      });
      console.log(`[geozone] Cached ${cachedZones.length} valid zones from local DB`);
    }

    for (const zone of cachedZones) {
      const polygon = extractCoords(zone.coordinates);
      if (!polygon) continue;

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