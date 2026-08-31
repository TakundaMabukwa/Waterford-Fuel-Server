const { createClient } = require('@supabase/supabase-js');
const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;
const db = require('./waterford-db');

const WATERFORD_URL = process.env.WATERFORD_SUPABASE_URL;
const WATERFORD_KEY = process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY;

let waterfordSupabase = null;

if (WATERFORD_URL && WATERFORD_KEY) {
  waterfordSupabase = createClient(WATERFORD_URL, WATERFORD_KEY, { auth: { persistSession: false } });
  console.log('[geozone] WATERFORD Supabase client initialized');
} else {
  console.warn('[geozone] WATERFORD Supabase credentials not set - fuel stop sync disabled');
}

const syncFuelStops = async () => {
  if (!waterfordSupabase) {
    console.warn('[geozone] Skipping sync - no WATERFORD Supabase client');
    return 0;
  }

  try {
    const { data: stops, error } = await waterfordSupabase
      .from('fuel_stops')
      .select('*');

    if (error) throw error;
    if (!stops || stops.length === 0) {
      console.log('[geozone] No fuel stops found in WATERFORD Supabase');
      return 0;
    }

    let synced = 0;
    for (const stop of stops) {
      await db.upsertFuelStop({
        id: stop.id,
        name: stop.name,
        coordinates: stop.coordinates,
        geozone_name: stop.geozone_name,
        geozone_coordinates: stop.geozone_coordinates,
        location_coordinates: stop.location_coordinates,
        radius: stop.radius || 100,
        type: stop.type || 'warehouse',
        address: stop.address,
        city: stop.city,
        state: stop.state,
        country: stop.country,
        contact_person: stop.contact_person,
        contact_phone: stop.contact_phone,
        operating_hours: stop.operating_hours,
        capacity: stop.capacity,
        notes: stop.notes,
        prescribed_value: stop.prescribed_value,
      });
      synced++;
    }

    console.log(`[geozone] Synced ${synced} fuel stops from WATERFORD Supabase`);
    return synced;
  } catch (err) {
    console.error(`[geozone] Sync failed: ${err.message}`);
    return 0;
  }
};

const findFuelStop = async (lat, lon) => {
  if (!lat || !lon) return null;

  try {
    const { rows } = await db.query('SELECT * FROM fuel_stops WHERE coordinates IS NOT NULL');

    for (const stop of rows) {
      let polygon = stop.coordinates;

      if (typeof polygon === 'string') {
        try { polygon = JSON.parse(polygon); } catch { continue; }
      }

      if (!Array.isArray(polygon) || polygon.length < 3) continue;

      const turfPolygon = {
        type: 'Feature',
        geometry: { type: 'Polygon', coordinates: [polygon] },
        properties: {}
      };

      const point = [lon, lat];

      if (booleanPointInPolygon(point, turfPolygon)) {
        return stop;
      }
    }

    return null;
  } catch (err) {
    console.error(`[geozone] findFuelStop error: ${err.message}`);
    return null;
  }
};

module.exports = { syncFuelStops, findFuelStop };
