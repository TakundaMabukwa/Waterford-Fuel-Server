const { createClient } = require('@supabase/supabase-js');
const db = require('../../waterford-db');

const WATERFORD_URL = process.env.WATERFORD_SUPABASE_URL;
const WATERFORD_KEY = process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY;
const waterfordSupabase = (WATERFORD_URL && WATERFORD_KEY)
  ? createClient(WATERFORD_URL, WATERFORD_KEY, { auth: { persistSession: false } })
  : null;

const parseCoordinates = (coordString) => {
  if (!coordString) return null;
  try {
    if (typeof coordString === 'string') {
      return JSON.parse(coordString);
    }
    return coordString;
  } catch {
    return null;
  }
};

const syncZones = async () => {
  if (!waterfordSupabase) {
    console.warn('[sync] Skipping zones sync - no WATERFORD Supabase client');
    return 0;
  }

  try {
    const { data: stops, error } = await waterfordSupabase
      .from('fuel_stops')
      .select('*');

    if (error) throw error;
    if (!stops || stops.length === 0) {
      console.log('[sync] No zones returned from WATERFORD Supabase');
      return 0;
    }

    // Deduplicate by name - keep first occurrence
    const seenNames = new Set();
    const uniqueStops = [];
    for (const stop of stops) {
      if (!seenNames.has(stop.name)) {
        seenNames.add(stop.name);
        uniqueStops.push(stop);
      } else {
        console.log(`[sync] Skipping duplicate zone name: ${stop.name} (id: ${stop.id})`);
      }
    }

    let synced = 0;
    const syncedIds = [];
    for (const stop of uniqueStops) {
      const coordinates = parseCoordinates(stop.coordinates) || stop.geozone_coordinates;
      if (!coordinates || !Array.isArray(coordinates) || coordinates.length < 3) continue;

      const locationLat = stop.location_coordinates?.lat || 
        (Array.isArray(coordinates) ? coordinates[0][1] : null);
      const locationLng = stop.location_coordinates?.lng || 
        (Array.isArray(coordinates) ? coordinates[0][0] : null);

      await db.upsertZone({
        id: stop.id,
        name: stop.name,
        coordinates: coordinates,
        geozone_name: stop.geozone_name,
        type: stop.type,
        source_type: stop.source_type,
        location_lat: locationLat,
        location_lng: locationLng,
        radius: stop.radius || 100
      });
      syncedIds.push(stop.id);
      synced++;
    }

    if (syncedIds.length > 0) {
      const removed = await db.deleteStaleZones(syncedIds);
      if (removed) console.log(`[sync] Removed ${removed} stale zones not in Supabase`);
    }

    console.log(`[sync] Synced ${synced} zones from WATERFORD Supabase (${uniqueStops.length} unique)`);
    return synced;
  } catch (err) {
    console.error(`[sync] Zones sync failed: ${err.message}`);
    return 0;
  }
};

module.exports = { syncZones };