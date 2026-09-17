const { createClient } = require('@supabase/supabase-js');
const db = require('../../waterford-db');

const WATERFORD_URL = process.env.WATERFORD_SUPABASE_URL;
const WATERFORD_KEY = process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY;
const waterfordSupabase = (WATERFORD_URL && WATERFORD_KEY)
  ? createClient(WATERFORD_URL, WATERFORD_KEY, { auth: { persistSession: false } })
  : null;

const extractVehicleReg = (vehicleassignments) => {
  if (!vehicleassignments || !Array.isArray(vehicleassignments) || vehicleassignments.length === 0) return null;
  const assignment = vehicleassignments[0];
  return assignment?.vehicle?.name?.toUpperCase() || null;
};

const extractDriverInfo = (vehicleassignments) => {
  if (!vehicleassignments || !Array.isArray(vehicleassignments) || vehicleassignments.length === 0) return null;
  const assignment = vehicleassignments[0];
  return assignment?.drivers?.[0] || null;
};

const extractTrailerInfo = (vehicleassignments) => {
  if (!vehicleassignments || !Array.isArray(vehicleassignments) || vehicleassignments.length === 0) return null;
  const assignment = vehicleassignments[0];
  return {
    trailer: assignment?.trailer || null,
    trailers: assignment?.trailers || null
  };
};

const syncTrips = async () => {
  if (!waterfordSupabase) {
    console.warn('[sync] Skipping trips sync - no WATERFORD Supabase client');
    return 0;
  }

  try {
    const { data: trips, error } = await waterfordSupabase
      .from('trips')
      .select('trip_id, vehicleassignments, status, selected_stop_points, origin, destination, created_at')
      .not('status', 'in', '("delivered","completed","cancelled")');

    if (error) throw error;
    if (!trips || trips.length === 0) {
      console.log('[sync] No active trips returned from WATERFORD Supabase');
      return 0;
    }

    let synced = 0;
    const syncedIds = [];
    for (const trip of trips) {
      const vehicleReg = extractVehicleReg(trip.vehicleassignments);
      if (!vehicleReg) {
        console.warn(`[sync] Trip ${trip.trip_id} has no vehicle assignment, skipping`);
        continue;
      }

      await db.upsertTrip({
        trip_id: trip.trip_id,
        vehicle_reg: vehicleReg,
        status: trip.status,
        selected_stop_points: trip.selected_stop_points || [],
        driver_info: extractDriverInfo(trip.vehicleassignments),
        trailer_info: extractTrailerInfo(trip.vehicleassignments),
        origin: trip.origin,
        destination: trip.destination
      });
      syncedIds.push(trip.trip_id);
      synced++;
    }

    if (syncedIds.length > 0) {
      const removed = await db.deleteStaleTrips(syncedIds);
      if (removed) console.log(`[sync] Removed ${removed} stale trips not in Supabase`);
    }

    console.log(`[sync] Synced ${synced} trips from WATERFORD Supabase`);
    return synced;
  } catch (err) {
    console.error(`[sync] Trips sync failed: ${err.message}`);
    return 0;
  }
};

module.exports = { syncTrips };