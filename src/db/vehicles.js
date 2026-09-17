const { createClient } = require('@supabase/supabase-js');
const { query } = require('./index');

let vehicleCache = new Set();

const WATERFORD_URL = process.env.WATERFORD_SUPABASE_URL;
const WATERFORD_KEY = process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY;
const waterfordSupabase = (WATERFORD_URL && WATERFORD_KEY)
  ? createClient(WATERFORD_URL, WATERFORD_KEY, { auth: { persistSession: false } })
  : null;

const loadVehicleCache = async () => {
  const { rows } = await query('SELECT plate FROM vehicles');
  vehicleCache.clear();
  rows.forEach(r => vehicleCache.add(r.plate));
  console.log(`[db] Vehicle cache loaded: ${vehicleCache.size} plates`);
};

const syncVehicles = async () => {
  if (!waterfordSupabase) {
    console.warn('[db] Skipping vehicle sync - no WATERFORD Supabase client');
    return 0;
  }

  try {
    const { data: vehicles, error } = await waterfordSupabase
      .from('vehiclesc')
      .select('registration_number');

    if (error) throw error;
    if (!vehicles || vehicles.length === 0) {
      console.log('[db] No vehicles returned from WATERFORD Supabase');
      return 0;
    }

    let synced = 0;
    for (const v of vehicles) {
      const plate = (v.registration_number || '').trim().toUpperCase();
      if (!plate) continue;
      await query(
        'INSERT INTO vehicles (plate, cost_code) VALUES ($1, $2) ON CONFLICT (plate) DO UPDATE SET cost_code = $2',
        [plate, 'WATE-0001']
      );
      synced++;
    }

    await loadVehicleCache();
    console.log(`[db] Synced ${synced} vehicles from WATERFORD Supabase (${vehicleCache.size} total cached)`);
    return synced;
  } catch (err) {
    console.error(`[db] Vehicle sync failed: ${err.message}`);
    return 0;
  }
};

const isKnownVehicle = (plate) => vehicleCache.has(plate);
const getCostCode = (plate) => vehicleCache.has(plate) ? 'WATE-0001' : null;

module.exports = { loadVehicleCache, syncVehicles, isKnownVehicle, getCostCode, vehicleCache };