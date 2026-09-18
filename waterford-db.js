const dbCore = require('./src/db/index');
const vehicles = require('./src/db/vehicles');
const history = require('./src/db/history');
const sessions = require('./src/db/sessions');
const zones = require('./src/db/zones');
const trips = require('./src/db/trips');
const tripEvents = require('./src/db/tripEvents');
const schema = require('./src/db/schema');
const { query } = require('./src/db/index');

// upsertFuelStop for fuel_stops table (used by existing fuel system)
const upsertFuelStop = async (stop) => {
  const sql = `
    INSERT INTO fuel_stops (id, name, coordinates, geozone_name, geozone_coordinates,
      location_coordinates, radius, type, address, city, state, country,
      contact_person, contact_phone, operating_hours, capacity, notes, prescribed_value, fuel_type, synced_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19, NOW())
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      coordinates = EXCLUDED.coordinates,
      geozone_name = EXCLUDED.geozone_name,
      geozone_coordinates = EXCLUDED.geozone_coordinates,
      location_coordinates = EXCLUDED.location_coordinates,
      radius = EXCLUDED.radius,
      type = EXCLUDED.type,
      address = EXCLUDED.address,
      city = EXCLUDED.city,
      state = EXCLUDED.state,
      country = EXCLUDED.country,
      contact_person = EXCLUDED.contact_person,
      contact_phone = EXCLUDED.contact_phone,
      operating_hours = EXCLUDED.operating_hours,
      capacity = EXCLUDED.capacity,
      notes = EXCLUDED.notes,
      prescribed_value = EXCLUDED.prescribed_value,
      fuel_type = EXCLUDED.fuel_type,
      synced_at = NOW()
  `;
  await query(sql, [
    stop.id, stop.name, JSON.stringify(stop.coordinates),
    stop.geozone_name, JSON.stringify(stop.geozone_coordinates),
    JSON.stringify(stop.location_coordinates), stop.radius, stop.type,
    stop.address, stop.city, stop.state, stop.country,
    stop.contact_person, stop.contact_phone, stop.operating_hours,
    stop.capacity, stop.notes, stop.prescribed_value, stop.fuel_type
  ]);
};

const init = async () => {
  if (dbCore._initialized) return;
  await dbCore.waitForDatabase();
  await schema.createTables();
  await vehicles.syncVehicles();
  await vehicles.loadVehicleCache();
  dbCore._initialized = true;
};

module.exports = {
  // Core
  init,
  close: dbCore.close,
  query: dbCore.query,
  waitForDatabase: dbCore.waitForDatabase,
  
  // Vehicles
  syncVehicles: vehicles.syncVehicles,
  loadVehicleCache: vehicles.loadVehicleCache,
  isKnownVehicle: vehicles.isKnownVehicle,
  getCostCode: vehicles.getCostCode,
  
  // History & Fuel
  insertHistory: history.insertHistory,
  upsertLatest: history.upsertLatest,
  insertGeozoneEvent: history.insertGeozoneEvent,
  getLatestFuelReading: history.getLatestFuelReading,
  getLatestFuelBefore: history.getLatestFuelBefore,
  
  // Supabase Sessions
  insertFillSession: sessions.insertFillSession,
  insertTheftSession: sessions.insertTheftSession,
  getOngoingSession: sessions.getOngoingSession,
  insertOperatingSession: sessions.insertOperatingSession,
  closeOperatingSession: sessions.closeOperatingSession,
  
  // Zones (NEW)
  upsertZone: zones.upsertZone,
  getAllZones: zones.getAllZones,
  getZoneById: zones.getZoneById,
  deleteStaleZones: zones.deleteStaleZones,
  
  // Trips (NEW)
  upsertTrip: trips.upsertTrip,
  getActiveTripForVehicle: trips.getActiveTripForVehicle,
  getTripById: trips.getTripById,
  updateTripStatus: trips.updateTripStatus,
  getAllActiveTrips: trips.getAllActiveTrips,
  deleteStaleTrips: trips.deleteStaleTrips,
  
  // Trip Events (NEW)
  insertTripZoneEvent: tripEvents.insertTripZoneEvent,
  getTripZoneEvents: tripEvents.getTripZoneEvents,
  getLatestEventPerZone: tripEvents.getLatestEventPerZone,
  getAllTripsProgress: tripEvents.getAllTripsProgress,
  
  // Fuel (existing system)
  upsertFuelStop,
};