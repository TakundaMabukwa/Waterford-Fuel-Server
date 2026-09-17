const dbCore = require('./src/db/index');
const vehicles = require('./src/db/vehicles');
const history = require('./src/db/history');
const sessions = require('./src/db/sessions');
const zones = require('./src/db/zones');
const trips = require('./src/db/trips');
const tripEvents = require('./src/db/tripEvents');
const schema = require('./src/db/schema');

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
};