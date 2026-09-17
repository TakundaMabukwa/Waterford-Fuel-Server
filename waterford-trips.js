const trips = require('./src/trips');
const sync = require('./src/sync');

module.exports = {
  // Sync functions
  syncZones: sync.syncZones,
  syncTrips: sync.syncTrips,
  
  // Trip tracking
  TripTracker: trips.TripTracker,
  setWSServer: trips.setWSServer,
  broadcastTripEvent: trips.broadcastTripEvent,
};