const db = require('../../waterford-db');
const { findZone } = require('../../waterford-geozone');
const { broadcastTripEvent } = require('./broadcaster');

// Normalize: lowercase, trim, collapse whitespace
const norm = (s) => (s || '').toLowerCase().trim().replace(/\s+/g, ' ');

// Fuzzy match: checks if two names overlap (ilike with wildcards)
const namesMatch = (a, b) => {
  const na = norm(a);
  const nb = norm(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  // Check if first significant word matches
  const wordsA = na.split(/\s+/);
  const wordsB = nb.split(/\s+/);
  // At least one word must match
  return wordsA.some(w => w.length > 3 && wordsB.includes(w));
};

class TripTracker {
  constructor() {
    // plate -> { tripId, stopStates: Map<stopKey, {inZone, sequence, zoneId, zoneName}> }
    this.tracking = new Map();
  }

  async processMessage(plate, msg, decoded) {
    // Get active trip for this vehicle
    const trip = await db.getActiveTripForVehicle(plate);
    if (!trip || !trip.selected_stop_points?.length) return;

    const tripId = trip.trip_id;
    const stops = trip.selected_stop_points;
    
    // Initialize tracking for this plate if needed
    if (!this.tracking.has(plate)) {
      this.tracking.set(plate, { tripId, stopStates: new Map() });
    }
    const tracking = this.tracking.get(plate);
    
    // If trip changed, reset tracking
    if (tracking.tripId !== tripId) {
      tracking.tripId = tripId;
      tracking.stopStates.clear();
    }

    const lat = msg.latitude;
    const lon = msg.longitude;
    if (!lat || !lon) return;

    // Use findZone to check if vehicle is in ANY zone (same as fuel system, turf-based)
    const currentZone = await findZone(lat, lon);
    
    // Find matching stop for current zone (fuzzy match)
    const matchedStop = currentZone
      ? stops.find(s => namesMatch(s.name, currentZone.name) || namesMatch(s.name2, currentZone.name))
      : null;
    
    // If in a zone that matches a stop point, handle entry
    if (currentZone && matchedStop) {
      const stopKey = matchedStop.name; // use stop name as key
      const zoneId = currentZone.id;
      
      // Initialize state if not exists
      if (!tracking.stopStates.has(stopKey)) {
        const sequence = stops.findIndex(s => s.name === matchedStop.name);
        tracking.stopStates.set(stopKey, { 
          inZone: false, 
          sequence, 
          zoneName: matchedStop.name, 
          zoneId: null 
        });
      }
      
      const state = tracking.stopStates.get(stopKey);
      const wasInZone = state.inZone;
      
      // Update zoneId when we first see the zone
      if (zoneId) state.zoneId = zoneId;
      
      if (!wasInZone) {
        // ENTER event
        await this.logEvent(tripId, plate, matchedStop, zoneId, 'ENTER', msg.loc_time, lat, lon, state.sequence);
        state.inZone = true;
      }
    }
    
    // Check for exits: any tracked stop zone that vehicle is NO longer in
    for (const [stopKey, state] of tracking.stopStates) {
      const stillInZone = currentZone && matchedStop && matchedStop.name === state.zoneName;
      if (state.inZone && !stillInZone) {
        // EXIT event
        const stop = stops.find(s => s.name === state.zoneName);
        await this.logEvent(tripId, plate, stop, state.zoneId, 'EXIT', msg.loc_time, lat, lon, state.sequence);
        state.inZone = false;
        await this.checkTripCompletion(tripId, plate);
      }
    }
  }

  async logEvent(tripId, plate, stop, zoneId, eventType, locTime, latitude, longitude, sequence) {
    const event = {
      trip_id: tripId,
      plate,
      zone_id: zoneId,
      zone_name: stop.name,
      event_type: eventType,
      loc_time: locTime,
      latitude,
      longitude,
      sequence_order: sequence
    };
    
    await db.insertTripZoneEvent(event);
    
    // Broadcast to frontend WebSocket
    broadcastTripEvent({
      type: 'TRIP_ZONE_EVENT',
      trip_id: tripId,
      plate,
      zone_id: zoneId,
      zone_name: stop.name,
      event: eventType,
      timestamp: locTime,
      sequence: sequence,
      latitude,
      longitude
    });
    
    console.log(`[trip] ${eventType}: ${plate} ${eventType} zone "${stop.name}" (trip: ${tripId}, seq: ${sequence})`);
  }

  async checkTripCompletion(tripId, plate) {
    const trip = await db.getTripById(tripId);
    if (!trip) return;

    const stops = trip.selected_stop_points;
    const events = await db.getLatestEventPerZone(tripId);
    
    // Check if all stops have been exited - match by zone_name (not ID)
    const exitedZoneNames = new Set(
      events.filter(e => e.event_type === 'EXIT').map(e => e.zone_name)
    );
    
    // All stops completed if every stop name has an EXIT event (fuzzy match)
    const allCompleted = stops.every(stop => 
      [...exitedZoneNames].some(name => namesMatch(name, stop.name))
    );
    
    if (allCompleted && trip.status !== 'delivered') {
      await db.updateTripStatus(tripId, 'delivered');
      console.log(`[trip] Trip ${tripId} auto-completed to 'delivered'`);
      
      // Broadcast completion
      broadcastTripEvent({
        type: 'TRIP_COMPLETED',
        trip_id: tripId,
        plate,
        timestamp: new Date().toISOString()
      });
    }
  }

  clearTracking(plate) {
    this.tracking.delete(plate);
  }
}

module.exports = { TripTracker };
