const db = require('../../waterford-db');
const { findZone } = require('../../waterford-geozone');
const { broadcastTripEvent } = require('./broadcaster');

class TripTracker {
  constructor() {
    // plate -> { tripId, stopStates: Map<zoneId, {inZone, sequence, zoneName}> }
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

    // Check each stop point zone for entry/exit
    for (let i = 0; i < stops.length; i++) {
      const stop = stops[i];
      const zoneId = stop.id; // "fuel:14"
      
      // Get zone coordinates from stop data
      const coordinates = stop.geozone_coordinates || (stop.coordinates ? JSON.parse(stop.coordinates) : null);
      if (!coordinates || !Array.isArray(coordinates) || coordinates.length < 3) continue;

      // Check if point is in zone
      const inZone = this.pointInZone(lat, lon, coordinates);
      
      const stateKey = zoneId;
      const wasInZone = tracking.stopStates.get(stateKey)?.inZone || false;

      if (!wasInZone && inZone) {
        // ENTER event
        await this.logEvent(tripId, plate, stop, zoneId, 'ENTER', msg.loc_time, lat, lon, i);
        tracking.stopStates.set(stateKey, { inZone: true, sequence: i, zoneName: stop.name });
      } else if (wasInZone && !inZone) {
        // EXIT event
        await this.logEvent(tripId, plate, stop, zoneId, 'EXIT', msg.loc_time, lat, lon, i);
        tracking.stopStates.set(stateKey, { inZone: false, sequence: i, zoneName: stop.name });
        await this.checkTripCompletion(tripId, plate);
      }
    }
  }

  pointInZone(lat, lon, coordinates) {
    try {
      const closedRing = [...coordinates, coordinates[0]];
      const turfPolygon = {
        type: 'Feature',
        geometry: { type: 'Polygon', coordinates: [closedRing] },
        properties: {}
      };
      const point = [lon, lat];
      const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;
      return booleanPointInPolygon(point, turfPolygon);
    } catch {
      return false;
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
    
    // Check if all stops have been exited
    const exitedZones = new Set(
      events.filter(e => e.event_type === 'EXIT').map(e => e.zone_id)
    );
    
    const allCompleted = stops.every(stop => exitedZones.has(stop.id));
    
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