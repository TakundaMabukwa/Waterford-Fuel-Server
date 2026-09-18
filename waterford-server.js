require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const db = require('./waterford-db');
const { createClient } = require('./waterford-ws-client');
const { syncFuelStops } = require('./waterford-geozone');
const { syncZones, syncTrips } = require('./waterford-trips');
const { createWSServer } = require('./waterford-ws-server');

const app = express();
app.use(express.json());
app.use(cors());

// Routes
app.use('/api/energy-rite', require('./routes/energy-rite-data'));
app.use('/api/energy-rite/vehicles', require('./routes/energy-rite-vehicles'));
app.use('/api/energy-rite/reports', require('./routes/energy-rite-reports'));
app.use('/api/energy-rite/report-storage', require('./routes/energy-rite-report-storage'));
app.use('/api/energy-rite/fuel-analysis', require('./routes/energy-rite-fuel-analysis'));
app.use('/api/energy-rite/emails', require('./routes/energy-rite-emails'));
app.use('/api/energy-rite/excel-reports', require('./routes/energy-rite-excel-reports'));
app.use('/api/energy-rite/activity-reports', require('./routes/energy-rite-activity-reports'));
app.use('/api/energy-rite/activity-excel-reports', require('./routes/energy-rite-activity-excel-reports'));
app.use('/api/energy-rite/monitoring', require('./routes/energy-rite-monitoring'));
app.use('/api/energy-rite/executive-dashboard', require('./routes/energy-rite-executive-dashboard'));
app.use('/api/energy-rite/enhanced-executive-dashboard', require('./routes/enhanced-executive-dashboard'));
app.use('/api/energy-rite/report-distribution', require('./routes/energy-rite-report-distribution'));
app.use('/api/energy-rite/fuel-fills', require('./routes/energy-rite-fuel-fills'));
app.use('/api/energy-rite/cumulative-snapshots', require('./routes/energy-rite-cumulative-snapshots'));
app.use('/api/cost-center-access', require('./routes/cost-center-access'));

app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    service: 'Waterford Fuel Server'
  });
});

app.post('/api/vehicles/sync', async (req, res) => {
  try {
    const synced = await db.syncVehicles();
    const { rows } = await db.query('SELECT COUNT(*) as total FROM vehicles');
    res.json({
      synced,
      total: parseInt(rows[0].total),
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/fuel-stops/sync', async (req, res) => {
  try {
    const count = await syncFuelStops();
    const { rows } = await db.query('SELECT COUNT(*) as total FROM fuel_stops');
    res.json({
      synced: count,
      total: parseInt(rows[0].total),
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/fuel-stops', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM fuel_stops ORDER BY name');
    res.json({ count: rows.length, stops: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Vehicle latest endpoints
app.get('/api/vehicles', async (req, res) => {
  try {
    const { rows } = await db.query(
      `SELECT plate, cost_code, speed, latitude, longitude, loc_time, mileage,
              status, message_type,
              fuel_probe_1_level, fuel_probe_1_volume_in_tank,
              fuel_probe_1_temperature, fuel_probe_1_level_percentage,
              fuel_probe_2_level, fuel_probe_2_volume_in_tank,
              fuel_probe_2_temperature, fuel_probe_2_level_percentage,
              item_installed, geozone, driver_name, updated_at
       FROM vehicle_latest
       ORDER BY plate`
    );
    res.json({ count: rows.length, vehicles: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/vehicles/:plate', async (req, res) => {
  try {
    const { rows } = await db.query(
      `SELECT * FROM vehicle_latest WHERE plate = $1`,
      [req.params.plate.toUpperCase()]
    );
    if (!rows.length) return res.status(404).json({ error: 'Vehicle not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// NEW: Trip monitoring endpoints
app.post('/api/trips/sync', async (req, res) => {
  try {
    const synced = await syncTrips();
    const { rows } = await db.query('SELECT COUNT(*) as total FROM trips');
    res.json({
      synced,
      total: parseInt(rows[0].total),
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Batch progress for all active trips (used by dashboard)
app.get('/api/trips/progress/all', async (req, res) => {
  try {
    const allProgress = await db.getAllTripsProgress();
    res.json(allProgress);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/trips/:tripId/progress', async (req, res) => {
  try {
    const trip = await db.getTripById(req.params.tripId);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });

    const events = await db.getLatestEventPerZone(req.params.tripId);
    const stops = trip.selected_stop_points || [];

    // Match by zone_name (not zone_id, since trip stops use fuel:X format)
    const exitedZoneNames = new Set(
      events.filter(e => e.event_type === 'EXIT').map(e => e.zone_name)
    );
    const enteredZoneNames = new Set(
      events.filter(e => e.event_type === 'ENTER').map(e => e.zone_name)
    );

    const completed = [];
    let current = null;
    const remaining = [];

    for (const stop of stops) {
      const stopName = stop.name;
      const latestEvent = events.find(e => e.zone_name === stopName);
      
      if (exitedZoneNames.has(stopName)) {
        completed.push({ ...stop, exitTime: latestEvent?.loc_time });
      } else if (enteredZoneNames.has(stopName)) {
        // Vehicle currently in this zone
        current = { ...stop, enterTime: latestEvent?.loc_time };
      } else {
        remaining.push(stop);
      }
    }

    const progress = stops.length > 0 ? Math.round((completed.length / stops.length) * 100) : 0;

    res.json({
      tripId: req.params.tripId,
      progress,
      completed,
      current,
      remaining,
      totalStops: stops.length,
      tripStatus: trip.status
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/trips/:tripId/events', async (req, res) => {
  try {
    const events = await db.getTripZoneEvents(req.params.tripId);
    res.json({ events });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/trips/:tripId', async (req, res) => {
  try {
    const trip = await db.getTripById(req.params.tripId);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    res.json(trip);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const start = async () => {
  try {
    await db.init();

    await db.syncVehicles();
    await syncZones();
    await syncFuelStops();
    await syncTrips();

    // Hourly cron: vehicles, zones, fuel stops, trips
    cron.schedule('0 * * * *', async () => {
      console.log('[cron] Running hourly sync: vehicles, zones, fuel stops, trips');
      await db.syncVehicles();
      await syncZones();
      await syncFuelStops();
      await syncTrips();
    });

    const wsClient = createClient(process.env.WEBSOCKET_URL || 'ws://209.38.217.58:8093');

    const PORT = process.env.PORT || 4000;
    const server = app.listen(PORT, () => {
      console.log(`[server] Running on port ${PORT}`);
      wsClient.connect();
    });

    // Create WebSocket server for frontend trip updates
    createWSServer(server);

    const shutdown = (signal) => {
      console.log(`[server] ${signal} received, shutting down`);
      wsClient.close();
      db.close().then(() => process.exit(0));
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

  } catch (err) {
    console.error('[server] Failed to start:', err.message);
    process.exit(1);
  }
};

start();