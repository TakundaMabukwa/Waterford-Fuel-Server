require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const db = require('./waterford-db');
const { createClient } = require('./waterford-ws-client');
const { syncFuelStops } = require('./waterford-geozone');

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

const start = async () => {
  try {
    await db.init();

    await db.syncVehicles();
    await syncFuelStops();

    cron.schedule('0 * * * *', async () => {
      console.log('[cron] Running hourly vehicle sync');
      await db.syncVehicles();
    });

    cron.schedule('0 * * * *', async () => {
      console.log('[cron] Running hourly fuel stops sync');
      await syncFuelStops();
    });

    const wsClient = createClient(process.env.WEBSOCKET_URL || 'ws://209.38.217.58:8093');

    const PORT = process.env.PORT || 4000;
    app.listen(PORT, () => {
      console.log(`[server] Running on port ${PORT}`);
      wsClient.connect();
    });

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
