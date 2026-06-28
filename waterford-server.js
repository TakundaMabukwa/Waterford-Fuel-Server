require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./waterford-db');
const { createClient } = require('./waterford-ws-client');

const app = express();
app.use(express.json());
app.use(cors());

app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    service: 'Waterford Fuel Server'
  });
});

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
