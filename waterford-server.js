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
