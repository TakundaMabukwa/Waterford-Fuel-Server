require('dotenv').config();
const express = require('express');
const cors = require('cors');
const EnergyRiteWebSocketClient = require('./websocket-client');

const app = express();

app.use(express.json());
app.use(cors());

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    service: 'Waterford Fuel Server'
  });
});

// Initialize WebSocket client
const wsClient = new EnergyRiteWebSocketClient(process.env.WEBSOCKET_URL);

// Start server
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🚀 Waterford Fuel server running on port ${PORT}`);
  console.log(`📡 WebSocket URL: ${process.env.WEBSOCKET_URL}`);
  console.log(`💾 Supabase URL: ${process.env.SUPABASE_URL}`);
  
  // Connect to WebSocket for real-time data
  wsClient.connect();
});

// Graceful shutdown
process.on('SIGTERM', () => {
  wsClient.close();
  process.exit(0);
});

process.on('SIGINT', () => {
  wsClient.close();
  process.exit(0);
});

module.exports = { app, wsClient };
