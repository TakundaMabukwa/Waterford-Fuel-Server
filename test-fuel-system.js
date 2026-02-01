// Load test environment
require('dotenv').config({ path: '.env.test' });

const express = require('express');
const WebSocket = require('ws');
const { detectFuelFill } = require('./helpers/fuel-fill-detector');

const app = express();
const PORT = process.env.PORT || 4000;

console.log('🧪 Starting Fuel Detection Test Server...');
console.log(`📡 Connecting to: ${process.env.WEBSOCKET_URL}`);

// WebSocket connection to mock server
function connectToWebSocket() {
  const ws = new WebSocket(process.env.WEBSOCKET_URL);
  
  ws.on('open', () => {
    console.log('✅ Connected to mock WebSocket server');
    console.log('🔍 Monitoring for fuel fill events...\n');
  });
  
  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
      // Log incoming data
      console.log(`📥 ${message.Plate}: ${message.DriverName || 'Normal'} - Fuel: ${message.fuel_probe_1_level}L`);
      
      // Test fuel fill detection
      if (message.DriverName) {
        const result = await detectFuelFill(
          message.Plate, 
          message.fuel_probe_1_level, 
          message.DriverName,
          message  // Pass complete fuel data
        );
        
        if (result.isFill) {
          console.log(`🎯 FUEL FILL DETECTED: ${message.Plate}`);
          console.log(`   Method: ${result.fillDetails.detectionMethod}`);
          console.log(`   Amount: ${result.fillDetails.fillAmount}L`);
          console.log(`   Volume: ${message.fuel_probe_1_volume_in_tank}L`);
          console.log(`   Percentage: ${message.fuel_probe_1_level_percentage}%`);
          console.log(`   Temperature: ${message.fuel_probe_1_temperature}°C\n`);
        }
      }
      
    } catch (error) {
      console.error('❌ Error processing message:', error.message);
    }
  });
  
  ws.on('close', () => {
    console.log('❌ WebSocket connection closed. Reconnecting in 5s...');
    setTimeout(connectToWebSocket, 5000);
  });
  
  ws.on('error', (error) => {
    console.error('❌ WebSocket error:', error.message);
  });
}

// Start WebSocket connection
connectToWebSocket();

// Start HTTP server
app.listen(PORT, () => {
  console.log(`🚀 Test server running on port ${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n👋 Shutting down test server...');
  process.exit(0);
});