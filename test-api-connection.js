#!/usr/bin/env node
require('dotenv').config();
const axios = require('axios');

async function testAPIConnection() {
  console.log('🔍 Testing external API connection...\n');
  
  try {
    console.log('📡 Connecting to http://64.227.138.235:3000/api/energy-rite/vehicles');
    
    const response = await axios.get('http://64.227.138.235:3000/api/energy-rite/vehicles', {
      timeout: 5000 // 5 second timeout
    });
    
    console.log('✅ API connection successful');
    console.log(`📊 Found ${response.data.data.length} vehicles`);
    
    // Check if MILNERTON exists
    const milnerton = response.data.data.find(v => v.branch === 'MILNERTON');
    if (milnerton) {
      console.log(`🏢 MILNERTON found with fuel data:`);
      console.log(`   Fuel Level: ${milnerton.fuel_probe_1_level}L`);
      console.log(`   Fuel Percentage: ${milnerton.fuel_probe_1_level_percentage}%`);
      console.log(`   Temperature: ${milnerton.fuel_probe_1_temperature}°C`);
    } else {
      console.log('⚠️ MILNERTON not found in API response');
    }
    
  } catch (error) {
    console.error('❌ API connection failed:', error.message);
    if (error.code === 'ECONNREFUSED') {
      console.log('💡 The external API server might be down or unreachable');
    } else if (error.code === 'ETIMEDOUT') {
      console.log('💡 Connection timed out - API server is not responding');
    }
  }
}

testAPIConnection();