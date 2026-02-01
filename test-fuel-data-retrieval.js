const axios = require('axios');

async function testFuelDataRetrieval() {
  console.log('🧪 Testing Fuel Data Retrieval...\n');
  
  try {
    // Test the endpoint that WebSocket uses
    console.log('📡 Testing endpoint: http://64.227.138.235:3000/api/energy-rite/vehicles');
    const response = await axios.get('http://64.227.138.235:3000/api/energy-rite/vehicles');
    const vehicles = response.data.data;
    
    console.log(`✅ Found ${vehicles.length} vehicles\n`);
    
    // Test specific vehicles that had missing data
    const testVehicles = ['FOURWAYS', 'JANSENPARK', 'MILNERTON', 'BRACKENHUR', 'LAMBTON'];
    
    console.log('🔍 Testing fuel data for vehicles with recent sessions:\n');
    
    testVehicles.forEach(plateName => {
      const vehicle = vehicles.find(v => v.branch === plateName);
      
      if (vehicle) {
        const fuelLevel = parseFloat(vehicle.fuel_probe_1_level) || parseFloat(vehicle.fuel_level) || 0;
        const fuelPercentage = parseFloat(vehicle.fuel_probe_1_level_percentage) || parseFloat(vehicle.fuel_percentage) || 0;
        const volume = parseFloat(vehicle.fuel_probe_1_volume_in_tank) || 0;
        const temperature = parseFloat(vehicle.fuel_probe_1_temperature) || 0;
        
        console.log(`🚛 ${plateName}:`);
        console.log(`   Fuel Level: ${fuelLevel}L`);
        console.log(`   Fuel Percentage: ${fuelPercentage}%`);
        console.log(`   Volume in Tank: ${volume}L`);
        console.log(`   Temperature: ${temperature}°C`);
        console.log(`   Engine Status: ${vehicle.engine_status || 'N/A'}`);
        console.log(`   Is Active: ${vehicle.is_active}`);
        console.log(`   Last Activity: ${vehicle.last_activity_time || 'N/A'}`);
        console.log('');
      } else {
        console.log(`❌ ${plateName}: Not found in endpoint data`);
      }
    });
    
    // Test the matching logic
    console.log('🔗 Testing WebSocket to Endpoint Matching:\n');
    
    // Simulate WebSocket message
    const mockWebSocketMessage = {
      Plate: 'FOURWAYS',
      DriverName: 'PTO ON / ENGINE ON'
    };
    
    const matchedVehicle = vehicles.find(v => v.branch === mockWebSocketMessage.Plate);
    
    if (matchedVehicle) {
      console.log(`✅ Match found for ${mockWebSocketMessage.Plate}:`);
      console.log(`   Branch: ${matchedVehicle.branch}`);
      console.log(`   Company: ${matchedVehicle.company}`);
      console.log(`   Cost Code: ${matchedVehicle.cost_code}`);
      console.log(`   Fuel Data Available: ${matchedVehicle.fuel_probe_1_level ? 'YES' : 'NO'}`);
      console.log(`   Driver Name in Endpoint: ${matchedVehicle.drivername || 'N/A'}`);
    } else {
      console.log(`❌ No match found for ${mockWebSocketMessage.Plate}`);
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.response?.data || error.message);
  }
}

testFuelDataRetrieval();