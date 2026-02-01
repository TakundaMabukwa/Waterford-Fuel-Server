// Add this method to websocket-client.js

async getFuelDataFallback(plate, quality) {
  try {
    const response = await axios.get('http://64.227.138.235:3000/api/energy-rite/vehicles', {
      timeout: 3000
    });
    
    if (response.data?.success && response.data?.data) {
      // First try matching by quality (more accurate)
      let vehicleInfo = response.data.data.find(v => v.quality === quality);
      
      // Fallback to branch matching if quality match fails
      if (!vehicleInfo) {
        vehicleInfo = response.data.data.find(v => v.branch === plate);
      }
      
      if (vehicleInfo) {
        console.log(`🔄 External API fuel data for ${plate} (matched by ${vehicleInfo.quality === quality ? 'quality' : 'branch'}): ${vehicleInfo.fuel_probe_1_level}L`);
        return {
          fuel_probe_1_level: vehicleInfo.fuel_probe_1_level,
          fuel_probe_1_level_percentage: vehicleInfo.fuel_probe_1_level_percentage,
          fuel_probe_1_volume_in_tank: vehicleInfo.fuel_probe_1_volume_in_tank,
          fuel_probe_1_temperature: vehicleInfo.fuel_probe_1_temperature
        };
      }
    }
  } catch (error) {
    console.log(`⚠️ External API failed for ${plate}:`, error.message);
  }
  return { fuel_probe_1_level: null };
}