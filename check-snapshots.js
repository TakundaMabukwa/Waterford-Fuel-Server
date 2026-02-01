const { supabase } = require('./supabase-client');

async function checkSnapshots() {
  console.log('🔍 Checking Activity Snapshots...\n');
  
  try {
    // Check if snapshots table exists and has data
    const { data: snapshots, error } = await supabase
      .from('energy_rite_activity_snapshots')
      .select('*')
      .order('snapshot_time', { ascending: false })
      .limit(5);
    
    if (error) {
      console.log('❌ Error querying snapshots:', error.message);
      return;
    }
    
    console.log(`📊 Found ${snapshots?.length || 0} recent snapshots`);
    
    if (snapshots && snapshots.length > 0) {
      console.log('\n📋 Recent Snapshots:');
      snapshots.forEach((snapshot, index) => {
        console.log(`  ${index + 1}. ${snapshot.snapshot_date} ${snapshot.time_slot} - ${snapshot.active_vehicles}/${snapshot.total_vehicles} vehicles`);
        
        if (snapshot.vehicles_data && snapshot.vehicles_data.length > 0) {
          console.log(`     Sample vehicle: ${snapshot.vehicles_data[0].plate} - ${snapshot.vehicles_data[0].fuel_level}L`);
        }
      });
    } else {
      console.log('⚠️  No snapshots found. Taking a manual snapshot...');
      
      // Take a manual snapshot
      const activitySnapshots = require('./helpers/activity-snapshots');
      const newSnapshot = await activitySnapshots.takeSnapshot();
      console.log('✅ Manual snapshot taken:', newSnapshot.snapshot_time);
    }
    
    // Check vehicle lookup table
    console.log('\n🚗 Checking Vehicle Lookup...');
    const { data: vehicles } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate, cost_code')
      .limit(5);
    
    console.log(`Found ${vehicles?.length || 0} vehicles in lookup table`);
    if (vehicles && vehicles.length > 0) {
      vehicles.forEach(v => {
        console.log(`  ${v.plate} - Cost Code: ${v.cost_code}`);
      });
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkSnapshots();