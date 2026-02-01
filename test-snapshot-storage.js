// Test script to check if cost codes are properly stored in snapshots
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('❌ Missing Supabase environment variables');
  console.error('Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in your .env file');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

async function testSnapshotStorage() {
  try {
    console.log('🔍 Testing Snapshot Storage with Cost Codes');
    console.log('=' .repeat(60));
    
    // Get the latest snapshots to check if cost codes are being stored
    console.log('📸 Fetching latest snapshots...');
    
    const { data: snapshots, error } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .order('snapshot_date', { ascending: false })
      .limit(10);
    
    if (error) {
      throw new Error(`Database error: ${error.message}`);
    }
    
    if (!snapshots || snapshots.length === 0) {
      console.log('⚠️  No snapshots found in database');
      return;
    }
    
    console.log(`\n📊 Found ${snapshots.length} recent snapshots`);
    
    // Check if any snapshots have cost codes in their snapshot_data
    const snapshotsWithCostCodes = snapshots.filter(snapshot => {
      const data = snapshot.snapshot_data;
      return data && Object.values(data).some(vehicleData => 
        vehicleData && vehicleData.cost_code
      );
    });
    
    console.log(`\n💰 Snapshots with cost codes: ${snapshotsWithCostCodes.length}/${snapshots.length}`);
    
    if (snapshotsWithCostCodes.length > 0) {
      const latestSnapshot = snapshotsWithCostCodes[0];
      console.log('\n🎯 Latest snapshot with cost codes:');
      console.log(`   Date: ${latestSnapshot.snapshot_date}`);
      console.log(`   Type: ${latestSnapshot.snapshot_type}`);
      
      // Show sample vehicles with cost codes
      const vehicleData = latestSnapshot.snapshot_data;
      const vehiclesWithCostCodes = Object.entries(vehicleData)
        .filter(([plate, data]) => data && data.cost_code)
        .slice(0, 5);
      
      console.log('\n🚛 Sample vehicles with cost codes:');
      vehiclesWithCostCodes.forEach(([plate, data]) => {
        console.log(`   ${plate}:`);
        console.log(`     Cost Code: ${data.cost_code}`);
        console.log(`     Fuel Level: ${data.fuel_level}L`);
        console.log(`     Engine Status: ${data.engine_status || 'Unknown'}`);
      });
      
      // Cost code distribution in this snapshot
      const costCodeStats = {};
      Object.values(vehicleData).forEach(data => {
        if (data && data.cost_code) {
          costCodeStats[data.cost_code] = (costCodeStats[data.cost_code] || 0) + 1;
        }
      });
      
      console.log('\n📈 Cost code distribution in latest snapshot:');
      Object.entries(costCodeStats)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .forEach(([code, count]) => {
          console.log(`   ${code}: ${count} vehicles`);
        });
      
      console.log('\n✅ Cost codes are being properly stored in snapshots!');
    } else {
      console.log('\n⚠️  No cost codes found in snapshot data');
      console.log('   This might mean:');
      console.log('   1. Snapshots were captured before cost code enhancement');
      console.log('   2. No vehicles had operating sessions during snapshot');
      console.log('   3. Cost code lookup failed during capture');
      
      // Show sample snapshot structure
      if (snapshots[0]) {
        console.log('\n📋 Sample snapshot structure:');
        const sampleData = snapshots[0].snapshot_data;
        const sampleVehicles = Object.keys(sampleData).slice(0, 3);
        sampleVehicles.forEach(plate => {
          console.log(`   ${plate}:`, JSON.stringify(sampleData[plate], null, 4));
        });
      }
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    throw error;
  }
}

// Run the test
testSnapshotStorage()
  .then(() => {
    console.log('\n🎉 Snapshot storage test completed!');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ Test failed:', error);
    process.exit(1);
  });