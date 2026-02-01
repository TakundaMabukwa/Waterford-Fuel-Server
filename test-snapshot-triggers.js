require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('❌ Missing Supabase environment variables');
  console.error('Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in your .env file');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Enhanced snapshot function with cost code integration
async function captureSnapshotWithCostCode(snapshotType = 'MANUAL_TEST') {
  console.log(`\n🔄 Triggering ${snapshotType} snapshot...`);
  
  try {
    // 1. Get cost code mapping from operating sessions
    console.log('📋 Step 1: Fetching cost code mapping...');
    const { data: sessions, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('site_id, cost_code')
      .not('cost_code', 'is', null)
      .not('cost_code', 'eq', '');

    if (sessionError) {
      console.log('❌ Error fetching operating sessions:', sessionError.message);
      return false;
    }

    // Create site mapping
    const siteMapping = {};
    sessions.forEach(session => {
      if (session.site_id && session.cost_code) {
        siteMapping[session.site_id] = session.cost_code;
      }
    });

    console.log(`✅ Found ${sessions.length} sessions with cost codes`);
    console.log(`🗺️  Site mapping created: ${Object.keys(siteMapping).length} sites`);
    if (Object.keys(siteMapping).length > 0) {
      console.log('   Sample mappings:', Object.entries(siteMapping).slice(0, 3));
    }

    // 2. Get current vehicle/fuel data
    console.log('\n📋 Step 2: Fetching current vehicle data...');
    const { data: vehicles, error: vehicleError } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .order('timestamp', { ascending: false })
      .limit(10);

    if (vehicleError) {
      console.log('❌ Error fetching vehicle data:', vehicleError.message);
      return false;
    }

    console.log(`✅ Found ${vehicles.length} recent vehicle records`);

    // Filter for active/recent vehicles (last 4 hours)
    const cutoffTime = new Date(Date.now() - 4 * 60 * 60 * 1000);
    const activeVehicles = vehicles.filter(vehicle => 
      new Date(vehicle.timestamp) > cutoffTime ||
      vehicle.engine_status === 'ON' ||
      vehicle.engine_status === 'IDLING'
    );

    console.log(`🚗 ${activeVehicles.length} active vehicles in last 4 hours`);

    // 3. Create snapshot entries for each active vehicle
    console.log('\n📋 Step 3: Creating snapshot entries...');
    let successCount = 0;
    let totalFuel = 0;

    for (const vehicle of activeVehicles) {
      try {
        // Get cost code for this vehicle's site
        const costCode = siteMapping[vehicle.site_id] || null;
        
        // Calculate fuel data
        const fuelLevel = vehicle.fuel_level || 0;
        const fuelVolume = vehicle.fuel_volume || 0;
        totalFuel += fuelVolume;

        // Create enhanced snapshot data
        const snapshotData = {
          cost_code: costCode,
          fuel_level: fuelLevel,
          fuel_volume: fuelVolume,
          engine_status: vehicle.engine_status || 'UNKNOWN',
          vehicle_plate: vehicle.plate_number || 'UNKNOWN',
          site_id: vehicle.site_id || null,
          timestamp: vehicle.timestamp,
          original_record_id: vehicle.id
        };

        // Insert snapshot
        const { data: snapshot, error: insertError } = await supabase
          .from('energy_rite_daily_snapshots')
          .insert({
            branch: vehicle.branch || 'DEFAULT_BRANCH',
            company: vehicle.company || 'DEFAULT_COMPANY', 
            snapshot_date: new Date().toISOString().split('T')[0],
            snapshot_time: new Date(),
            snapshot_type: snapshotType,
            snapshot_data: snapshotData
          })
          .select();

        if (insertError) {
          console.log(`❌ Failed to insert snapshot for vehicle ${vehicle.plate_number}:`, insertError.message);
        } else {
          console.log(`✅ Snapshot created for ${vehicle.plate_number} (${fuelVolume.toFixed(1)}L) - Cost Code: ${costCode || 'N/A'}`);
          successCount++;
        }

      } catch (error) {
        console.log(`❌ Error processing vehicle ${vehicle.plate_number}:`, error.message);
      }
    }

    // 4. Summary
    console.log(`\n📊 SNAPSHOT SUMMARY:`);
    console.log(`   ✅ Successful snapshots: ${successCount}/${activeVehicles.length}`);
    console.log(`   ⛽ Total fuel captured: ${totalFuel.toFixed(1)}L`);
    console.log(`   🏷️  Cost codes available: ${Object.keys(siteMapping).length} sites`);
    console.log(`   🕒 Snapshot type: ${snapshotType}`);

    return successCount > 0;

  } catch (error) {
    console.error('❌ Snapshot capture failed:', error.message);
    return false;
  }
}

// Function to verify snapshots were created correctly
async function verifySnapshots() {
  console.log('\n🔍 VERIFYING CREATED SNAPSHOTS...');
  
  try {
    // Get recent snapshots
    const { data: snapshots, error } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .order('snapshot_time', { ascending: false })
      .limit(10);

    if (error) {
      console.log('❌ Error fetching snapshots:', error.message);
      return;
    }

    console.log(`\n📋 Found ${snapshots.length} recent snapshots:`);
    
    snapshots.forEach((snapshot, index) => {
      const data = snapshot.snapshot_data || {};
      console.log(`\n${index + 1}. Snapshot ID: ${snapshot.id}`);
      console.log(`   📅 Date/Time: ${snapshot.snapshot_date} ${new Date(snapshot.snapshot_time).toLocaleTimeString()}`);
      console.log(`   🏷️  Type: ${snapshot.snapshot_type}`);
      console.log(`   🏢 Branch: ${snapshot.branch}`);
      console.log(`   💼 Cost Code: ${data.cost_code || 'N/A'}`);
      console.log(`   🚗 Vehicle: ${data.vehicle_plate || 'N/A'}`);
      console.log(`   ⛽ Fuel: ${data.fuel_volume || 0}L (${data.fuel_level || 0}%)`);
      console.log(`   🔧 Engine: ${data.engine_status || 'UNKNOWN'}`);
    });

    // Count snapshots with cost codes
    const withCostCodes = snapshots.filter(s => s.snapshot_data?.cost_code);
    console.log(`\n📊 VERIFICATION RESULTS:`);
    console.log(`   Total snapshots: ${snapshots.length}`);
    console.log(`   With cost codes: ${withCostCodes.length}`);
    console.log(`   Cost code coverage: ${snapshots.length > 0 ? ((withCostCodes.length/snapshots.length)*100).toFixed(1) : 0}%`);

  } catch (error) {
    console.error('❌ Verification failed:', error.message);
  }
}

// Test sequence
async function runSnapshotTests() {
  console.log('🚀 TESTING ENHANCED SNAPSHOT SYSTEM WITH COST CODES');
  console.log('='.repeat(60));

  // Test 1: Manual snapshot
  console.log('\n🧪 TEST 1: Manual Snapshot Trigger');
  const test1Success = await captureSnapshotWithCostCode('MANUAL_TEST_1');
  
  // Wait a moment
  await new Promise(resolve => setTimeout(resolve, 2000));

  // Test 2: Scheduled-style snapshot  
  console.log('\n🧪 TEST 2: Scheduled Snapshot Simulation');
  const test2Success = await captureSnapshotWithCostCode('SCHEDULED_TEST');
  
  // Wait a moment
  await new Promise(resolve => setTimeout(resolve, 2000));

  // Test 3: Another manual snapshot
  console.log('\n🧪 TEST 3: Second Manual Snapshot');
  const test3Success = await captureSnapshotWithCostCode('MANUAL_TEST_2');

  // Verify all snapshots
  await verifySnapshots();

  // Final summary
  console.log('\n🎯 FINAL TEST RESULTS:');
  console.log(`   Test 1 (Manual): ${test1Success ? '✅ PASSED' : '❌ FAILED'}`);
  console.log(`   Test 2 (Scheduled): ${test2Success ? '✅ PASSED' : '❌ FAILED'}`);
  console.log(`   Test 3 (Manual): ${test3Success ? '✅ PASSED' : '❌ FAILED'}`);
  
  const passedTests = [test1Success, test2Success, test3Success].filter(Boolean).length;
  console.log(`\n🏆 Overall: ${passedTests}/3 tests passed`);
  
  if (passedTests === 3) {
    console.log('\n🎉 Cost code integration is working perfectly!');
  } else if (passedTests > 0) {
    console.log('\n⚠️  Partial success - some issues detected');
  } else {
    console.log('\n❌ All tests failed - investigation needed');
  }
}

// Run the tests
runSnapshotTests();