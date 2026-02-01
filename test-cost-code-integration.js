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

async function testCostCodeIntegration() {
  console.log('🔍 Testing Cost Code Integration Status...\n');

  try {
    // 1. Check if snapshot_data column exists
    console.log('1. Checking database schema...');
    const { data: snapshots, error: snapshotError } = await supabase
      .from('energy_rite_daily_snapshots')
      .select('*')
      .limit(1);

    if (snapshotError) {
      console.log('❌ Error accessing snapshots table:', snapshotError.message);
      return;
    }

    // Check if snapshot_data column exists in the structure
    if (snapshots.length > 0) {
      const columns = Object.keys(snapshots[0]);
      const hasSnapshotData = columns.includes('snapshot_data');
      
      if (hasSnapshotData) {
        console.log('✅ snapshot_data column exists');
      } else {
        console.log('❌ snapshot_data column missing');
        console.log('🔧 Available columns:', columns.join(', '));
        console.log('💡 Apply simple-add-column.sql to fix this');
        return;
      }
    } else {
      console.log('ℹ️  No snapshots exist yet, but table accessible');
    }

    // 2. Check operating sessions for cost codes
    console.log('\n2. Checking cost code availability...');
    const { data: sessions, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('site_id, cost_code')
      .not('cost_code', 'is', null)
      .not('cost_code', 'eq', '')
      .limit(10);

    if (sessionError) {
      console.log('❌ Error accessing operating sessions:', sessionError.message);
      return;
    }

    console.log(`✅ Found ${sessions.length} sessions with cost codes`);
    if (sessions.length > 0) {
      console.log('📋 Sample cost codes:');
      sessions.slice(0, 3).forEach((session, index) => {
        console.log(`   ${index + 1}. Site ${session.site_id}: ${session.cost_code}`);
      });
    }

    // 3. Test creating a site mapping
    console.log('\n3. Testing site mapping creation...');
    const siteMapping = {};
    sessions.forEach(session => {
      if (session.site_id && session.cost_code) {
        siteMapping[session.site_id] = session.cost_code;
      }
    });

    console.log(`✅ Site mapping created with ${Object.keys(siteMapping).length} entries`);
    console.log('📋 Sample mapping:', Object.entries(siteMapping).slice(0, 3));

    // 4. Check for current vehicle data
    console.log('\n4. Checking current vehicle data...');
    const { data: vehicles, error: vehicleError } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .order('timestamp', { ascending: false })
      .limit(5);

    if (vehicleError) {
      console.log('❌ Error accessing vehicle data:', vehicleError.message);
      return;
    }

    console.log(`✅ Found ${vehicles.length} recent vehicle records`);
    if (vehicles.length > 0) {
      const activeVehicles = vehicles.filter(v => 
        v.engine_status === 'ON' || 
        v.engine_status === 'IDLING' || 
        (Date.now() - new Date(v.timestamp).getTime()) < 4 * 60 * 60 * 1000
      );
      
      console.log(`🚗 ${activeVehicles.length} vehicles active in last 4 hours`);
      
      if (activeVehicles.length > 0) {
        const totalFuel = activeVehicles.reduce((sum, v) => sum + (v.fuel_volume || 0), 0);
        console.log(`⛽ Total fuel volume: ${totalFuel.toFixed(1)}L`);
      }
    }

    // 5. Test what a snapshot would look like
    console.log('\n5. Testing snapshot data structure...');
    if (vehicles.length > 0 && Object.keys(siteMapping).length > 0) {
      const sampleVehicle = vehicles[0];
      const sampleSiteId = Object.keys(siteMapping)[0];
      const sampleCostCode = siteMapping[sampleSiteId];

      const sampleSnapshot = {
        cost_code: sampleCostCode,
        fuel_level: sampleVehicle.fuel_level || 75.5,
        fuel_volume: sampleVehicle.fuel_volume || 180.2,
        engine_status: sampleVehicle.engine_status || 'OFF',
        timestamp: new Date().toISOString()
      };

      console.log('✅ Sample snapshot data structure:');
      console.log(JSON.stringify(sampleSnapshot, null, 2));
    }

    // 6. Final status
    console.log('\n🎯 INTEGRATION STATUS:');
    const hasColumn = snapshots.length === 0 || Object.keys(snapshots[0]).includes('snapshot_data');
    const hasCostCodes = sessions.length > 0;
    const hasVehicleData = vehicles.length > 0;

    console.log(`📊 Database Schema: ${hasColumn ? '✅ Ready' : '❌ Needs snapshot_data column'}`);
    console.log(`💼 Cost Codes: ${hasCostCodes ? '✅ Available' : '❌ Missing'} (${sessions.length} found)`);
    console.log(`🚗 Vehicle Data: ${hasVehicleData ? '✅ Available' : '⚠️  Limited'} (${vehicles.length} records)`);

    if (hasColumn && hasCostCodes) {
      console.log('\n🎉 Cost code integration is READY!');
      console.log('The enhanced snapshot scheduler will automatically include cost codes.');
    } else {
      console.log('\n⚠️  Integration blocked by missing requirements above');
      if (!hasColumn) {
        console.log('💡 Run: ALTER TABLE energy_rite_daily_snapshots ADD COLUMN snapshot_data JSONB DEFAULT \'{}\';');
      }
    }

  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

// Run the test
testCostCodeIntegration();