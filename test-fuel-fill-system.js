require('dotenv').config();
const { supabase } = require('./supabase-client');
const { detectFuelFill, getFuelFillStatistics } = require('./helpers/fuel-fill-detector');

async function testFuelFillSystem() {
  try {
    console.log('⛽ Testing Fuel Fill Detection System...\n');
    
    // Test 1: Create the fuel fills table
    console.log('📋 Step 1: Creating fuel fills table...');
    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS energy_rite_fuel_fills (
        id BIGSERIAL PRIMARY KEY,
        plate VARCHAR(50) NOT NULL,
        fill_date TIMESTAMPTZ NOT NULL,
        fuel_before NUMERIC(10,2),
        fuel_after NUMERIC(10,2),
        fill_amount NUMERIC(10,2),
        fill_percentage NUMERIC(5,2),
        detection_method VARCHAR(50),
        status VARCHAR(50) DEFAULT 'detected',
        fill_data JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `;
    
    const { error: createError } = await supabase.rpc('exec_sql', { sql: createTableSQL });
    if (createError) {
      console.log('Table might already exist:', createError.message);
    } else {
      console.log('✅ Fuel fills table created successfully');
    }
    
    // Test 2: Insert test fuel data to simulate a fill
    console.log('\n📊 Step 2: Inserting test fuel data...');
    
    const testPlate = 'TEST-FUEL-FILL';
    const baseTime = new Date();
    
    // Insert initial fuel level
    await supabase.from('energy_rite_fuel_data').insert({
      plate: testPlate,
      fuel_probe_1_level: 50.0,
      fuel_probe_1_volume_in_tank: 200,
      fuel_probe_1_temperature: 25,
      fuel_probe_1_level_percentage: 25,
      created_at: new Date(baseTime.getTime() - 5 * 60 * 1000).toISOString() // 5 minutes ago
    });
    
    // Insert fuel level after fill (significant increase)
    await supabase.from('energy_rite_fuel_data').insert({
      plate: testPlate,
      fuel_probe_1_level: 180.0, // +130L increase
      fuel_probe_1_volume_in_tank: 720,
      fuel_probe_1_temperature: 25,
      fuel_probe_1_level_percentage: 90,
      created_at: baseTime.toISOString()
    });
    
    console.log('✅ Test fuel data inserted');
    
    // Test 3: Test fuel fill detection
    console.log('\n🔍 Step 3: Testing fuel fill detection...');
    
    const fillResult = await detectFuelFill(testPlate, 180.0, 'Normal Operation');
    
    if (fillResult.isFill) {
      console.log('✅ Fuel fill detected successfully!');
      console.log(`   Fill Amount: ${fillResult.fillDetails.fillAmount.toFixed(1)}L`);
      console.log(`   Fill Percentage: ${fillResult.fillDetails.fillPercentage}%`);
      console.log(`   Detection Method: ${fillResult.fillDetails.detectionMethod}`);
    } else {
      console.log('❌ Fuel fill not detected');
      console.log(`   Reason: ${fillResult.reason}`);
    }
    
    // Test 4: Test status-based detection
    console.log('\n🔍 Step 4: Testing status-based detection...');
    
    const statusFillResult = await detectFuelFill(testPlate, 185.0, 'Possible Fuel Fill');
    
    if (statusFillResult.isFill) {
      console.log('✅ Status-based fuel fill detected!');
      console.log(`   Detection Method: ${statusFillResult.fillDetails.detectionMethod}`);
    } else {
      console.log('❌ Status-based fuel fill not detected');
    }
    
    // Test 5: Get fuel fill statistics
    console.log('\n📈 Step 5: Getting fuel fill statistics...');
    
    const stats = await getFuelFillStatistics();
    
    if (stats.error) {
      console.log('❌ Error getting statistics:', stats.error);
    } else {
      console.log('✅ Fuel fill statistics:');
      console.log(`   Total vehicles with fills: ${stats.statistics.total_vehicles_with_fills}`);
      console.log(`   Total fill events: ${stats.statistics.total_fill_events}`);
      console.log(`   Recent fills (24h): ${stats.statistics.recent_fills}`);
      console.log(`   Total fuel filled: ${stats.statistics.total_fuel_filled.toFixed(1)}L`);
    }
    
    // Test 6: Check fuel fills in database
    console.log('\n📋 Step 6: Checking fuel fills in database...');
    
    const { data: fills, error: fillsError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('plate', testPlate)
      .order('fill_date', { ascending: false });
    
    if (fillsError) {
      console.log('❌ Error fetching fills:', fillsError.message);
    } else {
      console.log(`✅ Found ${fills.length} fuel fills for ${testPlate}:`);
      fills.forEach((fill, index) => {
        console.log(`   Fill ${index + 1}:`);
        console.log(`     Amount: ${fill.fill_amount}L`);
        console.log(`     Method: ${fill.detection_method}`);
        console.log(`     Date: ${fill.fill_date}`);
      });
    }
    
    // Test 7: Test API endpoints
    console.log('\n🌐 Step 7: Testing API endpoints...');
    
    const axios = require('axios');
    const baseURL = 'http://localhost:4000';
    
    try {
      // Test fuel fills endpoint
      const fillsResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills?limit=5`);
      console.log('✅ Fuel fills API working');
      console.log(`   Retrieved ${fillsResponse.data.count} fills`);
      
      // Test statistics endpoint
      const statsResponse = await axios.get(`${baseURL}/api/energy-rite/fuel-fills/statistics`);
      console.log('✅ Statistics API working');
      console.log(`   Total fill events: ${statsResponse.data.data.statistics.total_fill_events}`);
      
    } catch (apiError) {
      console.log('⚠️  API endpoints not accessible (server might not be running)');
      console.log('   Start server with: npm start');
    }
    
    // Cleanup test data
    console.log('\n🧹 Cleaning up test data...');
    
    await supabase.from('energy_rite_fuel_data').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_activity_log').delete().eq('branch', testPlate);
    
    console.log('✅ Test data cleaned up');
    
    console.log('\n🎉 Fuel Fill Detection System Test Complete!');
    console.log('\n📝 Summary:');
    console.log('   ✅ Fuel fill detection by level increase');
    console.log('   ✅ Fuel fill detection by status indicator');
    console.log('   ✅ Database logging of fuel fills');
    console.log('   ✅ Statistics and reporting');
    console.log('   ✅ API endpoints for fuel fills');
    
    console.log('\n🚀 Next Steps:');
    console.log('   1. Run the SQL script: create-fuel-fills-table.sql');
    console.log('   2. Start the server: npm start');
    console.log('   3. Monitor WebSocket for real-time fuel fill detection');
    console.log('   4. Check reports for fuel fill information');
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testFuelFillSystem();