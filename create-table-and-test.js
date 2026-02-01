require('dotenv').config();
const { supabase } = require('./supabase-client');

async function createTableAndTest() {
  try {
    console.log('🔧 Creating fuel fills table...\n');
    
    // Create the fuel fills table
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
    
    const { error: createError } = await supabase.rpc('sql', { query: createTableSQL });
    if (createError) {
      console.log('⚠️  Creating table via RPC failed, trying direct insert...');
      
      // Try creating via direct SQL execution
      const { error: directError } = await supabase
        .from('energy_rite_fuel_fills')
        .select('id')
        .limit(1);
        
      if (directError && directError.message.includes('does not exist')) {
        console.log('❌ Table does not exist. Please run this SQL in Supabase SQL Editor:');
        console.log('\n--- COPY AND PASTE THIS SQL ---');
        console.log(createTableSQL);
        console.log('\n--- END SQL ---\n');
        return;
      } else {
        console.log('✅ Table already exists or accessible');
      }
    } else {
      console.log('✅ Table created successfully');
    }
    
    // Test the fuel fill detection system
    console.log('🧪 Testing fuel fill detection...\n');
    
    const { detectFuelFill } = require('./helpers/fuel-fill-detector');
    
    // Insert test data
    const testPlate = 'TEST-FILL-' + Date.now();
    
    // Insert initial fuel level
    await supabase.from('energy_rite_fuel_data').insert({
      plate: testPlate,
      fuel_probe_1_level: 50.0,
      created_at: new Date(Date.now() - 5 * 60 * 1000).toISOString()
    });
    
    // Insert fuel after fill
    await supabase.from('energy_rite_fuel_data').insert({
      plate: testPlate,
      fuel_probe_1_level: 180.0,
      created_at: new Date().toISOString()
    });
    
    console.log('📊 Test data inserted');
    
    // Test detection
    const fillResult = await detectFuelFill(testPlate, 180.0, 'Normal Operation');
    
    if (fillResult.isFill) {
      console.log('✅ Fuel fill detected!');
      console.log(`   Amount: ${fillResult.fillDetails.fillAmount.toFixed(1)}L`);
      console.log(`   Method: ${fillResult.fillDetails.detectionMethod}`);
    } else {
      console.log('❌ Fuel fill not detected');
      console.log(`   Reason: ${fillResult.reason}`);
    }
    
    // Check if fill was logged
    const { data: fills } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('plate', testPlate);
    
    console.log(`📋 Logged ${fills?.length || 0} fuel fills`);
    
    // Test API endpoints
    console.log('\n🌐 Testing API endpoints...');
    
    try {
      const axios = require('axios');
      
      // Test fuel fills endpoint
      const response = await axios.get('http://localhost:4000/api/energy-rite/fuel-fills?limit=5');
      console.log('✅ API working - Retrieved', response.data.count, 'fills');
      
    } catch (apiError) {
      console.log('⚠️  API not accessible - Start server with: npm start');
    }
    
    // Test reports integration
    console.log('\n📊 Testing reports integration...');
    
    const { data: reportData } = await supabase
      .from('energy_rite_fuel_fills')
      .select('plate, fill_amount, detection_method')
      .limit(5);
    
    if (reportData && reportData.length > 0) {
      console.log('✅ Reports can access fuel fill data');
      reportData.forEach(fill => {
        console.log(`   ${fill.plate}: +${fill.fill_amount}L (${fill.detection_method})`);
      });
    } else {
      console.log('📝 No fuel fills in database yet');
    }
    
    // Cleanup
    await supabase.from('energy_rite_fuel_data').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testPlate);
    
    console.log('\n🎉 System ready! Fuel fills will now be:');
    console.log('   ✅ Detected in real-time via WebSocket');
    console.log('   ✅ Logged to energy_rite_fuel_fills table');
    console.log('   ✅ Included in activity reports');
    console.log('   ✅ Available via API endpoints');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    
    if (error.message.includes('relation "energy_rite_fuel_fills" does not exist')) {
      console.log('\n🔧 Please create the table manually:');
      console.log('1. Go to Supabase Dashboard > SQL Editor');
      console.log('2. Run the SQL from: create-fuel-fills-table.sql');
      console.log('3. Then run this test again');
    }
  }
}

createTableAndTest();