require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkAndFixTable() {
  try {
    console.log('🔧 Checking and fixing fuel fills table...\n');
    
    // Check current table structure
    const { data: sample, error: sampleError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .limit(1);
    
    if (sample && sample.length > 0) {
      console.log('📋 Current table columns:', Object.keys(sample[0]));
    } else {
      console.log('📋 Table is empty, checking structure...');
    }
    
    // Try to add missing columns
    console.log('\n🔧 Adding missing columns...');
    
    // Insert a test record to check what columns exist
    const testRecord = {
      plate: 'STRUCTURE-TEST',
      fill_date: new Date().toISOString(),
      fuel_before: 50,
      fuel_after: 100,
      fill_amount: 50,
      detection_method: 'TEST'
    };
    
    // Try with new columns
    try {
      await supabase.from('energy_rite_fuel_fills').insert({
        ...testRecord,
        cost_code: 'TEST-001',
        company: 'TEST-COMPANY',
        fill_day: new Date().toISOString().split('T')[0]
      });
      console.log('✅ New columns exist');
    } catch (insertError) {
      console.log('❌ New columns missing:', insertError.message);
      console.log('\n📝 Run this SQL in Supabase:');
      console.log(`
ALTER TABLE energy_rite_fuel_fills 
ADD COLUMN IF NOT EXISTS cost_code VARCHAR(50),
ADD COLUMN IF NOT EXISTS company VARCHAR(100),
ADD COLUMN IF NOT EXISTS fill_day DATE;

-- Update existing records
UPDATE energy_rite_fuel_fills 
SET fill_day = DATE(fill_date) 
WHERE fill_day IS NULL;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_fuel_fills_cost_code ON energy_rite_fuel_fills(cost_code);
CREATE INDEX IF NOT EXISTS idx_fuel_fills_day ON energy_rite_fuel_fills(fill_day);
CREATE INDEX IF NOT EXISTS idx_fuel_fills_cost_day ON energy_rite_fuel_fills(cost_code, fill_day);
      `);
      return;
    }
    
    // Clean up test record
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', 'STRUCTURE-TEST');
    
    // Check if we have vehicle lookup table
    console.log('\n🔍 Checking vehicle lookup table...');
    
    const { data: vehicleLookup, error: lookupError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate, cost_code, company')
      .limit(5);
    
    if (lookupError) {
      console.log('❌ Vehicle lookup table missing or inaccessible');
      console.log('   Creating sample vehicle lookup data...');
      
      // Create sample lookup data
      const sampleVehicles = [
        { plate: 'TEST-001', cost_code: 'KFC-001', company: 'KFC' },
        { plate: 'TEST-002', cost_code: 'KFC-002', company: 'KFC' },
        { plate: 'TEST-003', cost_code: 'KFC-003', company: 'KFC' }
      ];
      
      try {
        await supabase.from('energyrite_vehicle_lookup').upsert(sampleVehicles);
        console.log('✅ Sample vehicle lookup data created');
      } catch (createError) {
        console.log('❌ Cannot create vehicle lookup table');
        console.log('   Fuel fills will work without cost codes');
      }
    } else {
      console.log(`✅ Vehicle lookup table exists with ${vehicleLookup.length} sample records`);
      if (vehicleLookup.length > 0) {
        console.log('   Sample:', vehicleLookup[0]);
      }
    }
    
    // Test the updated system
    console.log('\n🧪 Testing updated system...');
    
    const { detectFuelFill } = require('./helpers/fuel-fill-detector');
    
    // Create test vehicle
    const testPlate = 'FINAL-TEST-' + Date.now();
    
    await supabase.from('energyrite_vehicle_lookup').upsert({
      plate: testPlate,
      cost_code: 'KFC-FINAL-001',
      company: 'KFC'
    });
    
    // Insert fuel data
    await supabase.from('energy_rite_fuel_data').insert([
      {
        plate: testPlate,
        fuel_probe_1_level: 50.0,
        created_at: new Date(Date.now() - 10 * 60 * 1000).toISOString()
      },
      {
        plate: testPlate,
        fuel_probe_1_level: 180.0,
        created_at: new Date().toISOString()
      }
    ]);
    
    // Test detection
    const fillResult = await detectFuelFill(testPlate, 180.0, 'Normal Operation');
    
    if (fillResult.isFill) {
      console.log('✅ Fuel fill detection working');
      console.log(`   Amount: ${fillResult.fillDetails.fillAmount}L`);
      
      // Check if it was logged with cost code
      const { data: loggedFill } = await supabase
        .from('energy_rite_fuel_fills')
        .select('plate, cost_code, company, fill_day, fill_amount')
        .eq('plate', testPlate)
        .single();
      
      if (loggedFill) {
        console.log('✅ Fill logged with cost center data:');
        console.log(`   Plate: ${loggedFill.plate}`);
        console.log(`   Cost Code: ${loggedFill.cost_code}`);
        console.log(`   Company: ${loggedFill.company}`);
        console.log(`   Fill Day: ${loggedFill.fill_day}`);
        console.log(`   Amount: ${loggedFill.fill_amount}L`);
      }
    } else {
      console.log('❌ Fuel fill detection failed:', fillResult.reason);
    }
    
    // Cleanup
    await supabase.from('energy_rite_fuel_data').delete().eq('plate', testPlate);
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testPlate);
    await supabase.from('energyrite_vehicle_lookup').delete().eq('plate', testPlate);
    
    console.log('\n🎉 Table check and fix complete!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkAndFixTable();