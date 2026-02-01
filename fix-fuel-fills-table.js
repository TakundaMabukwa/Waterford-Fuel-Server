require('dotenv').config();
const { supabase } = require('./supabase-client');

async function fixFuelFillsTable() {
  try {
    console.log('🔧 Fixing fuel fills table structure...\n');
    
    // Step 1: Check current table structure
    console.log('📋 Step 1: Checking current structure...');
    
    const { data: sample } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .limit(1);
    
    if (sample && sample.length > 0) {
      console.log('Current columns:', Object.keys(sample[0]));
    }
    
    // Step 2: Add missing columns using direct insert test
    console.log('\n🔧 Step 2: Testing column additions...');
    
    const testRecord = {
      plate: 'COLUMN-TEST',
      fill_date: new Date().toISOString(),
      fuel_before: 50,
      fuel_after: 100,
      fill_amount: 50,
      detection_method: 'TEST'
    };
    
    // Test basic insert
    try {
      await supabase.from('energy_rite_fuel_fills').insert(testRecord);
      console.log('✅ Basic insert works');
    } catch (basicError) {
      console.log('❌ Basic insert failed:', basicError.message);
      return;
    }
    
    // Test with new columns
    try {
      await supabase.from('energy_rite_fuel_fills').insert({
        ...testRecord,
        plate: 'COLUMN-TEST-2',
        cost_code: 'TEST-001',
        company: 'TEST-COMPANY'
      });
      console.log('✅ Insert with cost_code and company works');
    } catch (newColError) {
      console.log('❌ New columns missing:', newColError.message);
      console.log('\n📝 Please run this SQL in Supabase:');
      console.log(`
ALTER TABLE energy_rite_fuel_fills 
ADD COLUMN IF NOT EXISTS cost_code VARCHAR(50),
ADD COLUMN IF NOT EXISTS company VARCHAR(100);
      `);
    }
    
    // Clean up test records
    await supabase.from('energy_rite_fuel_fills').delete().like('plate', 'COLUMN-TEST%');
    
    console.log('\n✅ Table structure check complete');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

fixFuelFillsTable();