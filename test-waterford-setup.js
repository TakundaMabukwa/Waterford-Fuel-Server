require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testWaterfordSetup() {
  console.log('🔍 Testing Waterford Setup...\n');
  
  // Test 1: Check vehicle lookup
  const { data: vehicles, error } = await supabase
    .from('energyrite_vehicle_lookup')
    .select('*')
    .eq('cost_code', 'WATE-0001');
  
  if (error) {
    console.log('❌ Error:', error.message);
    return;
  }
  
  console.log(`✅ Found ${vehicles.length} vehicles with WATE-0001`);
  console.log(`   Sample: ${vehicles.slice(0, 3).map(v => v.plate).join(', ')}\n`);
  
  // Test 2: Check cost center access
  const costCenterAccess = require('./helpers/cost-center-access');
  const accessible = await costCenterAccess.getAccessibleCostCenters('WATE-0001');
  
  console.log(`✅ Cost center access for WATE-0001:`);
  console.log(`   Accessible: ${accessible.join(', ')}\n`);
  
  // Test 3: Check if reports will work
  const sites = await costCenterAccess.getAccessibleSites('WATE-0001');
  console.log(`✅ Accessible sites: ${sites.length}`);
  console.log(`   Sample: ${sites.slice(0, 5).map(s => s.plate).join(', ')}\n`);
  
  console.log('🎉 Waterford setup is ready!');
  console.log('\n📋 Next steps:');
  console.log('   1. Run setup-waterford-vehicles.sql in Supabase SQL Editor');
  console.log('   2. Start server: npm start');
  console.log('   3. Test reports with: cost_code=WATE-0001');
}

testWaterfordSetup();
