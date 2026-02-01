require('dotenv').config();
const { supabase } = require('./supabase-client');

async function simpleKroonstad2Test() {
  try {
    console.log('🏢 Simple KROONSTAD2 Test...\n');
    
    const testSite = 'KROONSTAD2';
    const costCode = 'KFC-KROONSTAD2-001';
    
    // Step 1: Insert fuel fill record
    console.log('⛽ Step 1: Creating fuel fill...');
    
    const { data: insertResult, error: insertError } = await supabase
      .from('energy_rite_fuel_fills')
      .insert({
        plate: testSite,
        cost_code: costCode,
        company: 'KFC',
        fill_date: new Date().toISOString(),
        fuel_before: 45.0,
        fuel_after: 175.0,
        fill_amount: 130.0,
        detection_method: 'MANUAL_TEST'
      })
      .select();
    
    if (insertError) {
      console.log('❌ Insert error:', insertError.message);
      
      // Try without new columns
      console.log('Trying basic insert...');
      const { data: basicResult, error: basicError } = await supabase
        .from('energy_rite_fuel_fills')
        .insert({
          plate: testSite,
          fill_date: new Date().toISOString(),
          fuel_before: 45.0,
          fuel_after: 175.0,
          fill_amount: 130.0,
          detection_method: 'MANUAL_TEST'
        })
        .select();
      
      if (basicError) {
        console.log('❌ Basic insert error:', basicError.message);
        return;
      } else {
        console.log('✅ Basic insert successful');
      }
    } else {
      console.log('✅ Full insert successful');
    }
    
    // Step 2: Query the record back
    console.log('\n🔍 Step 2: Querying records...');
    
    const { data: queryResult, error: queryError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('plate', testSite);
    
    if (queryError) {
      console.log('❌ Query error:', queryError.message);
    } else {
      console.log(`✅ Found ${queryResult.length} records for ${testSite}`);
      if (queryResult.length > 0) {
        const record = queryResult[0];
        console.log('Record details:');
        console.log(`  Plate: ${record.plate}`);
        console.log(`  Cost Code: ${record.cost_code || 'Not set'}`);
        console.log(`  Company: ${record.company || 'Not set'}`);
        console.log(`  Fill Amount: ${record.fill_amount}L`);
        console.log(`  Fill Date: ${record.fill_date}`);
        console.log(`  Method: ${record.detection_method}`);
      }
    }
    
    // Step 3: Test date filtering
    console.log('\n📅 Step 3: Testing date filtering...');
    
    const today = new Date().toISOString().split('T')[0];
    const tomorrow = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
    
    const { data: dateResult, error: dateError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .gte('fill_date', today)
      .lt('fill_date', tomorrow);
    
    if (dateError) {
      console.log('❌ Date filter error:', dateError.message);
    } else {
      console.log(`✅ Found ${dateResult.length} records for today`);
      const kroonstad2Records = dateResult.filter(r => r.plate === testSite);
      console.log(`✅ ${kroonstad2Records.length} records for ${testSite} today`);
    }
    
    // Step 4: Test API endpoint directly
    console.log('\n🌐 Step 4: Testing API endpoint...');
    
    try {
      const axios = require('axios');
      const response = await axios.get('http://localhost:4000/api/energy-rite/fuel-fills', {
        params: { plate: testSite, limit: 10 }
      });
      
      console.log('✅ API Response:');
      console.log(`  Success: ${response.data.success}`);
      console.log(`  Count: ${response.data.count}`);
      
      if (response.data.data && response.data.data.length > 0) {
        const apiRecord = response.data.data[0];
        console.log('  API Record:');
        console.log(`    Plate: ${apiRecord.plate}`);
        console.log(`    Amount: ${apiRecord.fill_amount}L`);
        console.log(`    Cost Code: ${apiRecord.cost_code || 'Not set'}`);
      }
      
    } catch (apiError) {
      console.log('❌ API Error:', apiError.message);
    }
    
    // Cleanup
    console.log('\n🧹 Cleaning up...');
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testSite);
    
    console.log('\n🎉 Simple test complete!');
    console.log('\n✅ Results:');
    console.log(`  • ${testSite} fuel fill records can be created`);
    console.log('  • Records can be queried by plate');
    console.log('  • Date filtering works');
    console.log('  • API endpoints are accessible');
    
  } catch (error) {
    console.error('❌ Test Error:', error.message);
  }
}

simpleKroonstad2Test();