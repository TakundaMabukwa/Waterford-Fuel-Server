require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugFuelFills() {
  try {
    console.log('🔍 Debugging Fuel Fills System...\n');
    
    const testSite = 'KROONSTAD2';
    const testDate = new Date().toISOString().split('T')[0];
    
    // Step 1: Insert test data
    console.log('📊 Step 1: Inserting test data...');
    
    await supabase.from('energy_rite_fuel_fills').insert({
      plate: testSite,
      cost_code: 'KFC-KROONSTAD2-001',
      company: 'KFC',
      fill_date: new Date().toISOString(),
      fill_day: testDate,
      fuel_before: 45.0,
      fuel_after: 175.0,
      fill_amount: 130.0,
      detection_method: 'MANUAL_TEST'
    });
    
    console.log('✅ Test data inserted');
    
    // Step 2: Check direct query
    console.log('\n🔍 Step 2: Direct database query...');
    
    const { data: directQuery, error: directError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('plate', testSite);
    
    if (directError) {
      console.log('❌ Direct query error:', directError.message);
    } else {
      console.log(`✅ Found ${directQuery.length} records directly`);
      if (directQuery.length > 0) {
        console.log('   Record:', directQuery[0]);
      }
    }
    
    // Step 3: Check date filtering
    console.log('\n📅 Step 3: Testing date filtering...');
    
    const { data: dateQuery, error: dateError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('fill_day', testDate);
    
    if (dateError) {
      console.log('❌ Date query error:', dateError.message);
    } else {
      console.log(`✅ Found ${dateQuery.length} records for ${testDate}`);
    }
    
    // Step 4: Check cost code filtering
    console.log('\n🏢 Step 4: Testing cost code filtering...');
    
    const { data: costQuery, error: costError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('cost_code', 'KFC-KROONSTAD2-001');
    
    if (costError) {
      console.log('❌ Cost code query error:', costError.message);
    } else {
      console.log(`✅ Found ${costQuery.length} records for cost code`);
    }
    
    // Step 5: Test the exact query from reports
    console.log('\n📊 Step 5: Testing reports query...');
    
    let fillsQuery = supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .eq('fill_day', testDate);
    
    const { data: reportsQuery, error: reportsError } = await fillsQuery;
    
    if (reportsError) {
      console.log('❌ Reports query error:', reportsError.message);
    } else {
      console.log(`✅ Reports query found ${reportsQuery.length} records`);
      if (reportsQuery.length > 0) {
        console.log('   Sample record:', reportsQuery[0]);
      }
    }
    
    // Step 6: Test grouping by vehicle
    console.log('\n🚗 Step 6: Testing vehicle grouping...');
    
    const fillsByVehicle = {};
    if (reportsQuery && reportsQuery.length > 0) {
      reportsQuery.forEach(fill => {
        if (!fillsByVehicle[fill.plate]) {
          fillsByVehicle[fill.plate] = {
            fill_count: 0,
            total_filled: 0,
            fills: []
          };
        }
        fillsByVehicle[fill.plate].fill_count++;
        fillsByVehicle[fill.plate].total_filled += parseFloat(fill.fill_amount || 0);
        fillsByVehicle[fill.plate].fills.push({
          time: fill.fill_date,
          amount: fill.fill_amount,
          method: fill.detection_method
        });
      });
      
      console.log('✅ Grouped by vehicle:', Object.keys(fillsByVehicle));
      console.log('   Data:', fillsByVehicle);
    }
    
    // Cleanup
    await supabase.from('energy_rite_fuel_fills').delete().eq('plate', testSite);
    
    console.log('\n🎉 Debug complete!');
    
  } catch (error) {
    console.error('❌ Debug Error:', error.message);
  }
}

debugFuelFills();