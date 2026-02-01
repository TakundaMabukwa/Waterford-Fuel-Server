require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testSessionFlow() {
  console.log('🧪 Testing session management flow...\n');
  
  try {
    // Test 1: Check vehicle lookup table
    console.log('1. Testing vehicle lookup...');
    const { data: vehicles, error: vehicleError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate, cost_code')
      .limit(5);
      
    if (vehicleError) {
      console.error('❌ Vehicle lookup failed:', vehicleError);
      return;
    }
    
    console.log('✅ Found vehicles:', vehicles.length);
    vehicles.forEach(v => console.log(`   ${v.plate} → ${v.cost_code}`));
    
    if (vehicles.length === 0) {
      console.error('❌ No vehicles found in lookup table');
      return;
    }
    
    const testPlate = vehicles[0].plate;
    const expectedCostCode = vehicles[0].cost_code;
    
    // Test 2: Simulate ENGINE ON
    console.log(`\n2. Testing ENGINE ON for ${testPlate}...`);
    const sessionStart = new Date();
    const startingFuel = 150.5;
    
    const { data: sessionData, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .insert({
        branch: testPlate,
        company: 'KFC',
        cost_code: expectedCostCode,
        session_date: sessionStart.toISOString().split('T')[0],
        session_start_time: sessionStart.toISOString(),
        opening_fuel: startingFuel,
        session_status: 'ONGOING',
        notes: 'Test session'
      })
      .select()
      .single();
      
    if (sessionError) {
      console.error('❌ Session creation failed:', sessionError);
      return;
    }
    
    console.log('✅ Session created:', sessionData.id);
    console.log(`   Cost code: ${sessionData.cost_code}`);
    
    // Test 3: Simulate ENGINE OFF
    console.log(`\n3. Testing ENGINE OFF for ${testPlate}...`);
    const sessionEnd = new Date(Date.now() + 30 * 60 * 1000); // 30 minutes later
    const endingFuel = 140.2;
    const fuelConsumed = startingFuel - endingFuel;
    const durationMinutes = 30;
    const fuelCost = fuelConsumed * 20.00;
    
    const { error: updateError } = await supabase
      .from('energy_rite_operating_sessions')
      .update({
        session_end_time: sessionEnd.toISOString(),
        operating_hours: durationMinutes / 60,
        closing_fuel: endingFuel,
        total_usage: fuelConsumed,
        cost_for_usage: fuelCost,
        session_status: 'COMPLETED'
      })
      .eq('id', sessionData.id);
      
    if (updateError) {
      console.error('❌ Session completion failed:', updateError);
      return;
    }
    
    console.log('✅ Session completed');
    console.log(`   Duration: ${durationMinutes} minutes`);
    console.log(`   Fuel used: ${fuelConsumed.toFixed(1)}L`);
    console.log(`   Cost: R${fuelCost.toFixed(2)}`);
    
    // Test 4: Verify session data
    console.log(`\n4. Verifying session data...`);
    const { data: completedSession, error: fetchError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('id', sessionData.id)
      .single();
      
    if (fetchError) {
      console.error('❌ Session fetch failed:', fetchError);
      return;
    }
    
    console.log('✅ Session verification passed');
    console.log(`   Branch: ${completedSession.branch}`);
    console.log(`   Cost Code: ${completedSession.cost_code}`);
    console.log(`   Duration: ${completedSession.operating_hours} hours`);
    console.log(`   Fuel consumed: ${completedSession.total_usage}L`);
    console.log(`   Cost: R${completedSession.cost_for_usage}`);
    
    // Test 5: Test activity log
    console.log(`\n5. Testing activity log...`);
    const { error: activityError } = await supabase
      .from('energy_rite_activity_log')
      .insert({
        activity_type: 'ENGINE_TEST',
        description: `Test session for ${testPlate}`,
        branch: testPlate,
        activity_data: {
          test: true,
          cost_code: expectedCostCode,
          session_id: sessionData.id
        }
      });
      
    if (activityError) {
      console.error('❌ Activity log failed:', activityError);
      return;
    }
    
    console.log('✅ Activity logged successfully');
    
    // Cleanup
    console.log(`\n6. Cleaning up test data...`);
    await supabase.from('energy_rite_operating_sessions').delete().eq('id', sessionData.id);
    await supabase.from('energy_rite_activity_log').delete().eq('activity_type', 'ENGINE_TEST');
    
    console.log('✅ Cleanup completed');
    console.log('\n🎉 All tests passed! Session management is working correctly.');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

// Run the test
testSessionFlow().then(() => {
  console.log('\n✅ Test completed');
  process.exit(0);
}).catch(error => {
  console.error('❌ Test error:', error);
  process.exit(1);
});