require('dotenv').config();
const { supabase } = require('./supabase-client');

/**
 * Test fuel fills being stored in operating_sessions table
 */

async function testFuelFillsInSessions() {
  console.log('🧪 Testing Fuel Fills in Operating Sessions\n');

  // Simulate fuel fill data
  const testFills = [
    {
      plate: 'SUNVALLEY',
      preFill: { level: 150.0, percentage: 75 },
      postFill: { level: 185.0, percentage: 92 },
      fillAmount: 35.0,
      duration: 300 // 5 minutes
    },
    {
      plate: 'MORULA', 
      preFill: { level: 120.0, percentage: 60 },
      postFill: { level: 160.0, percentage: 80 },
      fillAmount: 40.0,
      duration: 420 // 7 minutes
    }
  ];

  for (const fill of testFills) {
    console.log(`⛽ Creating fuel fill session for ${fill.plate}`);
    
    const fillStartTime = new Date(Date.now() - fill.duration * 1000);
    const fillEndTime = new Date();
    
    try {
      // Get vehicle info
      const { data: vehicleInfo } = await supabase
        .from('energyrite_vehicle_lookup')
        .select('cost_code, company')
        .eq('plate', fill.plate)
        .single();

      // Insert fuel fill as operating session
      const { data, error } = await supabase
        .from('energy_rite_operating_sessions')
        .insert({
          branch: fill.plate,
          company: vehicleInfo?.company || 'KFC',
          cost_code: vehicleInfo?.cost_code,
          session_date: fillEndTime.toISOString().split('T')[0],
          session_start_time: fillStartTime.toISOString(),
          session_end_time: fillEndTime.toISOString(),
          operating_hours: fill.duration / 3600,
          opening_fuel: fill.preFill.level,
          opening_percentage: fill.preFill.percentage,
          closing_fuel: fill.postFill.level,
          closing_percentage: fill.postFill.percentage,
          total_fill: fill.fillAmount,
          session_status: 'FUEL_FILL',
          notes: `Fuel Fill: +${fill.fillAmount}L (${fill.preFill.level}L → ${fill.postFill.level}L) Duration: ${fill.duration}s`
        })
        .select();

      if (error) {
        console.log(`   ❌ Error: ${error.message}`);
      } else {
        console.log(`   ✅ Created session ID: ${data[0].id}`);
        console.log(`   📊 Fill: +${fill.fillAmount}L`);
        console.log(`   ⏱️  Duration: ${fill.duration}s`);
      }
      
    } catch (error) {
      console.log(`   ❌ Error: ${error.message}`);
    }
    
    console.log('');
  }

  // Check results
  console.log('📋 Checking fuel fill sessions...\n');
  
  const { data: sessions } = await supabase
    .from('energy_rite_operating_sessions')
    .select('*')
    .eq('session_status', 'FUEL_FILL')
    .gte('created_at', new Date(Date.now() - 10 * 60 * 1000).toISOString())
    .order('created_at', { ascending: false });

  console.log(`Found ${sessions?.length || 0} fuel fill sessions:`);
  
  if (sessions && sessions.length > 0) {
    sessions.forEach(session => {
      console.log(`\n🚗 ${session.branch}:`);
      console.log(`   ID: ${session.id}`);
      console.log(`   Date: ${session.session_date}`);
      console.log(`   Fill Amount: ${session.total_fill}L`);
      console.log(`   Before: ${session.opening_fuel}L (${session.opening_percentage}%)`);
      console.log(`   After: ${session.closing_fuel}L (${session.closing_percentage}%)`);
      console.log(`   Duration: ${(session.operating_hours * 3600).toFixed(0)}s`);
      console.log(`   Status: ${session.session_status}`);
      console.log(`   Notes: ${session.notes}`);
    });
  }

  console.log('\n✅ Fuel fills are now stored in operating_sessions table!');
  console.log('💡 Session status "FUEL_FILL" distinguishes from engine sessions');
}

testFuelFillsInSessions().then(() => process.exit(0));