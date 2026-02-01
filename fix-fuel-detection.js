require('dotenv').config();
const { supabase } = require('./supabase-client');

async function fixFuelDetection() {
  try {
    console.log('🔧 Analyzing fuel detection issues...');
    
    // Check current fuel fills
    const { data: fills, error: fillError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .order('fill_date', { ascending: false })
      .limit(10);

    if (fillError) throw fillError;
    
    console.log(`📊 Current fuel fills in database: ${fills.length}`);
    
    // Check operating sessions with fuel data
    const { data: sessions, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gt('total_fill', 0)
      .order('session_date', { ascending: false })
      .limit(10);

    if (sessionError) throw sessionError;
    
    console.log(`📊 Sessions with fuel fills: ${sessions.length}`);
    
    if (sessions.length > 0) {
      console.log('Sample session with fill:', {
        branch: sessions[0].branch,
        date: sessions[0].session_date,
        opening_fuel: sessions[0].opening_fuel,
        closing_fuel: sessions[0].closing_fuel,
        total_fill: sessions[0].total_fill
      });
    }
    
    // Check fuel data table
    const { data: fuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(5);

    if (fuelError) {
      console.log('⚠️ No fuel_data table or no data');
    } else {
      console.log(`📊 Real-time fuel data entries: ${fuelData.length}`);
    }
    
    console.log('\n🔍 Issues identified:');
    console.log('1. Fuel fills are detected from operating_sessions, not real-time data');
    console.log('2. Need to create fuel fills from historical sessions with total_fill > 0');
    console.log('3. Real-time detection needs fuel_data table entries');
    
  } catch (error) {
    console.error('❌ Analysis failed:', error.message);
  }
}

fixFuelDetection();