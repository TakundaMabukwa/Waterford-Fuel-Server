require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkAvailableDates() {
  try {
    console.log('📅 Checking available dates in fuel data...\n');
    
    // Get recent fuel data dates
    const { data: fuelDates, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('created_at')
      .order('created_at', { ascending: false })
      .limit(10);
      
    if (fuelError) {
      console.error('❌ Fuel data error:', fuelError.message);
      return;
    }
    
    console.log('🔥 Recent fuel data timestamps:');
    fuelDates.forEach((record, index) => {
      const date = new Date(record.created_at);
      console.log(`${index + 1}. ${date.toISOString().split('T')[0]} ${date.toTimeString().split(' ')[0]}`);
    });
    
    // Get recent session dates
    const { data: sessionDates, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('session_date, branch')
      .order('session_date', { ascending: false })
      .limit(10);
      
    if (sessionError) {
      console.error('❌ Session data error:', sessionError.message);
      return;
    }
    
    console.log('\n🏃 Recent session dates:');
    sessionDates.forEach((record, index) => {
      console.log(`${index + 1}. ${record.session_date} - ${record.branch}`);
    });
    
    // Get unique plates from fuel data
    const { data: plates, error: platesError } = await supabase
      .from('energy_rite_fuel_data')
      .select('plate')
      .not('plate', 'is', null);
      
    if (platesError) {
      console.error('❌ Plates error:', platesError.message);
      return;
    }
    
    const uniquePlates = [...new Set(plates.map(p => p.plate))];
    console.log(`\n🚗 Available plates (${uniquePlates.length} total):`, uniquePlates.slice(0, 10));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkAvailableDates();