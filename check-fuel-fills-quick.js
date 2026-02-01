require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkFuelFills() {
  console.log('🔍 Checking fuel fills data...');
  
  const { data: fills, error } = await supabase
    .from('energy_rite_fuel_fills')
    .select('*')
    .order('fill_date', { ascending: false })
    .limit(10);
    
  if (error) {
    console.error('Error:', error.message);
    return;
  }
  
  console.log(`📊 Found ${fills.length} recent fuel fills`);
  fills.forEach(fill => {
    console.log(`   ${fill.plate}: +${fill.fill_amount}L on ${fill.fill_date.split('T')[0]}`);
  });
  
  // Check total fills by date
  const { data: dailyFills } = await supabase
    .from('energy_rite_fuel_fills')
    .select('fill_day, fill_amount')
    .order('fill_day', { ascending: false })
    .limit(20);
    
  const dailyStats = {};
  dailyFills?.forEach(fill => {
    const day = fill.fill_day;
    if (!dailyStats[day]) {
      dailyStats[day] = { count: 0, total: 0 };
    }
    dailyStats[day].count++;
    dailyStats[day].total += parseFloat(fill.fill_amount || 0);
  });
  
  console.log('\n📅 Daily fuel fill totals:');
  Object.entries(dailyStats).slice(0, 5).forEach(([day, stats]) => {
    console.log(`   ${day}: ${stats.count} fills, ${stats.total.toFixed(1)}L total`);
  });
}

checkFuelFills();