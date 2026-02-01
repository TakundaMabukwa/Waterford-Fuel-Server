require('dotenv').config();
const { supabase } = require('./supabase-client');

async function updateCosts() {
  try {
    console.log('💰 Updating cost calculations...');
    
    const { data, error } = await supabase
      .rpc('update_costs', { cost_per_liter: 21.41 });
    
    if (error) throw error;
    
    console.log(`✅ Updated ${data.length} sessions with cost calculations`);
    console.log('💰 Cost per liter: R21.41');
    
  } catch (error) {
    console.error('❌ Cost update failed:', error.message);
  }
}

updateCosts();