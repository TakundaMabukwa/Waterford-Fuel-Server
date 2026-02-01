require('dotenv').config();
const { supabase } = require('./supabase-client');

async function createMissingFuelFills() {
  try {
    console.log('🔧 Creating missing fuel fill records...');
    
    // Get all sessions with fuel fills
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gt('total_fill', 0)
      .order('session_date', { ascending: false });

    if (error) throw error;
    
    console.log(`📊 Found ${sessions.length} sessions with fuel fills`);
    
    let created = 0;
    for (const session of sessions) {
      // Check if fuel fill record already exists
      const { data: existing } = await supabase
        .from('energy_rite_fuel_fills')
        .select('id')
        .eq('plate', session.branch)
        .eq('fill_date', `${session.session_date}T12:00:00.000Z`)
        .single();
      
      if (!existing) {
        const fillRecord = {
          plate: session.branch,
          cost_code: session.cost_code,
          company: session.company || 'KFC',
          fill_date: `${session.session_date}T12:00:00Z`,
          fuel_before: session.opening_fuel || 0,
          fuel_after: session.closing_fuel || 0,
          fill_amount: session.total_fill,
          fill_percentage: session.opening_fuel > 0 ? (session.total_fill / session.opening_fuel) * 100 : 0,
          detection_method: 'SESSION_IMPORT',
          status: 'confirmed'
        };

        const { error: insertError } = await supabase
          .from('energy_rite_fuel_fills')
          .insert(fillRecord);

        if (!insertError) {
          created++;
          console.log(`⛽ Created fuel fill: ${session.branch} - ${session.session_date} - ${session.total_fill}L`);
        }
      }
    }
    
    console.log(`✅ Created ${created} fuel fill records`);
    
  } catch (error) {
    console.error('❌ Failed to create fuel fills:', error.message);
  }
}

createMissingFuelFills();