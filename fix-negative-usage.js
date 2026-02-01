require('dotenv').config();
const { supabase } = require('./supabase-client');

async function fixNegativeUsage() {
  try {
    console.log('🔍 Analyzing sessions with negative usage...\n');

    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .lt('total_usage', 0)
      .order('session_start_time', { ascending: false });

    if (error) throw error;

    console.log(`Found ${sessions.length} sessions with negative usage\n`);

    let fixed = 0;
    for (const session of sessions) {
      const openingFuel = session.opening_fuel || 0;
      const closingFuel = session.closing_fuel || 0;
      const diff = closingFuel - openingFuel;

      console.log(`${session.branch} ${session.session_date}:`);
      console.log(`  Opening: ${openingFuel}L → Closing: ${closingFuel}L`);
      console.log(`  Difference: ${diff}L`);

      if (diff > 5) {
        // This is a FILL (fuel increased significantly)
        console.log(`  ✅ Converting to FUEL FILL (+${diff.toFixed(1)}L)\n`);
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            total_usage: 0,
            total_fill: diff,
            session_status: 'FUEL_FILL_COMPLETED',
            cost_for_usage: 0,
            liter_usage_per_hour: 0,
            notes: `${session.notes || ''} | Corrected: Fuel fill detected (+${diff.toFixed(1)}L)`
          })
          .eq('id', session.id);
        
        fixed++;
      } else if (diff < -5) {
        // This is USAGE (fuel decreased)
        console.log(`  ✅ Correcting usage (${Math.abs(diff).toFixed(1)}L consumed)\n`);
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            total_usage: Math.abs(diff),
            total_fill: 0,
            cost_for_usage: Math.abs(diff) * 21.00,
            liter_usage_per_hour: session.operating_hours > 0 ? Math.abs(diff) / session.operating_hours : 0,
            notes: `${session.notes || ''} | Corrected: Usage calculated from fuel drop`
          })
          .eq('id', session.id);
        
        fixed++;
      } else {
        // Small difference, likely measurement error
        console.log(`  ⚠️ Small difference (${diff.toFixed(1)}L), setting to 0\n`);
        
        await supabase
          .from('energy_rite_operating_sessions')
          .update({
            total_usage: 0,
            total_fill: 0,
            cost_for_usage: 0,
            notes: `${session.notes || ''} | Corrected: Negligible fuel change`
          })
          .eq('id', session.id);
        
        fixed++;
      }
    }

    console.log(`\n✅ Fixed ${fixed} sessions`);
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

fixNegativeUsage();
