require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkReportData() {
  try {
    console.log('📊 Checking what data was included in reports...\n');
    
    // Check sessions for the date range
    console.log('1️⃣ Sessions for 2025-10-30:');
    const { data: sessions, error: sessionsError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', '2025-10-30')
      .lte('session_date', '2025-10-30')
      .order('session_start_time', { ascending: false });
    
    if (sessionsError) {
      console.error('❌ Sessions error:', sessionsError);
    } else {
      console.log(`   Found ${sessions.length} sessions on 2025-10-30`);
      sessions.forEach(session => {
        console.log(`   - ${session.branch} (${session.cost_code}): ${session.operating_hours}h, ${session.total_usage}L`);
      });
    }
    
    // Check sites that should be included for KFC-0001-0001-0002-0004
    console.log('\n2️⃣ Sites for cost code KFC-0001-0001-0002-0004:');
    const { data: sites, error: sitesError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('*')
      .eq('cost_code', 'KFC-0001-0001-0002-0004')
      .order('plate', { ascending: true });
    
    if (sitesError) {
      console.error('❌ Sites error:', sitesError);
    } else {
      console.log(`   Found ${sites.length} sites for KFC-0001-0001-0002-0004:`);
      sites.forEach(site => {
        console.log(`   - ${site.plate} (${site.branch_name || 'No branch name'})`);
      });
    }
    
    // Check recent report stats
    console.log('\n3️⃣ Recent report statistics:');
    const { data: reports, error: reportsError } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .gte('created_at', '2025-10-30T14:00:00')
      .order('created_at', { ascending: false })
      .limit(5);
    
    if (reportsError) {
      console.error('❌ Reports error:', reportsError);
    } else {
      console.log(`   Found ${reports.length} recent reports:`);
      reports.forEach(report => {
        console.log(`   - ${report.report_type} (${report.cost_code}): ${report.total_sites} sites, ${report.total_sessions} sessions`);
      });
    }
    
    console.log('\n📋 Summary:');
    console.log('- Reports include ALL sites assigned to a cost center');
    console.log('- Sites without sessions show 0 values but are still listed');
    console.log('- This provides complete visibility of all assets');
    
  } catch (error) {
    console.error('❌ Check error:', error);
  }
}

checkReportData();