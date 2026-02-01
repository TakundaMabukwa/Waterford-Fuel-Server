require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugAllSites() {
  try {
    console.log('🔍 Debugging ALL sites issue...\n');
    
    // Check total sites in vehicle lookup
    console.log('1️⃣ Total sites in vehicle lookup:');
    const { data: allSites, error: allError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('*')
      .order('plate', { ascending: true });
    
    if (allError) {
      console.error('❌ Error:', allError);
    } else {
      console.log(`   Found ${allSites.length} total sites:`);
      const byCostCode = {};
      allSites.forEach(site => {
        const cc = site.cost_code || 'NULL';
        if (!byCostCode[cc]) byCostCode[cc] = [];
        byCostCode[cc].push(site.plate);
      });
      
      Object.entries(byCostCode).forEach(([cc, sites]) => {
        console.log(`   ${cc}: ${sites.length} sites - ${sites.join(', ')}`);
      });
    }
    
    // Check what the report generator sees for ALL
    console.log('\n2️⃣ What report generator sees for ALL:');
    const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');
    
    try {
      const result = await ExcelReportGenerator.generateDailyReport(
        new Date('2025-10-29'), 
        null  // null = ALL cost codes
      );
      
      console.log(`   Report generated with ${result.stats.total_sites} sites`);
      console.log(`   Sessions: ${result.stats.total_sessions}`);
      console.log(`   Hours: ${result.stats.total_operating_hours}`);
    } catch (error) {
      console.error('❌ Report generation error:', error.message);
    }
    
    // Check sessions for 2025-10-29
    console.log('\n3️⃣ Sessions for 2025-10-29:');
    const { data: sessions, error: sessError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('branch, cost_code, operating_hours, total_usage')
      .gte('session_date', '2025-10-29')
      .lte('session_date', '2025-10-29')
      .order('branch', { ascending: true });
    
    if (sessError) {
      console.error('❌ Sessions error:', sessError);
    } else {
      console.log(`   Found ${sessions.length} sessions:`);
      const sessionSites = new Set();
      sessions.forEach(session => {
        sessionSites.add(session.branch);
        console.log(`   - ${session.branch} (${session.cost_code}): ${session.operating_hours}h`);
      });
      console.log(`   Unique sites with sessions: ${sessionSites.size}`);
    }
    
  } catch (error) {
    console.error('❌ Debug error:', error);
  }
}

debugAllSites();