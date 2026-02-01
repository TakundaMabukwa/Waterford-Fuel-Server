require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkLatestReports() {
  try {
    console.log('📊 Checking latest report statistics...\n');
    
    const { data: reports, error } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .gte('created_at', '2025-10-30T14:35:00')
      .order('created_at', { ascending: false })
      .limit(5);
    
    if (error) {
      console.error('❌ Error:', error);
      return;
    }
    
    console.log(`Found ${reports.length} recent reports:\n`);
    
    reports.forEach((report, index) => {
      console.log(`📋 Report ${index + 1}:`);
      console.log(`   Type: ${report.report_type}`);
      console.log(`   Cost Code: ${report.cost_code}`);
      console.log(`   Date: ${report.report_date}`);
      console.log(`   Sites: ${report.total_sites}`);
      console.log(`   Sessions: ${report.total_sessions}`);
      console.log(`   Hours: ${report.total_operating_hours}`);
      console.log(`   File: ${report.file_name}`);
      console.log(`   Created: ${report.created_at}`);
      console.log('');
    });
    
    // Check if Brian's report now has all sites
    const brianReport = reports.find(r => r.cost_code === 'ALL');
    if (brianReport) {
      console.log('🎯 Brian\'s Report (ALL cost codes):');
      console.log(`   ✅ Total Sites: ${brianReport.total_sites} (should be 81)`);
      console.log(`   ✅ Sessions: ${brianReport.total_sessions}`);
      console.log(`   ✅ Hours: ${brianReport.total_operating_hours}`);
      
      if (brianReport.total_sites >= 80) {
        console.log('   🎉 SUCCESS: Brian now gets ALL sites!');
      } else {
        console.log('   ❌ Still missing sites');
      }
    }
    
  } catch (error) {
    console.error('❌ Check error:', error);
  }
}

checkLatestReports();