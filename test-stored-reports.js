require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testStoredReports() {
  try {
    console.log('📋 Testing stored reports query...');
    
    // Get all stored reports
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .eq('status', 'generated')
      .order('created_at', { ascending: false })
      .limit(10);
    
    if (error) {
      console.error('❌ Query error:', error);
      return;
    }
    
    console.log(`✅ Found ${data.length} stored reports:`);
    
    data.forEach((report, index) => {
      console.log(`\n📊 Report ${index + 1}:`);
      console.log(`   ID: ${report.id}`);
      console.log(`   Type: ${report.report_type}`);
      console.log(`   Cost Code: ${report.cost_code}`);
      console.log(`   Date: ${report.report_date}`);
      console.log(`   File: ${report.file_name}`);
      console.log(`   Size: ${report.file_size} bytes`);
      console.log(`   URL: ${report.report_url}`);
      console.log(`   Created: ${report.created_at}`);
      console.log(`   Stats: ${report.total_sites} sites, ${report.total_sessions} sessions`);
    });
    
    // Test filtering by cost code
    console.log('\n🔍 Testing filter by cost code...');
    const { data: filtered, error: filterError } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .eq('cost_code', 'KFC-0001-0001-0002-0002')
      .order('created_at', { ascending: false });
    
    if (filterError) {
      console.error('❌ Filter error:', filterError);
    } else {
      console.log(`✅ Found ${filtered.length} reports for KFC-0001-0001-0002-0002`);
    }
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testStoredReports();