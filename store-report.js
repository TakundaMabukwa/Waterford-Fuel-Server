require('dotenv').config();
const { supabase } = require('./supabase-client');

async function storeGeneratedReport() {
  try {
    console.log('💾 Storing generated report...');
    
    const reportData = {
      cost_code: 'KFC-0001-0001-0003',
      report_type: 'daily',
      report_url: 'https://energyrite-reports.s3.amazonaws.com/daily-report-2025-10-28.pdf',
      report_date: new Date().toISOString().split('T')[0],
      file_size: 2048576, // 2MB example
      status: 'generated'
    };
    
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .insert([reportData])
      .select();
    
    if (error) {
      throw new Error(`Database error: ${error.message}`);
    }
    
    console.log('✅ Report stored successfully!');
    console.log('📄 Report Details:');
    console.log(`   ID: ${data[0].id}`);
    console.log(`   Cost Code: ${data[0].cost_code}`);
    console.log(`   Type: ${data[0].report_type}`);
    console.log(`   Date: ${data[0].report_date}`);
    console.log(`   URL: ${data[0].report_url}`);
    console.log(`   Size: ${(data[0].file_size / 1024 / 1024).toFixed(2)} MB`);
    console.log(`   Generated: ${data[0].generated_at}`);
    
  } catch (error) {
    console.error('❌ Error storing report:', error.message);
  }
}

storeGeneratedReport();