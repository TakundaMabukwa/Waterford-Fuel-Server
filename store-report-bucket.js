require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function generateAndStoreReport() {
  try {
    // Generate report data
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportContent = JSON.stringify(response.data, null, 2);
    
    // Create filename
    const date = new Date().toISOString().split('T')[0];
    const filename = `daily-report-${date}.json`;
    
    // Upload to Supabase bucket
    const { data, error } = await supabase.storage
      .from('energyrite-reports')
      .upload(filename, reportContent, {
        contentType: 'application/json',
        upsert: true
      });
    
    if (error) throw error;
    
    // Get public URL
    const { data: { publicUrl } } = supabase.storage
      .from('energyrite-reports')
      .getPublicUrl(filename);
    
    // Store metadata in database
    const { data: dbData, error: dbError } = await supabase
      .from('energy_rite_generated_reports')
      .upsert({
        cost_code: 'ALL',
        report_type: 'daily',
        report_url: publicUrl,
        report_date: date,
        file_size: Buffer.byteLength(reportContent),
        status: 'generated'
      })
      .select();
    
    if (dbError) throw dbError;
    
    console.log('✅ Report stored in bucket:', publicUrl);
    console.log('📄 Database record:', dbData[0].id);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

generateAndStoreReport();