require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function storeInBucket() {
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportContent = JSON.stringify(response.data, null, 2);
    
    const date = new Date().toISOString().split('T')[0];
    const filename = `daily-report-${date}.json`;
    
    const { data, error } = await supabase.storage
      .from('reports')
      .upload(filename, reportContent, {
        contentType: 'application/json',
        upsert: true
      });
    
    if (error) throw error;
    
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl(filename);
    
    const { data: dbData, error: dbError } = await supabase
      .from('energy_rite_generated_reports')
      .insert({
        cost_code: `BUCKET-${Date.now()}`,
        report_type: 'daily',
        report_url: publicUrl,
        report_date: date,
        file_size: Buffer.byteLength(reportContent),
        status: 'generated'
      })
      .select();
    
    if (dbError) throw dbError;
    
    console.log('✅ Report stored in bucket:', publicUrl);
    console.log('📄 Database ID:', dbData[0].id);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

storeInBucket();