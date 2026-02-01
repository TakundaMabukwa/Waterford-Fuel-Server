require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function storeUniqueReport() {
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportData = response.data;
    
    const timestamp = new Date().toISOString();
    const date = timestamp.split('T')[0];
    const time = timestamp.split('T')[1].split('.')[0].replace(/:/g, '-');
    
    const filename = `daily-report-${date}-${time}.json`;
    // Use environment-based URL construction instead of hardcoded URL
    const baseUrl = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
    const projectRef = baseUrl.split('//')[1].split('.')[0];
    const mockUrl = `https://${projectRef}.supabase.co/storage/v1/object/public/reports/${filename}`;
    
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .insert({
        cost_code: `ALL-${time}`,
        report_type: 'daily',
        report_url: mockUrl,
        report_date: date,
        file_size: JSON.stringify(reportData).length,
        status: 'generated'
      })
      .select();
    
    if (error) throw error;
    
    console.log('✅ Unique report stored in bucket simulation');
    console.log('📄 Record ID:', data[0].id);
    console.log('🔗 Bucket URL:', data[0].report_url);
    console.log('📊 Report contains:', reportData.data.sites.length, 'sites');
    console.log('💾 File size:', data[0].file_size, 'bytes');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

storeUniqueReport();