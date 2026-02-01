require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function storeReportSimple() {
  try {
    // Generate report data
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportData = response.data;
    
    // Store directly in database with environment-based URL
    const date = new Date().toISOString().split('T')[0];
    // Use environment-based URL construction instead of hardcoded URL
    const baseUrl = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
    const projectRef = baseUrl.split('//')[1].split('.')[0];
    const mockUrl = `https://${projectRef}.supabase.co/storage/v1/object/public/reports/daily-report-${date}.json`;
    
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .upsert({
        cost_code: 'ALL',
        report_type: 'daily',
        report_url: mockUrl,
        report_date: date,
        file_size: JSON.stringify(reportData).length,
        status: 'generated'
      })
      .select();
    
    if (error) throw error;
    
    console.log('✅ Report metadata stored');
    console.log('📄 Record ID:', data[0].id);
    console.log('🔗 URL:', data[0].report_url);
    console.log('📊 Sites in report:', reportData.data.sites.length);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

storeReportSimple();