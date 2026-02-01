require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function testReportsBucket() {
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportContent = JSON.stringify(response.data, null, 2);
    
    const filename = `test-${Date.now()}.json`;
    
    const { data, error } = await supabase.storage
      .from('reports')
      .upload(filename, reportContent, { upsert: true });
    
    if (error) throw error;
    
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl(filename);
    
    console.log('✅ Uploaded to reports bucket:', publicUrl);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testReportsBucket();