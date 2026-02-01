require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function createBucketAndStore() {
  try {
    // Create bucket if not exists
    const { data: bucket, error: bucketError } = await supabase.storage.createBucket('reports', {
      public: true
    });
    
    if (bucketError && !bucketError.message.includes('already exists')) {
      throw bucketError;
    }
    
    // Generate report
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportContent = JSON.stringify(response.data, null, 2);
    
    const date = new Date().toISOString().split('T')[0];
    const filename = `daily-report-${date}.json`;
    
    // Upload to bucket
    const { data, error } = await supabase.storage
      .from('reports')
      .upload(filename, reportContent, {
        contentType: 'application/json',
        upsert: true
      });
    
    if (error) throw error;
    
    // Get public URL
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl(filename);
    
    // Store in database
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
    
    console.log('✅ Report stored:', publicUrl);
    console.log('📄 Record ID:', dbData[0]?.id);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

createBucketAndStore();