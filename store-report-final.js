require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

// Use service role for storage operations
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

async function storeReport() {
  try {
    // Generate report
    const response = await axios.get('http://localhost:4000/api/energy-rite/reports/daily');
    const reportContent = JSON.stringify(response.data, null, 2);
    
    const date = new Date().toISOString().split('T')[0];
    const filename = `daily-report-${date}.json`;
    
    // Upload to storage
    const { data, error } = await supabase.storage
      .from('reports')
      .upload(filename, reportContent, {
        contentType: 'application/json',
        upsert: true
      });
    
    if (error) {
      // Try creating bucket first
      await supabase.storage.createBucket('reports', { public: true });
      
      // Retry upload
      const { data: retryData, error: retryError } = await supabase.storage
        .from('reports')
        .upload(filename, reportContent, {
          contentType: 'application/json',
          upsert: true
        });
      
      if (retryError) throw retryError;
    }
    
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl(filename);
    
    console.log('✅ Report stored in bucket:', publicUrl);
    console.log('📄 File size:', Buffer.byteLength(reportContent), 'bytes');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

storeReport();