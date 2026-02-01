require('dotenv').config();
const { supabase } = require('./supabase-client');

async function createBucket() {
  try {
    const { data, error } = await supabase.storage.createBucket('reports', { public: true });
    
    if (error && !error.message.includes('already exists')) {
      throw error;
    }
    
    console.log('✅ Bucket created or exists');
    
    // Test upload
    const testContent = JSON.stringify({ test: 'data', date: new Date() });
    const { data: uploadData, error: uploadError } = await supabase.storage
      .from('reports')
      .upload('test.json', testContent, { upsert: true });
    
    if (uploadError) throw uploadError;
    
    const { data: { publicUrl } } = supabase.storage
      .from('reports')
      .getPublicUrl('test.json');
    
    console.log('✅ Test file uploaded:', publicUrl);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

createBucket();