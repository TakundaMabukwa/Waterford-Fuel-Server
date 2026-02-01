require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testBucket() {
  try {
    const { data, error } = await supabase.storage.listBuckets();
    
    if (error) throw error;
    
    console.log('✅ Available buckets:', data.map(b => b.name));
    
    const { data: files, error: listError } = await supabase.storage
      .from('reports')
      .list();
    
    if (listError) throw listError;
    
    console.log('📁 Files in reports bucket:', files.length);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

testBucket();