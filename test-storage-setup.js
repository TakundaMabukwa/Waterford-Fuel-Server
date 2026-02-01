require('dotenv').config();
const { supabase } = require('./supabase-client');

async function setupStorage() {
  try {
    console.log('🔧 Setting up Supabase storage...');
    
    // Create bucket if it doesn't exist
    const { data: buckets, error: listError } = await supabase.storage.listBuckets();
    
    if (listError) {
      console.error('❌ Error listing buckets:', listError);
      return;
    }
    
    const bucketExists = buckets.find(bucket => bucket.name === 'energyrite-reports');
    
    if (!bucketExists) {
      console.log('📦 Creating energyrite-reports bucket...');
      const { data, error } = await supabase.storage.createBucket('energyrite-reports', {
        public: true,
        allowedMimeTypes: ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/json'],
        fileSizeLimit: 50 * 1024 * 1024 // 50MB
      });
      
      if (error) {
        console.error('❌ Error creating bucket:', error);
        return;
      }
      
      console.log('✅ Bucket created successfully');
    } else {
      console.log('✅ Bucket already exists');
    }
    
    // Test upload
    console.log('🧪 Testing file upload...');
    const testContent = 'Test file content';
    const testPath = `test/test-${Date.now()}.txt`;
    
    const { data: uploadData, error: uploadError } = await supabase.storage
      .from('energyrite-reports')
      .upload(testPath, testContent, {
        contentType: 'text/plain',
        upsert: true
      });
    
    if (uploadError) {
      console.error('❌ Upload error:', uploadError);
      return;
    }
    
    console.log('✅ Test upload successful');
    
    // Get public URL
    const { data: { publicUrl } } = supabase.storage
      .from('energyrite-reports')
      .getPublicUrl(testPath);
    
    console.log('🔗 Public URL:', publicUrl);
    
    // Clean up test file
    await supabase.storage
      .from('energyrite-reports')
      .remove([testPath]);
    
    console.log('🧹 Test file cleaned up');
    console.log('🎉 Storage setup complete!');
    
  } catch (error) {
    console.error('❌ Setup error:', error);
  }
}

setupStorage();