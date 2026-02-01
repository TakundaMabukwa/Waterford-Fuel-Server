require('dotenv').config();
const { supabase } = require('./supabase-client');
const ExcelJS = require('exceljs');

async function testExcelUpload() {
  try {
    console.log('📊 Testing Excel file upload...');
    
    // Create a simple Excel workbook
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Test');
    worksheet.addRow(['Test', 'Data']);
    worksheet.addRow(['Hello', 'World']);
    
    // Convert to buffer
    const buffer = await workbook.xlsx.writeBuffer();
    
    // Upload to Supabase
    const fileName = `test-report-${Date.now()}.xlsx`;
    const bucketPath = `test/${fileName}`;
    
    const { data: uploadData, error: uploadError } = await supabase.storage
      .from('energyrite-reports')
      .upload(bucketPath, buffer, {
        contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        upsert: true
      });
    
    if (uploadError) {
      console.error('❌ Upload error:', uploadError);
      return;
    }
    
    console.log('✅ Excel upload successful:', uploadData.path);
    
    // Get public URL
    const { data: { publicUrl } } = supabase.storage
      .from('energyrite-reports')
      .getPublicUrl(bucketPath);
    
    console.log('🔗 Public URL:', publicUrl);
    
    // Test if URL is accessible
    console.log('🧪 Testing URL accessibility...');
    const response = await fetch(publicUrl);
    console.log('📊 Response status:', response.status);
    console.log('📊 Response headers:', Object.fromEntries(response.headers.entries()));
    
    console.log('🎉 Excel upload test complete!');
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testExcelUpload();