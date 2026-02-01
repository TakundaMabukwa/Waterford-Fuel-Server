require('dotenv').config();
const axios = require('axios');

async function testActivityExcelAll() {
  console.log('📊 Testing Activity Excel Report for All Cost Centers...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/activity-excel-reports/generate', {
      params: { 
        date: '2025-10-31'
      }
    });
    
    console.log('✅ Activity Excel report generated successfully!');
    console.log('📄 File Name:', response.data.data.file_name);
    console.log('📅 Date:', response.data.data.date);
    console.log('🏷️ Cost Code:', response.data.data.cost_code);
    console.log('🏪 Total Sites:', response.data.data.total_sites);
    console.log('📁 File Size:', (response.data.data.file_size / 1024).toFixed(2) + ' KB');
    console.log('🔗 Download URL:', response.data.data.download_url);
    
    console.log('\n✅ Excel report for all cost centers generated successfully!');
    
  } catch (error) {
    console.error('❌ Activity Excel test failed:', error.response?.data || error.message);
  }
}

testActivityExcelAll();