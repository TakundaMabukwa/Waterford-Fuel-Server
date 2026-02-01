require('dotenv').config();
const axios = require('axios');

async function testSingleSiteExcel() {
  console.log('📊 Testing Single Site Excel Report...\n');
  
  try {
    const response = await axios.post('http://localhost:4000/api/energy-rite/excel-reports/generate', {
      report_type: 'daily',
      target_date: '2025-10-30',
      site_id: 'BALLYCLARE'
    });
    
    console.log('✅ Single site Excel report generated!');
    console.log('📄 File:', response.data.data.file_name);
    console.log('📊 Stats:', response.data.data.stats);
    
  } catch (error) {
    console.error('❌ Test failed:', error.response?.data || error.message);
  }
}

testSingleSiteExcel();