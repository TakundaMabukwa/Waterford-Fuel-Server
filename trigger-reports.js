const axios = require('axios');

async function triggerAutomatedReports() {
  console.log('🚀 Triggering Automated Reports...\n');
  
  try {
    const response = await axios.post('http://localhost:3000/api/energy-rite/reports/generate', {
      reportType: 'automated',
      trigger: 'manual'
    });
    
    console.log('✅ Reports triggered successfully!');
    console.log('📊 Response:', response.data);
    
  } catch (error) {
    console.error('❌ Failed to trigger reports:', error.response?.data || error.message);
  }
}

triggerAutomatedReports();