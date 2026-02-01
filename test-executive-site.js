require('dotenv').config();
const axios = require('axios');

async function testExecutiveSite() {
  console.log('📊 Testing Executive Dashboard with Site Filter...\n');
  
  try {
    const response = await axios.get('http://localhost:4000/api/energy-rite/executive-dashboard', {
      params: { 
        site_id: 'MILNERTON',
        days: 7
      }
    });
    
    console.log('✅ Executive dashboard with site filter working!');
    console.log('📅 Period:', response.data.data.period);
    console.log('🚗 Fleet Overview:', response.data.data.fleet_overview);
    console.log('⚙️ Operational Metrics:', response.data.data.operational_metrics);
    console.log('🏆 Top Sites:', response.data.data.top_performing_sites.length);
    console.log('💡 Key Insights:', response.data.data.key_insights);
    
  } catch (error) {
    console.error('❌ Executive dashboard site test failed:', error.response?.data || error.message);
  }
}

testExecutiveSite();