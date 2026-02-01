require('dotenv').config();
const controller = require('./controllers/energy-rite/energyRiteCostCenterReportController');

// Mock request and response objects
const mockReq = {
  query: {
    cost_code: 'KFC-0001-0001-0002-0002',
    date: '2024-12-28'
  }
};

const mockRes = {
  status: (code) => ({
    json: (data) => {
      console.log(`📊 Response Status: ${code}`);
      console.log('📊 Response Data:', JSON.stringify(data, null, 2));
      return data;
    }
  })
};

async function testDirectEndpoint() {
  try {
    console.log('🧪 Testing direct controller endpoint...');
    
    // Test daily report generation
    await controller.generateDailyReport(mockReq, mockRes);
    
    console.log('\n🔍 Testing stored reports fetch...');
    
    // Test stored reports fetch
    const fetchReq = { query: { limit: 5, cost_code: 'KFC-0001-0001-0002-0002' } };
    await controller.getStoredReports(fetchReq, mockRes);
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testDirectEndpoint();