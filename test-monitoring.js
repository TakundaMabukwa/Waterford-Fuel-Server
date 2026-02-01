require('dotenv').config();
const controller = require('./controllers/energy-rite/energyRiteMonitoringController');

// Mock request and response objects
const mockReq = {
  query: { days: 30, limit: 10 }
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

async function testMonitoring() {
  try {
    console.log('🔍 Testing Top Usage Sites...\n');
    await controller.getTopUsageSites(mockReq, mockRes);
    
    console.log('\n⏰ Testing Long Running Sites...\n');
    await controller.getLongRunningSites({ query: { hours: 24 } }, mockRes);
    
    console.log('\n📊 Testing Monitoring Dashboard...\n');
    await controller.getMonitoringDashboard({ query: {} }, mockRes);
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testMonitoring();