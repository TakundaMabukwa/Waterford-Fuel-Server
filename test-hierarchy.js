require('dotenv').config();
const costCenterAccess = require('./helpers/cost-center-access');

async function testHierarchy() {
  try {
    console.log('🏗️ Testing Cost Center Hierarchy...\n');
    
    // Test different levels
    const testCases = [
      'KFC-0001-0001-0002',           // Parent - should get all children
      'KFC-0001-0001-0002-0001',     // Child - should get grandchildren
      'KFC-0001-0001-0002-0004',     // Leaf - should get only itself
      'KFC-0001-0001-0003'           // Parent with many children
    ];
    
    for (const costCode of testCases) {
      console.log(`🔍 Testing access for: ${costCode}`);
      const accessible = await costCenterAccess.getAccessibleCostCenters(costCode);
      console.log(`   Accessible: ${accessible.length} cost centers`);
      accessible.forEach(cc => console.log(`   - ${cc}`));
      console.log('');
    }
    
    // Test report generation with hierarchy
    console.log('📊 Testing report with hierarchy...');
    const ExcelReportGenerator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');
    
    const result = await ExcelReportGenerator.generateDailyReport(
      new Date('2025-10-30'), 
      'KFC-0001-0001-0002'  // Parent should get ALL children
    );
    
    console.log(`✅ Report for KFC-0001-0001-0002 (parent):`);
    console.log(`   Sites: ${result.stats.total_sites}`);
    console.log(`   Sessions: ${result.stats.total_sessions}`);
    console.log(`   Should include all KFC-0001-0001-0002-XXXX sites`);
    
  } catch (error) {
    console.error('❌ Test error:', error);
  }
}

testHierarchy();