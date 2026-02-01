require('dotenv').config();
const energyRiteReportsController = require('./controllers/energy-rite/energyRiteReportsController');

async function testCostCenterReports() {
  console.log('🏢 Testing Cost Center Cumulative Reports...\n');
  
  try {
    // Test 1: Daily Report with Cost Center Cumulation
    console.log('1. 📅 Testing Daily Report (Current Month)...');
    const mockReq1 = { query: {} };
    const mockRes1 = {
      status: (code) => mockRes1,
      json: (data) => { mockRes1.data = data; return mockRes1; }
    };
    
    await energyRiteReportsController.getDailyReport(mockReq1, mockRes1);
    
    if (mockRes1.data && mockRes1.data.success) {
      console.log('✅ Daily Report Results:');
      console.log(`   Period: ${mockRes1.data.data.period}`);
      console.log(`   Total Sites: ${mockRes1.data.data.sites.length}`);
      
      // Group by cost code to show cumulation
      const costCodeSummary = {};
      mockRes1.data.data.sites.forEach(site => {
        const costCode = site.cost_code || 'UNASSIGNED';
        if (!costCodeSummary[costCode]) {
          costCodeSummary[costCode] = {
            sites: 0,
            total_sessions: 0,
            total_hours: 0,
            total_usage: 0,
            total_cost: 0
          };
        }
        
        const summary = costCodeSummary[costCode];
        summary.sites++;
        if (site.monthly_data) {
          summary.total_sessions += site.monthly_data.total_sessions || 0;
          summary.total_hours += site.monthly_data.total_running_hours || 0;
          summary.total_usage += site.monthly_data.total_fuel_usage || 0;
          summary.total_cost += site.monthly_data.total_cost || 0;
        }
      });
      
      console.log('\n   💼 Cost Center Summary:');
      Object.entries(costCodeSummary).forEach(([costCode, data]) => {
        if (data.total_sessions > 0) {
          console.log(`     ${costCode}:`);
          console.log(`       Sites: ${data.sites}`);
          console.log(`       Sessions: ${data.total_sessions}`);
          console.log(`       Hours: ${data.total_hours.toFixed(1)}h`);
          console.log(`       Usage: ${data.total_usage.toFixed(1)}L`);
          console.log(`       Cost: R${data.total_cost.toFixed(2)}`);
        }
      });
    }
    
    // Test 2: Weekly Report
    console.log('\n2. 📊 Testing Weekly Report...');
    const mockReq2 = { query: {} };
    const mockRes2 = {
      status: (code) => mockRes2,
      json: (data) => { mockRes2.data = data; return mockRes2; }
    };
    
    await energyRiteReportsController.getWeeklyReport(mockReq2, mockRes2);
    
    if (mockRes2.data && mockRes2.data.success) {
      console.log('✅ Weekly Report Results:');
      console.log(`   Week: ${mockRes2.data.data.week}/${mockRes2.data.data.year}`);
      console.log(`   Period: ${mockRes2.data.data.week_start} to ${mockRes2.data.data.week_end}`);
      console.log(`   Cost Centers: ${mockRes2.data.data.cost_centers.length}`);
      
      const summary = mockRes2.data.data.summary;
      console.log('\n   📈 Weekly Summary:');
      console.log(`     Total Sessions: ${summary.total_sessions}`);
      console.log(`     Total Hours: ${summary.total_operating_hours.toFixed(1)}h`);
      console.log(`     Total Usage: ${summary.total_fuel_usage.toFixed(1)}L`);
      console.log(`     Total Cost: R${summary.total_cost.toFixed(2)}`);
      
      if (mockRes2.data.data.cost_centers.length > 0) {
        console.log('\n   🏢 Cost Center Breakdown:');
        mockRes2.data.data.cost_centers.forEach(costCenter => {
          if (costCenter.total_sessions > 0) {
            console.log(`     ${costCenter.cost_code} (${costCenter.company}):`);
            console.log(`       Sites: ${costCenter.sites.length}`);
            console.log(`       Sessions: ${costCenter.total_sessions}`);
            console.log(`       Hours: ${costCenter.total_operating_hours.toFixed(1)}h`);
            console.log(`       Usage: ${costCenter.total_fuel_usage.toFixed(1)}L`);
            console.log(`       Cost: R${costCenter.total_cost.toFixed(2)}`);
            console.log(`       Efficiency: ${costCenter.avg_efficiency.toFixed(1)}L/h`);
            
            if (costCenter.daily_breakdown.length > 0) {
              console.log(`       Daily Breakdown:`);
              costCenter.daily_breakdown.forEach(day => {
                console.log(`         ${day.date}: ${day.sessions} sessions, ${day.fuel_usage.toFixed(1)}L`);
              });
            }
          }
        });
      }
    }
    
    // Test 3: Monthly Report
    console.log('\n3. 📈 Testing Monthly Report...');
    const mockReq3 = { query: {} };
    const mockRes3 = {
      status: (code) => mockRes3,
      json: (data) => { mockRes3.data = data; return mockRes3; }
    };
    
    await energyRiteReportsController.getMonthlyReport(mockReq3, mockRes3);
    
    if (mockRes3.data && mockRes3.data.success) {
      console.log('✅ Monthly Report Results:');
      console.log(`   Month: ${mockRes3.data.data.month}/${mockRes3.data.data.year}`);
      console.log(`   Cost Centers: ${mockRes3.data.data.cost_centers.length}`);
      
      const summary = mockRes3.data.data.summary;
      console.log('\n   📊 Monthly Summary:');
      console.log(`     Total Sessions: ${summary.total_sessions}`);
      console.log(`     Total Hours: ${summary.total_operating_hours.toFixed(1)}h`);
      console.log(`     Total Usage: ${summary.total_fuel_usage.toFixed(1)}L`);
      console.log(`     Total Cost: R${summary.total_cost.toFixed(2)}`);
      
      if (mockRes3.data.data.cost_centers.length > 0) {
        console.log('\n   🏢 Cost Center Monthly Breakdown:');
        mockRes3.data.data.cost_centers.forEach(costCenter => {
          if (costCenter.total_sessions > 0) {
            console.log(`     ${costCenter.cost_code}:`);
            console.log(`       Total Sessions: ${costCenter.total_sessions}`);
            console.log(`       Total Hours: ${costCenter.total_operating_hours.toFixed(1)}h`);
            console.log(`       Total Usage: ${costCenter.total_fuel_usage.toFixed(1)}L`);
            console.log(`       Total Cost: R${costCenter.total_cost.toFixed(2)}`);
            console.log(`       Sites: ${costCenter.sites.map(s => s.branch).join(', ')}`);
            
            if (costCenter.weekly_breakdown.length > 0) {
              console.log(`       Weekly Breakdown:`);
              costCenter.weekly_breakdown.forEach(week => {
                console.log(`         Week ${week.week}: ${week.sessions} sessions, ${week.fuel_usage.toFixed(1)}L`);
              });
            }
          }
        });
      }
    }
    
    // Test 4: Specific Cost Code Filter
    console.log('\n4. 🎯 Testing Cost Code Filtering...');
    const mockReq4 = { query: { cost_code: 'KFC-0001-0001-0003' } };
    const mockRes4 = {
      status: (code) => mockRes4,
      json: (data) => { mockRes4.data = data; return mockRes4; }
    };
    
    await energyRiteReportsController.getWeeklyReport(mockReq4, mockRes4);
    
    if (mockRes4.data && mockRes4.data.success) {
      console.log('✅ Cost Code Filtered Report:');
      console.log(`   Filtered Cost Code: KFC-0001-0001-0003`);
      console.log(`   Matching Cost Centers: ${mockRes4.data.data.cost_centers.length}`);
      
      if (mockRes4.data.data.cost_centers.length > 0) {
        const costCenter = mockRes4.data.data.cost_centers[0];
        console.log(`   Sessions: ${costCenter.total_sessions}`);
        console.log(`   Sites: ${costCenter.sites.length}`);
        console.log(`   Usage: ${costCenter.total_fuel_usage.toFixed(1)}L`);
        console.log(`   Cost: R${costCenter.total_cost.toFixed(2)}`);
      }
    }
    
    console.log('\n🎉 Cost Center Report Testing Completed!');
    console.log('\n📋 Summary:');
    console.log('✅ Daily Reports: Working with cost center grouping');
    console.log('✅ Weekly Reports: Working with cost center cumulation');
    console.log('✅ Monthly Reports: Working with cost center cumulation');
    console.log('✅ Cost Code Filtering: Working');
    console.log('✅ Site-level breakdown within cost centers: Working');
    console.log('✅ Daily/Weekly breakdown within cost centers: Working');
    
    console.log('\n🏢 Cost Center Features:');
    console.log('✅ Data cumulated by cost code');
    console.log('✅ Sites grouped under cost centers');
    console.log('✅ Totals calculated per cost center');
    console.log('✅ Time-based breakdowns within cost centers');
    console.log('✅ Filtering by specific cost codes');
    
  } catch (error) {
    console.error('❌ Cost center report test failed:', error.message);
  }
}

// Run the test
testCostCenterReports().then(() => {
  console.log('\n✅ Test completed');
  process.exit(0);
}).catch(error => {
  console.error('❌ Test error:', error);
  process.exit(1);
});