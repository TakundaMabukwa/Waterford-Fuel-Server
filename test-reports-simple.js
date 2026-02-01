const axios = require('axios');

class SimpleReportTest {
  constructor() {
    this.baseURL = 'http://localhost:4000';
  }
  
  async testTodaysSessionsAPI() {
    console.log('\n=== TESTING TODAY\'S SESSIONS API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/today`);
      
      console.log('✅ Today\'s Sessions API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      console.log(`📅 Date: ${response.data.date}`);
      console.log(`📊 Total Sessions: ${response.data.total_sessions}`);
      
      if (response.data.sessions && response.data.sessions.length > 0) {
        console.log('\nSession Details:');
        response.data.sessions.forEach((session, index) => {
          console.log(`   ${index + 1}. ${session.branch} (${session.cost_code || 'N/A'})`);
          console.log(`      Status: ${session.session_status} | Hours: ${session.operating_hours || 0}`);
          console.log(`      Usage: ${session.total_usage || 0}L | Fill: ${session.total_fill || 0}L`);
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Today\'s Sessions API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testActivityReportAPI() {
    console.log('\n=== TESTING ACTIVITY REPORT API ===');
    
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/activity`, {
        params: { date: today }
      });
      
      console.log('✅ Activity Report API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success && response.data.data) {
        const report = response.data.data;
        console.log(`📅 Date: ${report.date}`);
        console.log(`📊 Summary:`);
        console.log(`   Total Sites: ${report.summary.total_sites}`);
        console.log(`   Total Sessions: ${report.summary.total_sessions}`);
        console.log(`   Completed: ${report.summary.completed_sessions}`);
        console.log(`   Ongoing: ${report.summary.ongoing_sessions}`);
        console.log(`   Total Hours: ${report.summary.total_operating_hours.toFixed(2)}`);
        console.log(`   Total Fuel Usage: ${report.summary.total_fuel_usage.toFixed(2)}L`);
        console.log(`   Total Cost: $${report.summary.total_cost.toFixed(2)}`);
        
        console.log('\nSite Activity:');
        report.sites.forEach((site, index) => {
          console.log(`   ${index + 1}. ${site.branch} - ${site.sessions.length} sessions`);
          console.log(`      Hours: ${site.total_operating_hours.toFixed(2)} | Usage: ${site.total_fuel_usage.toFixed(2)}L`);
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Activity Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testDailyReportAPI() {
    console.log('\n=== TESTING DAILY REPORT API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/daily`, {
        params: { month: 10, year: 2025 }
      });
      
      console.log('✅ Daily Report API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success && response.data.data) {
        const report = response.data.data;
        console.log(`📅 Period: ${report.period}`);
        console.log(`🏢 Total Sites: ${report.sites.length}`);
        
        const activeSites = report.sites.filter(site => site.monthly_data.total_sessions > 0);
        console.log(`🔥 Active Sites: ${activeSites.length}`);
        
        console.log('\nTop Active Sites:');
        activeSites.slice(0, 5).forEach((site, index) => {
          console.log(`   ${index + 1}. ${site.branch} (${site.cost_code})`);
          console.log(`      Sessions: ${site.monthly_data.total_sessions}`);
          console.log(`      Hours: ${site.monthly_data.total_running_hours.toFixed(2)}`);
          console.log(`      Usage: ${site.monthly_data.total_fuel_usage.toFixed(2)}L`);
          console.log(`      Cost: $${site.monthly_data.total_cost.toFixed(2)}`);
        });
        
        const totalSessions = activeSites.reduce((sum, site) => sum + site.monthly_data.total_sessions, 0);
        const totalUsage = activeSites.reduce((sum, site) => sum + site.monthly_data.total_fuel_usage, 0);
        const totalCost = activeSites.reduce((sum, site) => sum + site.monthly_data.total_cost, 0);
        
        console.log('\n📊 Monthly Totals:');
        console.log(`   Sessions: ${totalSessions} | Usage: ${totalUsage.toFixed(2)}L | Cost: $${totalCost.toFixed(2)}`);
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Daily Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testMonthlyReportAPI() {
    console.log('\n=== TESTING MONTHLY REPORT API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/monthly`, {
        params: { month: 10, year: 2025 }
      });
      
      console.log('✅ Monthly Report API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success && response.data.data) {
        const report = response.data.data;
        console.log(`📅 Period: ${report.period}`);
        console.log(`🏢 Cost Centers: ${report.cost_centers.length}`);
        console.log(`📊 Summary:`);
        console.log(`   Total Sessions: ${report.summary.total_sessions}`);
        console.log(`   Total Hours: ${report.summary.total_operating_hours.toFixed(2)}`);
        console.log(`   Total Fuel: ${report.summary.total_fuel_usage.toFixed(2)}L`);
        console.log(`   Total Cost: $${report.summary.total_cost.toFixed(2)}`);
        
        console.log('\nCost Center Breakdown:');
        report.cost_centers.forEach((cc, index) => {
          console.log(`   ${index + 1}. ${cc.cost_code} (${cc.company})`);
          console.log(`      Sessions: ${cc.total_sessions} | Sites: ${cc.sites.length}`);
          console.log(`      Hours: ${cc.total_operating_hours.toFixed(2)} | Usage: ${cc.total_fuel_usage.toFixed(2)}L`);
          console.log(`      Cost: $${cc.total_cost.toFixed(2)} | Efficiency: ${cc.avg_efficiency.toFixed(2)}L/hr`);
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Monthly Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async runAllTests() {
    console.log('🚀 Starting EnergyRite Report Tests...');
    
    try {
      const todaysSessions = await this.testTodaysSessionsAPI();
      const activityReport = await this.testActivityReportAPI();
      const dailyReport = await this.testDailyReportAPI();
      const monthlyReport = await this.testMonthlyReportAPI();
      
      console.log('\n=== TEST SUMMARY ===');
      console.log('✅ All report tests completed successfully!');
      console.log(`📊 Today's Sessions: ${todaysSessions.total_sessions || 0}`);
      console.log(`📊 Activity Report: ${activityReport.data?.summary?.total_sessions || 0} sessions`);
      console.log(`📊 Daily Report: ${dailyReport.data?.sites?.length || 0} sites`);
      console.log(`📊 Monthly Report: ${monthlyReport.data?.summary?.total_sessions || 0} sessions`);
      
      return { success: true };
      
    } catch (error) {
      console.error('\n❌ Test Suite Failed:', error.message);
      return { success: false, error: error.message };
    }
  }
}

// Run tests
if (require.main === module) {
  const tester = new SimpleReportTest();
  tester.runAllTests()
    .then(result => {
      if (result.success) {
        console.log('\n🎉 All tests passed!');
        process.exit(0);
      } else {
        console.log('\n💥 Tests failed!');
        process.exit(1);
      }
    })
    .catch(error => {
      console.error('💥 Test execution failed:', error);
      process.exit(1);
    });
}

module.exports = SimpleReportTest;