const axios = require('axios');

class APIReportTest {
  constructor() {
    this.baseURL = 'http://localhost:4000';
  }
  
  async testDailyReportAPI() {
    console.log('\n=== TESTING DAILY REPORT API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/daily`, {
        params: {
          month: 10,
          year: 2025
        }
      });
      
      console.log('✅ Daily Report API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success && response.data.data) {
        const report = response.data.data;
        console.log(`📅 Report Date: ${report.report_date}`);
        console.log(`📊 Period: ${report.period}`);
        console.log(`🏢 Total Sites: ${report.sites.length}`);
        
        // Show first few sites
        report.sites.slice(0, 3).forEach(site => {
          console.log(`   Site: ${site.branch} | Fuel: ${site.current_fuel_level}L | Sessions: ${site.monthly_data.total_sessions}`);
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Daily Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testWeeklyReportAPI() {
    console.log('\n=== TESTING WEEKLY REPORT API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/weekly`, {
        params: {
          week: 43,
          year: 2025
        }
      });
      
      console.log('✅ Weekly Report API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success && response.data.data) {
        const report = response.data.data;
        console.log(`📅 Week: ${report.week_start} to ${report.week_end}`);
        console.log(`🏢 Cost Centers: ${report.cost_centers.length}`);
        console.log(`📊 Total Sessions: ${report.summary.total_sessions}`);
        console.log(`⏱️  Total Hours: ${report.summary.total_operating_hours.toFixed(2)}`);
        console.log(`⛽ Total Fuel: ${report.summary.total_fuel_usage.toFixed(2)}L`);
        
        // Show cost center breakdown
        report.cost_centers.forEach(cc => {
          console.log(`   Cost Center ${cc.cost_code}: ${cc.total_sessions} sessions, ${cc.sites.length} sites`);
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Weekly Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testMonthlyReportAPI() {
    console.log('\n=== TESTING MONTHLY REPORT API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/monthly`, {
        params: {
          month: 10,
          year: 2025
        }
      });
      
      console.log('✅ Monthly Report API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success && response.data.data) {
        const report = response.data.data;
        console.log(`📅 Period: ${report.period}`);
        console.log(`🏢 Cost Centers: ${report.cost_centers.length}`);
        console.log(`📊 Total Sessions: ${report.summary.total_sessions}`);
        console.log(`⏱️  Total Hours: ${report.summary.total_operating_hours.toFixed(2)}`);
        console.log(`⛽ Total Fuel: ${report.summary.total_fuel_usage.toFixed(2)}L`);
        console.log(`💰 Total Cost: $${report.summary.total_cost.toFixed(2)}`);
        
        // Show cost center breakdown with weekly data
        report.cost_centers.forEach(cc => {
          console.log(`   Cost Center ${cc.cost_code}:`);
          console.log(`     Sessions: ${cc.total_sessions} | Sites: ${cc.sites.length}`);
          console.log(`     Efficiency: ${cc.avg_efficiency.toFixed(2)}L/hr`);
          console.log(`     Weekly Breakdown: ${cc.weekly_breakdown.length} weeks`);
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Monthly Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testActivityReportAPI() {
    console.log('\n=== TESTING ACTIVITY REPORT API ===');
    
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/activity`, {
        params: {
          date: today
        }
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
        
        // Show site activity
        report.sites.forEach(site => {
          console.log(`   Site ${site.branch}: ${site.sessions.length} sessions`);
          if (site.sessions.length > 0) {
            const firstSession = site.sessions[0];
            console.log(`     First Session: ${firstSession.start_time} - ${firstSession.end_time || 'ongoing'}`);
            console.log(`     Fuel Usage: ${firstSession.fuel_usage}L | Status: ${firstSession.status}`);
          }
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Activity Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testTodaysSessionsAPI() {
    console.log('\n=== TESTING TODAY\'S SESSIONS API ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/today`);
      
      console.log('✅ Today\'s Sessions API Response:');
      console.log(`Status: ${response.status}`);
      console.log(`Success: ${response.data.success}`);
      
      if (response.data.success) {
        console.log(`📅 Date: ${response.data.date}`);
        console.log(`📊 Total Sessions: ${response.data.total_sessions}`);
        
        if (response.data.sessions && response.data.sessions.length > 0) {
          console.log('Sessions:');
          response.data.sessions.forEach(session => {
            console.log(`   ${session.branch} | ${session.cost_code} | ${session.session_status}`);
            console.log(`     Hours: ${session.operating_hours} | Usage: ${session.total_usage}L`);
          });
        }
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Today\'s Sessions API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testServerHealth() {
    console.log('\n=== TESTING SERVER HEALTH ===');
    
    try {
      const response = await axios.get(`${this.baseURL}/health`);
      
      console.log('✅ Server Health Check:');
      console.log(`Status: ${response.status}`);
      console.log(`Response: ${JSON.stringify(response.data, null, 2)}`);
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Server Health Check Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async runAllAPITests() {
    console.log('🚀 Starting EnergyRite API Report Tests...\n');
    
    try {
      // Test server health first
      await this.testServerHealth();
      
      // Test all report APIs
      const todaysSessions = await this.testTodaysSessionsAPI();
      const activityReport = await this.testActivityReportAPI();
      const dailyReport = await this.testDailyReportAPI();
      const weeklyReport = await this.testWeeklyReportAPI();
      const monthlyReport = await this.testMonthlyReportAPI();
      
      console.log('\n=== API TEST SUMMARY ===');
      console.log('✅ All API report tests completed successfully!');
      console.log(`📊 Today's Sessions: ${todaysSessions.total_sessions || 0} sessions`);
      console.log(`📊 Activity Report: ${activityReport.data?.summary?.total_sessions || 0} sessions`);
      console.log(`📊 Daily Report: ${dailyReport.data?.sites?.length || 0} sites`);
      console.log(`📊 Weekly Report: ${weeklyReport.data?.summary?.total_sessions || 0} sessions`);
      console.log(`📊 Monthly Report: ${monthlyReport.data?.summary?.total_sessions || 0} sessions`);
      
      return {
        success: true,
        results: {
          health: true,
          todaysSessions,
          activityReport,
          dailyReport,
          weeklyReport,
          monthlyReport
        }
      };
      
    } catch (error) {
      console.error('\n❌ API Test Suite Failed:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }
}

// Run tests if called directly
if (require.main === module) {
  const tester = new APIReportTest();
  tester.runAllAPITests()
    .then(result => {
      if (result.success) {
        console.log('\n🎉 All API tests passed!');
        process.exit(0);
      } else {
        console.log('\n💥 API tests failed!');
        process.exit(1);
      }
    })
    .catch(error => {
      console.error('💥 API test execution failed:', error);
      process.exit(1);
    });
}

module.exports = APIReportTest;