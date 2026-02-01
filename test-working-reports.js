const axios = require('axios');

class WorkingReportTest {
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
      
      if (response.data.success) {
        console.log(`📅 Date: ${response.data.date}`);
        console.log(`📊 Total Sessions: ${response.data.total_sessions}`);
        
        if (response.data.sessions && response.data.sessions.length > 0) {
          console.log('Sessions Details:');
          response.data.sessions.forEach((session, index) => {
            console.log(`   ${index + 1}. Branch: ${session.branch}`);
            console.log(`      Cost Code: ${session.cost_code || 'N/A'}`);
            console.log(`      Status: ${session.session_status}`);
            console.log(`      Hours: ${session.operating_hours || 0}`);
            console.log(`      Usage: ${session.total_usage || 0}L`);
            console.log(`      Fill: ${session.total_fill || 0}L`);
            console.log(`      Efficiency: ${session.liter_usage_per_hour || 0}L/hr`);
            console.log('');
          });
        }
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
        console.log(`   Total Fuel Filled: ${report.summary.total_fuel_filled.toFixed(2)}L`);
        console.log(`   Total Cost: $${report.summary.total_cost.toFixed(2)}`);
        
        console.log('\nSite Activity Details:');
        report.sites.forEach((site, index) => {
          console.log(`   ${index + 1}. Site: ${site.branch}`);
          console.log(`      Company: ${site.company}`);
          console.log(`      Cost Code: ${site.cost_code}`);
          console.log(`      Sessions: ${site.sessions.length}`);
          console.log(`      Total Hours: ${site.total_operating_hours.toFixed(2)}`);
          console.log(`      Total Usage: ${site.total_fuel_usage.toFixed(2)}L`);
          console.log(`      Total Filled: ${site.total_fuel_filled.toFixed(2)}L`);
          console.log(`      Total Cost: $${site.total_cost.toFixed(2)}`);
          
          if (site.sessions.length > 0) {
            console.log(`      Session Details:`);
            site.sessions.forEach((session, sessionIndex) => {
              console.log(`        ${sessionIndex + 1}. ${session.start_time} - ${session.end_time || 'ongoing'}`);
              console.log(`           Duration: ${session.duration_hours.toFixed(2)}h | Usage: ${session.fuel_usage}L | Status: ${session.status}`);
            });
          }
          console.log('');
        });
      }
      
      return response.data;
      
    } catch (error) {
      console.error('❌ Activity Report API Failed:', error.response?.data || error.message);
      throw error;
    }
  }
  
  async testDailyReportAPI() {
    console.log('\n=== TESTING DAILY REPORT API ===');\n    \n    try {\n      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/daily`, {\n        params: {\n          month: 10,\n          year: 2025\n        }\n      });\n      \n      console.log('✅ Daily Report API Response:');\n      console.log(`Status: ${response.status}`);\n      console.log(`Success: ${response.data.success}`);\n      \n      if (response.data.success && response.data.data) {\n        const report = response.data.data;\n        console.log(`📅 Report Date: ${report.report_date}`);\n        console.log(`📊 Period: ${report.period}`);\n        console.log(`🏢 Total Sites: ${report.sites.length}`);\n        \n        // Show sites with activity\n        const activeSites = report.sites.filter(site => site.monthly_data.total_sessions > 0);\n        console.log(`🔥 Active Sites: ${activeSites.length}`);\n        \n        console.log('\\nActive Sites Details:');\n        activeSites.slice(0, 5).forEach((site, index) => {\n          console.log(`   ${index + 1}. Site: ${site.branch}`);\n          console.log(`      Company: ${site.company}`);\n          console.log(`      Cost Code: ${site.cost_code}`);\n          console.log(`      Current Fuel: ${site.current_fuel_level}L`);\n          console.log(`      Engine Status: ${site.current_engine_status}`);\n          console.log(`      Monthly Data:`);\n          console.log(`        Sessions: ${site.monthly_data.total_sessions}`);\n          console.log(`        Hours: ${site.monthly_data.total_running_hours.toFixed(2)}`);\n          console.log(`        Usage: ${site.monthly_data.total_fuel_usage.toFixed(2)}L`);\n          console.log(`        Filled: ${site.monthly_data.total_fuel_filled.toFixed(2)}L`);\n          console.log(`        Cost: $${site.monthly_data.total_cost.toFixed(2)}`);\n          console.log(`        Efficiency: ${site.monthly_data.avg_efficiency.toFixed(2)}L/hr`);\n          console.log('');\n        });\n        \n        // Show summary stats\n        const totalSessions = report.sites.reduce((sum, site) => sum + site.monthly_data.total_sessions, 0);\n        const totalHours = report.sites.reduce((sum, site) => sum + site.monthly_data.total_running_hours, 0);\n        const totalUsage = report.sites.reduce((sum, site) => sum + site.monthly_data.total_fuel_usage, 0);\n        const totalCost = report.sites.reduce((sum, site) => sum + site.monthly_data.total_cost, 0);\n        \n        console.log('📊 Monthly Summary:');\n        console.log(`   Total Sessions: ${totalSessions}`);\n        console.log(`   Total Hours: ${totalHours.toFixed(2)}`);\n        console.log(`   Total Usage: ${totalUsage.toFixed(2)}L`);\n        console.log(`   Total Cost: $${totalCost.toFixed(2)}`);\n        console.log(`   Average Efficiency: ${totalHours > 0 ? (totalUsage / totalHours).toFixed(2) : 0}L/hr`);\n      }\n      \n      return response.data;\n      \n    } catch (error) {\n      console.error('❌ Daily Report API Failed:', error.response?.data || error.message);\n      throw error;\n    }\n  }\n  \n  async testMonthlyReportAPI() {\n    console.log('\\n=== TESTING MONTHLY REPORT API ===');\n    \n    try {\n      const response = await axios.get(`${this.baseURL}/api/energy-rite/reports/monthly`, {\n        params: {\n          month: 10,\n          year: 2025\n        }\n      });\n      \n      console.log('✅ Monthly Report API Response:');\n      console.log(`Status: ${response.status}`);\n      console.log(`Success: ${response.data.success}`);\n      \n      if (response.data.success && response.data.data) {\n        const report = response.data.data;\n        console.log(`📅 Period: ${report.period}`);\n        console.log(`🏢 Cost Centers: ${report.cost_centers.length}`);\n        console.log(`📊 Summary:`);\n        console.log(`   Total Sessions: ${report.summary.total_sessions}`);\n        console.log(`   Total Hours: ${report.summary.total_operating_hours.toFixed(2)}`);\n        console.log(`   Total Fuel: ${report.summary.total_fuel_usage.toFixed(2)}L`);\n        console.log(`   Total Cost: $${report.summary.total_cost.toFixed(2)}`);\n        \n        console.log('\\nCost Center Breakdown:');\n        report.cost_centers.forEach((cc, index) => {\n          console.log(`   ${index + 1}. Cost Center: ${cc.cost_code}`);\n          console.log(`      Company: ${cc.company}`);\n          console.log(`      Sessions: ${cc.total_sessions}`);\n          console.log(`      Sites: ${cc.sites.length}`);\n          console.log(`      Hours: ${cc.total_operating_hours.toFixed(2)}`);\n          console.log(`      Usage: ${cc.total_fuel_usage.toFixed(2)}L`);\n          console.log(`      Cost: $${cc.total_cost.toFixed(2)}`);\n          console.log(`      Efficiency: ${cc.avg_efficiency.toFixed(2)}L/hr`);\n          console.log(`      Weekly Breakdown: ${cc.weekly_breakdown.length} weeks`);\n          \n          // Show top sites for this cost center\n          const topSites = cc.sites.slice(0, 3);\n          if (topSites.length > 0) {\n            console.log(`      Top Sites:`);\n            topSites.forEach(site => {\n              console.log(`        - ${site.branch}: ${site.sessions} sessions, ${site.fuel_usage.toFixed(2)}L`);\n            });\n          }\n          console.log('');\n        });\n      }\n      \n      return response.data;\n      \n    } catch (error) {\n      console.error('❌ Monthly Report API Failed:', error.response?.data || error.message);\n      throw error;\n    }\n  }\n  \n  async runWorkingTests() {\n    console.log('🚀 Starting EnergyRite Working Report Tests...\\n');\n    \n    try {\n      // Test working endpoints\n      const todaysSessions = await this.testTodaysSessionsAPI();\n      const activityReport = await this.testActivityReportAPI();\n      const dailyReport = await this.testDailyReportAPI();\n      const monthlyReport = await this.testMonthlyReportAPI();\n      \n      console.log('\\n=== WORKING TESTS SUMMARY ===');\n      console.log('✅ All working report tests completed successfully!');\n      console.log(`📊 Today\\'s Sessions: ${todaysSessions.total_sessions || 0} sessions`);\n      console.log(`📊 Activity Report: ${activityReport.data?.summary?.total_sessions || 0} sessions`);\n      console.log(`📊 Daily Report: ${dailyReport.data?.sites?.length || 0} sites`);\n      console.log(`📊 Monthly Report: ${monthlyReport.data?.summary?.total_sessions || 0} sessions`);\n      \n      return {\n        success: true,\n        results: {\n          todaysSessions,\n          activityReport,\n          dailyReport,\n          monthlyReport\n        }\n      };\n      \n    } catch (error) {\n      console.error('\\n❌ Working Test Suite Failed:', error.message);\n      return {\n        success: false,\n        error: error.message\n      };\n    }\n  }\n}\n\n// Run tests if called directly\nif (require.main === module) {\n  const tester = new WorkingReportTest();\n  tester.runWorkingTests()\n    .then(result => {\n      if (result.success) {\n        console.log('\\n🎉 All working tests passed!');\n        process.exit(0);\n      } else {\n        console.log('\\n💥 Working tests failed!');\n        process.exit(1);\n      }\n    })\n    .catch(error => {\n      console.error('💥 Test execution failed:', error);\n      process.exit(1);\n    });\n}\n\nmodule.exports = WorkingReportTest;