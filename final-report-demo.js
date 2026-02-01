require('dotenv').config();
const axios = require('axios');
const { supabase } = require('./supabase-client');

const BASE_URL = 'http://localhost:4000';

async function generateComprehensiveReport() {
  console.log('🚀 EnergyRite Comprehensive Report Generator');
  console.log('===========================================\n');

  try {
    // 1. Get today's sessions
    console.log('📅 1. Today\'s Sessions Report');
    console.log('----------------------------');
    const todayResponse = await axios.get(`${BASE_URL}/api/energy-rite/reports/today`);
    console.log(`✅ Date: ${todayResponse.data.date}`);
    console.log(`📊 Total Sessions: ${todayResponse.data.total_sessions}`);
    
    // 2. Generate daily report
    console.log('\n📈 2. Daily Report Generation');
    console.log('-----------------------------');
    const dailyResponse = await axios.get(`${BASE_URL}/api/energy-rite/reports/daily`);
    console.log(`✅ Period: ${dailyResponse.data.data.period}`);
    console.log(`🏢 Total Sites: ${dailyResponse.data.data.sites.length}`);
    
    const totalFuel = dailyResponse.data.data.sites.reduce((sum, site) => 
      sum + (site.monthly_data?.total_fuel_usage || 0), 0);
    const totalHours = dailyResponse.data.data.sites.reduce((sum, site) => 
      sum + (site.monthly_data?.total_running_hours || 0), 0);
    
    console.log(`⛽ Total Monthly Fuel Usage: ${totalFuel.toFixed(2)}L`);
    console.log(`⏱️  Total Monthly Operating Hours: ${totalHours.toFixed(2)}h`);
    
    // 3. Store the daily report
    console.log('\n💾 3. Storing Report Metadata');
    console.log('-----------------------------');
    const reportData = {
      cost_code: 'ALL',
      report_type: 'daily',
      report_url: `https://energyrite-reports.s3.amazonaws.com/daily-report-${new Date().toISOString().split('T')[0]}.pdf`,
      report_date: new Date().toISOString().split('T')[0],
      file_size: Math.floor(Math.random() * 3000000) + 1000000,
      status: 'generated'
    };
    
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .insert([reportData])
      .select();
    
    if (error) {
      console.log(`⚠️  Report already exists or error: ${error.message}`);
    } else {
      console.log(`✅ Report stored with ID: ${data[0].id}`);
      console.log(`📄 Size: ${(data[0].file_size / 1024 / 1024).toFixed(2)} MB`);
    }
    
    // 4. Get activity report for today
    console.log('\n📋 4. Activity Report for Today');
    console.log('-------------------------------');
    const activityResponse = await axios.get(`${BASE_URL}/api/energy-rite/reports/activity`);
    console.log(`✅ Date: ${activityResponse.data.data.date}`);
    console.log(`🏢 Sites with Activity: ${activityResponse.data.data.sites.length}`);
    console.log(`📊 Summary:`);
    console.log(`   Total Sites: ${activityResponse.data.data.summary.total_sites}`);
    console.log(`   Total Sessions: ${activityResponse.data.data.summary.total_sessions}`);
    console.log(`   Completed Sessions: ${activityResponse.data.data.summary.completed_sessions}`);
    console.log(`   Total Operating Hours: ${activityResponse.data.data.summary.total_operating_hours.toFixed(2)}h`);
    console.log(`   Total Fuel Usage: ${activityResponse.data.data.summary.total_fuel_usage.toFixed(2)}L`);
    console.log(`   Total Cost: R${activityResponse.data.data.summary.total_cost.toFixed(2)}`);
    
    // 5. Show top 5 most active sites
    if (activityResponse.data.data.sites.length > 0) {
      console.log('\n🏆 5. Top 5 Most Active Sites');
      console.log('-----------------------------');
      const sortedSites = activityResponse.data.data.sites
        .sort((a, b) => b.total_fuel_usage - a.total_fuel_usage)
        .slice(0, 5);
      
      sortedSites.forEach((site, index) => {
        console.log(`${index + 1}. ${site.branch}`);
        console.log(`   Cost Code: ${site.cost_code}`);
        console.log(`   Sessions: ${site.sessions.length}`);
        console.log(`   Fuel Usage: ${site.total_fuel_usage.toFixed(2)}L`);
        console.log(`   Operating Hours: ${site.total_operating_hours.toFixed(2)}h`);
        console.log('');
      });
    }
    
    // 6. Get all stored reports
    console.log('📚 6. Previously Generated Reports');
    console.log('----------------------------------');
    const { data: storedReports, error: reportsError } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .order('generated_at', { ascending: false })
      .limit(5);
    
    if (reportsError) {
      console.log(`⚠️  Error fetching reports: ${reportsError.message}`);
    } else {
      console.log(`📄 Total Reports in Database: ${storedReports.length}`);
      storedReports.forEach((report, index) => {
        console.log(`${index + 1}. ${report.report_type.toUpperCase()} - ${report.report_date}`);
        console.log(`   Cost Code: ${report.cost_code}`);
        console.log(`   Size: ${(report.file_size / 1024 / 1024).toFixed(2)} MB`);
        console.log(`   Status: ${report.status}`);
        console.log('');
      });
    }
    
    console.log('🎉 Comprehensive Report Generation Completed Successfully!');
    console.log('=========================================================');
    
  } catch (error) {
    console.error('❌ Error in comprehensive report generation:', error.message);
    if (error.response) {
      console.error('Response:', error.response.data);
    }
  }
}

generateComprehensiveReport();