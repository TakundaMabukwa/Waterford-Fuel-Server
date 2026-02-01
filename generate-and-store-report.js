require('dotenv').config();
const axios = require('axios');
const { supabase } = require('./supabase-client');

const BASE_URL = 'http://localhost:4000';

async function generateAndStoreReport(reportType = 'daily', costCode = null) {
  try {
    console.log(`🔄 Generating ${reportType} report...`);
    
    // Generate the report
    let endpoint = `/api/energy-rite/reports/${reportType}`;
    if (costCode) {
      endpoint += `?cost_code=${costCode}`;
    }
    
    const response = await axios.get(`${BASE_URL}${endpoint}`);
    
    console.log(`✅ ${reportType.charAt(0).toUpperCase() + reportType.slice(1)} report generated successfully!`);
    
    // Create a mock report URL (in real implementation, this would be uploaded to S3)
    const reportUrl = `https://energyrite-reports.s3.amazonaws.com/${reportType}-report-${new Date().toISOString().split('T')[0]}.pdf`;
    
    // Store the report metadata
    const reportData = {
      cost_code: costCode || 'ALL',
      report_type: reportType,
      report_url: reportUrl,
      report_date: new Date().toISOString().split('T')[0],
      file_size: Math.floor(Math.random() * 5000000) + 1000000, // Random size between 1-5MB
      status: 'generated'
    };
    
    const { data, error } = await supabase
      .from('energy_rite_generated_reports')
      .insert([reportData])
      .select();
    
    if (error) {
      throw new Error(`Database error: ${error.message}`);
    }
    
    console.log('💾 Report metadata stored successfully!');
    console.log('📄 Report Details:');
    console.log(`   ID: ${data[0].id}`);
    console.log(`   Cost Code: ${data[0].cost_code}`);
    console.log(`   Type: ${data[0].report_type}`);
    console.log(`   Date: ${data[0].report_date}`);
    console.log(`   URL: ${data[0].report_url}`);
    console.log(`   Size: ${(data[0].file_size / 1024 / 1024).toFixed(2)} MB`);
    console.log(`   Generated: ${data[0].generated_at}`);
    
    // Display report summary
    if (response.data.data) {
      const reportData = response.data.data;
      console.log('\n📊 Report Summary:');
      
      if (reportType === 'daily') {
        console.log(`   Period: ${reportData.period}`);
        console.log(`   Total Sites: ${reportData.sites?.length || 0}`);
        
        if (reportData.sites && reportData.sites.length > 0) {
          const totalFuel = reportData.sites.reduce((sum, site) => 
            sum + (site.monthly_data?.total_fuel_usage || 0), 0);
          const totalHours = reportData.sites.reduce((sum, site) => 
            sum + (site.monthly_data?.total_running_hours || 0), 0);
          
          console.log(`   Total Monthly Fuel Usage: ${totalFuel.toFixed(2)}L`);
          console.log(`   Total Monthly Operating Hours: ${totalHours.toFixed(2)}h`);
        }
      } else if (reportType === 'weekly' || reportType === 'monthly') {
        console.log(`   Cost Centers: ${reportData.cost_centers?.length || 0}`);
        if (reportData.summary) {
          console.log(`   Total Sessions: ${reportData.summary.total_sessions}`);
          console.log(`   Total Operating Hours: ${reportData.summary.total_operating_hours.toFixed(2)}h`);
          console.log(`   Total Fuel Usage: ${reportData.summary.total_fuel_usage.toFixed(2)}L`);
          console.log(`   Total Cost: R${reportData.summary.total_cost.toFixed(2)}`);
        }
      }
    }
    
    return data[0];
    
  } catch (error) {
    console.error(`❌ Error generating ${reportType} report:`, error.message);
    if (error.response) {
      console.error('Response:', error.response.data);
    }
    throw error;
  }
}

// Command line interface
const args = process.argv.slice(2);
const reportType = args[0] || 'daily';
const costCode = args[1] || null;

console.log('🚀 EnergyRite Report Generator');
console.log('===============================');

generateAndStoreReport(reportType, costCode)
  .then(() => {
    console.log('\n🎉 Report generation completed successfully!');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n💥 Report generation failed:', error.message);
    process.exit(1);
  });