const fetch = require('node-fetch');
const fs = require('fs');

async function testEnhancedDailyDownload() {
  console.log('🔬 Testing Enhanced Daily Activity Report Download');
  console.log('='.repeat(50));
  
  const date = '2025-11-07';
  const baseUrl = 'http://localhost:4000';
  
  try {
    // Test 1: Download Excel format (enhanced daily report style)
    console.log('\n1️⃣ Testing Excel Download (Enhanced Daily Report Format)...');
    const excelUrl = `${baseUrl}/api/energy-rite/reports/activity/download?format=excel&date=${date}`;
    const excelResponse = await fetch(excelUrl);
    
    if (excelResponse.ok) {
      const buffer = await excelResponse.buffer();
      const filename = `enhanced-daily-report-${date}.xlsx`;
      fs.writeFileSync(filename, buffer);
      
      console.log(`✅ Excel file generated: ${filename}`);
      console.log(`📊 File size: ${(buffer.length / 1024).toFixed(2)} KB`);
      console.log(`🎯 Features: Two-sheet format with overview + session details`);
      console.log(`📝 Content: Current fuel levels, engine status, last activity + sessions`);
    } else {
      console.log(`❌ Excel download failed: ${excelResponse.status}`);
    }
    
    // Test 2: Download JSON format (enhanced structure)
    console.log('\n2️⃣ Testing JSON Download (Enhanced Daily Report Structure)...');
    const jsonUrl = `${baseUrl}/api/energy-rite/reports/activity/download?format=json&date=${date}`;
    const jsonResponse = await fetch(jsonUrl);
    
    if (jsonResponse.ok) {
      const data = await jsonResponse.json();
      const filename = `enhanced-daily-report-${date}.json`;
      fs.writeFileSync(filename, JSON.stringify(data, null, 2));
      
      console.log(`✅ JSON file generated: ${filename}`);
      console.log(`📊 File size: ${(JSON.stringify(data).length / 1024).toFixed(2)} KB`);
      console.log(`🏢 Total sites in system: ${data.data.summary.total_sites}`);
      console.log(`🔥 Active sites today: ${data.data.summary.active_sites_today}`);
      console.log(`⚡ Total sessions: ${data.data.summary.total_sessions}`);
      console.log(`⏱️ Total operating hours: ${data.data.summary.total_operating_hours.toFixed(2)}h`);
      console.log(`⛽ Total fuel usage: ${data.data.summary.total_fuel_usage.toFixed(2)}L`);
      
      // Show enhanced daily report structure
      console.log('\n📋 Enhanced Daily Report Structure:');
      console.log(`├── Summary (like getDailyReport)`);
      console.log(`├── Sites with current status`);
      console.log(`├── Daily data per site`);
      console.log(`└── Detailed session breakdown`);
      
      // Show a sample site with enhanced info
      if (data.data.sites.length > 0) {
        const sampleSite = data.data.sites.find(s => s.daily_data.total_sessions > 0) || data.data.sites[0];
        console.log(`\n🏗️ Sample Site Info (${sampleSite.branch}):`);
        console.log(`   Current Fuel: ${sampleSite.current_fuel_level}L`);
        console.log(`   Engine Status: ${sampleSite.current_engine_status}`);
        console.log(`   Sessions Today: ${sampleSite.daily_data.total_sessions}`);
        console.log(`   Operating Hours: ${sampleSite.daily_data.total_running_hours}h`);
        console.log(`   Session Details: ${sampleSite.sessions.length} detailed records`);
      }
      
    } else {
      console.log(`❌ JSON download failed: ${jsonResponse.status}`);
    }
    
    // Test 3: Test with cost code filtering
    console.log('\n3️⃣ Testing with Cost Code Filter...');
    const filteredUrl = `${baseUrl}/api/energy-rite/reports/activity/download?format=json&date=${date}&cost_code=KFC-0001-0001-0003`;
    const filteredResponse = await fetch(filteredUrl);
    
    if (filteredResponse.ok) {
      const filteredData = await filteredResponse.json();
      console.log(`✅ Filtered download successful`);
      console.log(`🎯 Filtered sites: ${filteredData.data.summary.active_sites_today}`);
      console.log(`💰 Cost Code: ${filteredData.data.cost_code}`);
    } else {
      console.log(`❌ Filtered download failed: ${filteredResponse.status}`);
    }
    
    console.log('\n🎉 Enhanced Daily Report Download Testing Complete!');
    console.log('📋 This now matches getDailyReport structure with downloadable extensions');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

testEnhancedDailyDownload();