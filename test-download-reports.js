const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Test the download activity report endpoint
async function testDownloadReport() {
  console.log('🔽 TESTING DOWNLOAD ACTIVITY REPORT ENDPOINT\n');
  
  const baseUrl = 'http://localhost:4000';
  const today = new Date().toISOString().split('T')[0];
  
  try {
    console.log('📊 Testing Excel Download...');
    
    // Test Excel download
    const excelResponse = await axios({
      method: 'get',
      url: `${baseUrl}/api/energy-rite/reports/activity/download`,
      params: {
        date: today,
        format: 'excel'
      },
      responseType: 'stream'
    });
    
    // Save Excel file
    const excelFilename = `activity-report-${today}.xlsx`;
    const excelPath = path.join(__dirname, excelFilename);
    const excelWriter = fs.createWriteStream(excelPath);
    
    excelResponse.data.pipe(excelWriter);
    
    await new Promise((resolve, reject) => {
      excelWriter.on('finish', resolve);
      excelWriter.on('error', reject);
    });
    
    const excelStats = fs.statSync(excelPath);
    console.log(`✅ Excel file generated: ${excelFilename}`);
    console.log(`   File size: ${(excelStats.size / 1024).toFixed(2)} KB`);
    console.log(`   Path: ${excelPath}`);
    
    console.log('\n📄 Testing JSON Download...');
    
    // Test JSON download
    const jsonResponse = await axios({
      method: 'get',
      url: `${baseUrl}/api/energy-rite/reports/activity/download`,
      params: {
        date: today,
        format: 'json'
      }
    });
    
    // Save JSON file
    const jsonFilename = `activity-report-${today}.json`;
    const jsonPath = path.join(__dirname, jsonFilename);
    fs.writeFileSync(jsonPath, JSON.stringify(jsonResponse.data, null, 2));
    
    const jsonStats = fs.statSync(jsonPath);
    console.log(`✅ JSON file generated: ${jsonFilename}`);
    console.log(`   File size: ${(jsonStats.size / 1024).toFixed(2)} KB`);
    console.log(`   Sites included: ${jsonResponse.data.data.sites.length}`);
    console.log(`   Total sessions: ${jsonResponse.data.data.summary.total_sessions}`);
    
    console.log('\n🎯 Testing Filtered Downloads...');
    
    // Test with site filter
    if (jsonResponse.data.data.sites.length > 0) {
      const firstSite = jsonResponse.data.data.sites[0].branch;
      
      const siteResponse = await axios({
        method: 'get',
        url: `${baseUrl}/api/energy-rite/reports/activity/download`,
        params: {
          date: today,
          site_id: firstSite,
          format: 'excel'
        },
        responseType: 'stream'
      });
      
      const siteFilename = `activity-report-${today}-${firstSite}.xlsx`;
      const sitePath = path.join(__dirname, siteFilename);
      const siteWriter = fs.createWriteStream(sitePath);
      
      siteResponse.data.pipe(siteWriter);
      
      await new Promise((resolve, reject) => {
        siteWriter.on('finish', resolve);
        siteWriter.on('error', reject);
      });
      
      console.log(`✅ Site-filtered Excel generated: ${siteFilename}`);
      console.log(`   Filtered for site: ${firstSite}`);
    }
    
    console.log('\n📋 DOWNLOAD ENDPOINT SUMMARY:');
    console.log('=====================================');
    console.log('✅ Excel download working');
    console.log('✅ JSON download working');
    console.log('✅ Site filtering working');
    console.log('✅ Files saved to current directory');
    
    console.log('\n🔗 FRONTEND INTEGRATION:');
    console.log('=====================================');
    console.log('// Excel download');
    console.log(`const downloadUrl = '/api/energy-rite/reports/activity/download?date=${today}&format=excel';`);
    console.log('window.open(downloadUrl, "_blank");');
    console.log('');
    console.log('// JSON download');
    console.log(`const jsonUrl = '/api/energy-rite/reports/activity/download?date=${today}&format=json';`);
    console.log('fetch(jsonUrl).then(r => r.json()).then(data => console.log(data));');
    
  } catch (error) {
    console.error('❌ Download test failed:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
  }
}

// Test different download scenarios
async function testDownloadScenarios() {
  console.log('\n🧪 TESTING DOWNLOAD SCENARIOS\n');
  
  const baseUrl = 'http://localhost:4000';
  const today = new Date().toISOString().split('T')[0];
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  
  const scenarios = [
    { 
      name: 'Today - All Sites', 
      params: { date: today, format: 'excel' } 
    },
    { 
      name: 'Yesterday - All Sites', 
      params: { date: yesterday, format: 'excel' } 
    },
    { 
      name: 'Today - JSON Format', 
      params: { date: today, format: 'json' } 
    },
    { 
      name: 'Cost Code Filter', 
      params: { date: today, cost_code: 'KFC-0001-0001-0003', format: 'excel' } 
    }
  ];
  
  for (const scenario of scenarios) {
    try {
      console.log(`📊 Testing: ${scenario.name}`);
      
      const response = await axios({
        method: 'get',
        url: `${baseUrl}/api/energy-rite/reports/activity/download`,
        params: scenario.params,
        responseType: scenario.params.format === 'json' ? 'json' : 'stream',
        timeout: 10000
      });
      
      if (scenario.params.format === 'json') {
        console.log(`   ✅ Success - ${response.data.data.sites.length} sites, ${response.data.data.summary.total_sessions} sessions`);
      } else {
        console.log(`   ✅ Success - Excel file generated`);
      }
      
    } catch (error) {
      console.log(`   ❌ Failed - ${error.message}`);
    }
  }
}

// Run tests
testDownloadReport()
  .then(() => testDownloadScenarios())
  .catch(console.error);