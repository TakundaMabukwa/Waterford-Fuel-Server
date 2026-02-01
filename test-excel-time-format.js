#!/usr/bin/env node
require('dotenv').config();
const generator = require('./controllers/energy-rite/energyRiteExcelReportGenerator');

async function testExcelGeneration() {
  try {
    console.log('🔄 Testing Excel report generation with time format...');
    const result = await generator.generateDailyReport();
    console.log('✅ Report generated successfully:', result.file_name);
    console.log('📁 Download URL:', result.download_url);
  } catch (error) {
    console.error('❌ Error generating report:', error.message);
  }
}

testExcelGeneration();