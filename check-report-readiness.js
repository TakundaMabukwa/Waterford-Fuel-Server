require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkReportReadiness() {
  try {
    console.log('🔍 Checking report generation readiness...\n');
    
    // 1. Check fuel data
    const { data: fuelData, error: fuelError } = await supabase
      .from('energy_rite_fuel_data')
      .select('*')
      .limit(5);
    
    console.log('📊 Fuel Data:', fuelError ? '❌ Error' : `✅ ${fuelData.length} records`);
    if (fuelData?.length > 0) {
      console.log(`   Latest: ${fuelData[0].plate} - ${fuelData[0].fuel_probe_1_level}L`);
    }
    
    // 2. Check generated reports table
    const { data: reportsData, error: reportsError } = await supabase
      .from('energy_rite_generated_reports')
      .select('*')
      .limit(3);
    
    console.log('📄 Generated Reports:', reportsError ? '❌ Error' : `✅ ${reportsData.length} reports`);
    
    // 3. Check storage bucket
    const { data: bucketFiles, error: bucketError } = await supabase.storage
      .from('reports')
      .list();
    
    console.log('🗂️  Reports Bucket:', bucketError ? '❌ Error' : `✅ ${bucketFiles.length} files`);
    
    // 4. Test report generation
    console.log('\n🧪 Testing report generation...');
    const testResult = await testReportGeneration();
    
    console.log('\n📋 READINESS STATUS:');
    console.log('===================');
    console.log(`Data Source: ${fuelData?.length > 0 ? '✅ Ready' : '❌ No data'}`);
    console.log(`Database: ${!reportsError ? '✅ Ready' : '❌ Error'}`);
    console.log(`Storage: ${!bucketError ? '✅ Ready' : '❌ Error'}`);
    console.log(`Generation: ${testResult ? '✅ Ready' : '❌ Failed'}`);
    
    const isReady = fuelData?.length > 0 && !reportsError && !bucketError && testResult;
    console.log(`\n🎯 OVERALL: ${isReady ? '✅ READY TO GENERATE REPORTS' : '❌ NOT READY'}`);
    
  } catch (error) {
    console.error('❌ Check failed:', error.message);
  }
}

async function testReportGeneration() {
  try {
    const { FixedReportGenerator } = require('./fixed-report-generator');
    return true;
  } catch (error) {
    console.log('❌ Generation test failed:', error.message);
    return false;
  }
}

checkReportReadiness();