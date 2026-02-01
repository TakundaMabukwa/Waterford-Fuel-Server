require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugDateFiltering() {
  try {
    console.log('🔍 Debugging Date Filtering Issue\n');
    
    const targetDate = '2025-12-12';
    
    // 1. Check what dates exist in the database
    console.log('1️⃣ Checking session_date values in database:');
    const { data: dates, error: datesError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('session_date, count(*)')
      .eq('branch', 'BRAAMFONTE')
      .gte('session_date', '2025-12-10')
      .lte('session_date', '2025-12-14');
    
    if (datesError) {
      console.error('❌ Dates error:', datesError);
    } else {
      console.log('   Session dates found:', dates);
    }
    
    // 2. Check the exact date format being used
    console.log('\n2️⃣ Testing different date formats:');
    
    const startDate = new Date(targetDate);
    startDate.setHours(0, 0, 0, 0);
    const dateString = startDate.toISOString().split('T')[0];
    
    console.log(`   Input date: ${targetDate}`);
    console.log(`   Processed date: ${dateString}`);
    console.log(`   Start date object: ${startDate.toISOString()}`);
    
    // 3. Test direct query with exact date
    console.log('\n3️⃣ Testing direct queries:');
    
    // Test with string date
    const { data: stringResult, error: stringError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, branch, session_date, session_status')
      .eq('branch', 'BRAAMFONTE')
      .eq('session_date', '2025-12-12');
    
    if (stringError) {
      console.error('❌ String date error:', stringError);
    } else {
      console.log(`   String date query: Found ${stringResult.length} sessions`);
    }
    
    // Test with processed date
    const { data: processedResult, error: processedError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, branch, session_date, session_status')
      .eq('branch', 'BRAAMFONTE')
      .eq('session_date', dateString);
    
    if (processedError) {
      console.error('❌ Processed date error:', processedError);
    } else {
      console.log(`   Processed date query: Found ${processedResult.length} sessions`);
    }
    
    // 4. Check what the Excel generator is actually doing
    console.log('\n4️⃣ Simulating Excel generator date logic:');
    
    const reportDate = new Date(targetDate);
    const endDate = new Date(targetDate);
    endDate.setHours(23, 59, 59, 999);
    
    const excelStartDate = new Date(reportDate);
    excelStartDate.setHours(0, 0, 0, 0);
    
    console.log(`   Excel start date: ${excelStartDate.toISOString()}`);
    console.log(`   Excel end date: ${endDate.toISOString()}`);
    console.log(`   Excel date string: ${excelStartDate.toISOString().split('T')[0]}`);
    
    // Test with Excel's exact logic
    const { data: excelResult, error: excelError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('id, branch, session_date, session_status')
      .eq('branch', 'BRAAMFONTE')
      .eq('session_date', excelStartDate.toISOString().split('T')[0]);
    
    if (excelError) {
      console.error('❌ Excel logic error:', excelError);
    } else {
      console.log(`   Excel logic query: Found ${excelResult.length} sessions`);
      excelResult.forEach(r => {
        console.log(`     - ID: ${r.id}, Status: ${r.session_status}, Date: ${r.session_date}`);
      });
    }
    
  } catch (error) {
    console.error('❌ Debug error:', error);
  }
}

debugDateFiltering();