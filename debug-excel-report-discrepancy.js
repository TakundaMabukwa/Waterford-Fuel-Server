require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugExcelReportDiscrepancy() {
  try {
    console.log('🔍 Debugging Excel Report vs Database Discrepancy\n');
    
    const targetDate = '2025-12-12';
    console.log(`📅 Analyzing data for: ${targetDate}\n`);
    
    // 1. Check what the Excel generator query would return
    console.log('1️⃣ Excel Generator Query (COMPLETED sessions only):');
    const startDate = new Date(targetDate);
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(targetDate);
    endDate.setHours(23, 59, 59, 999);
    
    const { data: completedSessions, error: completedError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .gte('session_start_time', startDate.toISOString())
      .lte('session_start_time', endDate.toISOString())
      .order('session_start_time', { ascending: false });
    
    if (completedError) {
      console.error('❌ Completed sessions error:', completedError);
    } else {
      console.log(`   Found ${completedSessions.length} COMPLETED sessions`);
      completedSessions.forEach(session => {
        console.log(`   - ${session.branch}: ${session.session_status}, Usage: ${session.total_usage}L, Fill: ${session.total_fill}L`);
      });
    }
    
    // 2. Check ALL sessions for the date (regardless of status)
    console.log('\n2️⃣ ALL Sessions for the date (any status):');
    const { data: allSessions, error: allError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_date', targetDate)
      .order('session_start_time', { ascending: false });
    
    if (allError) {
      console.error('❌ All sessions error:', allError);
    } else {
      console.log(`   Found ${allSessions.length} total sessions on ${targetDate}`);
      
      // Group by branch and status
      const byBranch = {};
      allSessions.forEach(session => {
        if (!byBranch[session.branch]) {
          byBranch[session.branch] = {};
        }
        if (!byBranch[session.branch][session.session_status]) {
          byBranch[session.branch][session.session_status] = [];
        }
        byBranch[session.branch][session.session_status].push(session);
      });
      
      Object.keys(byBranch).sort().forEach(branch => {
        console.log(`\n   📍 ${branch}:`);
        Object.keys(byBranch[branch]).forEach(status => {
          const sessions = byBranch[branch][status];
          console.log(`     ${status}: ${sessions.length} sessions`);
          sessions.forEach(session => {
            console.log(`       - ID: ${session.id}, Hours: ${session.operating_hours}, Usage: ${session.total_usage}L, Fill: ${session.total_fill}L`);
          });
        });
      });
    }
    
    // 3. Check BRAAMFONTE specifically (from your Excel example)
    console.log('\n3️⃣ BRAAMFONTE Sessions Analysis:');
    const braamfonteSessions = allSessions.filter(s => s.branch === 'BRAAMFONTE');
    console.log(`   Found ${braamfonteSessions.length} BRAAMFONTE sessions`);
    
    braamfonteSessions.forEach((session, index) => {
      console.log(`   Session ${index + 1}:`);
      console.log(`     - ID: ${session.id}`);
      console.log(`     - Status: ${session.session_status}`);
      console.log(`     - Start: ${session.session_start_time}`);
      console.log(`     - End: ${session.session_end_time}`);
      console.log(`     - Hours: ${session.operating_hours}`);
      console.log(`     - Opening: ${session.opening_percentage}% (${session.opening_fuel}L)`);
      console.log(`     - Closing: ${session.closing_percentage}% (${session.closing_fuel}L)`);
      console.log(`     - Usage: ${session.total_usage}L`);
      console.log(`     - Fill: ${session.total_fill}L`);
      console.log(`     - Cost: R${session.cost_for_usage}`);
      console.log('');
    });
    
    // 4. Check what sites should be included for ALL cost codes
    console.log('4️⃣ Vehicle Lookup Analysis:');
    const { data: vehicles, error: vehicleError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate, cost_code')
      .order('plate');
    
    if (vehicleError) {
      console.error('❌ Vehicle lookup error:', vehicleError);
    } else {
      console.log(`   Found ${vehicles.length} total vehicles in lookup`);
      
      // Count by cost code
      const byCostCode = {};
      vehicles.forEach(vehicle => {
        const costCode = vehicle.cost_code || 'NULL';
        if (!byCostCode[costCode]) {
          byCostCode[costCode] = [];
        }
        byCostCode[costCode].push(vehicle.plate);
      });
      
      Object.keys(byCostCode).sort().forEach(costCode => {
        console.log(`   ${costCode}: ${byCostCode[costCode].length} sites`);
      });
    }
    
    // 5. Identify the root cause
    console.log('\n🎯 ROOT CAUSE ANALYSIS:');
    console.log('');
    
    console.log('📊 Excel Report Issues Found:');
    console.log('1. STATUS FILTERING: Excel only shows COMPLETED sessions');
    console.log('   - Your DB has ONGOING and FUEL_FILL_COMPLETED sessions');
    console.log('   - These are excluded from Excel reports');
    console.log('');
    
    console.log('2. NULL VALUES: Many sessions have null total_usage/total_fill');
    console.log('   - Excel shows 0.00 for null values');
    console.log('   - Need to handle null vs actual zero values');
    console.log('');
    
    console.log('3. DATE FILTERING: Excel uses session_start_time range');
    console.log('   - Should use session_date for daily reports');
    console.log('   - Time-based filtering can miss sessions');
    console.log('');
    
    console.log('4. SITE INCLUSION: Excel includes ALL sites from vehicle lookup');
    console.log('   - Shows sites with 0 sessions (correct behavior)');
    console.log('   - But may miss sites not in lookup table');
    
    // 6. Recommendations
    console.log('\n💡 RECOMMENDED FIXES:');
    console.log('');
    console.log('1. Update session status filtering:');
    console.log('   - Include ONGOING sessions in reports');
    console.log('   - Include FUEL_FILL_COMPLETED sessions');
    console.log('   - Or create separate report sections for different statuses');
    console.log('');
    
    console.log('2. Fix date filtering:');
    console.log('   - Use session_date instead of session_start_time for daily reports');
    console.log('   - Keep time-based filtering for weekly/monthly reports');
    console.log('');
    
    console.log('3. Handle null values properly:');
    console.log('   - Distinguish between null and 0.0 values');
    console.log('   - Show "N/A" for incomplete sessions');
    console.log('');
    
    console.log('4. Add session status column to Excel:');
    console.log('   - Show session status in reports');
    console.log('   - Help users understand why values are zero');
    
  } catch (error) {
    console.error('❌ Debug error:', error);
  }
}

debugExcelReportDiscrepancy();