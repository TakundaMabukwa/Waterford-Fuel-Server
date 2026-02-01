require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugSingleSiteFiltering() {
  try {
    console.log('🔍 Debugging Single Site Filtering for BRAAMFONTE\n');
    
    const targetDate = '2025-12-12';
    const siteId = 'BRAAMFONTE';
    
    // 1. Check if BRAAMFONTE exists in vehicle lookup
    console.log('1️⃣ Checking vehicle lookup for BRAAMFONTE:');
    const { data: siteData, error: siteError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate, cost_code')
      .eq('plate', siteId)
      .single();
    
    if (siteError) {
      console.log(`❌ Site lookup error: ${siteError.message}`);
      console.log('🔍 Let\'s check what plates exist:');
      
      const { data: allPlates, error: allError } = await supabase
        .from('energyrite_vehicle_lookup')
        .select('plate')
        .ilike('plate', '%BRAAM%');
      
      if (allError) {
        console.error('❌ All plates error:', allError);
      } else {
        console.log('   Similar plates found:', allPlates.map(p => p.plate));
      }
    } else {
      console.log(`✅ Found site: ${siteData.plate} (cost_code: ${siteData.cost_code})`);
    }
    
    // 2. Check sessions directly for BRAAMFONTE
    console.log('\n2️⃣ Checking sessions directly for BRAAMFONTE:');
    const { data: directSessions, error: directError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('branch', 'BRAAMFONTE')
      .eq('session_date', targetDate);
    
    if (directError) {
      console.error('❌ Direct sessions error:', directError);
    } else {
      console.log(`   Found ${directSessions.length} sessions for BRAAMFONTE on ${targetDate}`);
      directSessions.forEach(session => {
        console.log(`   - ID: ${session.id}, Status: ${session.session_status}, Hours: ${session.operating_hours}`);
      });
    }
    
    // 3. Check what the Excel generator query would return
    console.log('\n3️⃣ Testing Excel generator queries:');
    
    // Sessions query
    const { data: sessions, error: sessionsError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .eq('session_date', targetDate)
      .in('branch', ['BRAAMFONTE']);
    
    if (sessionsError) {
      console.error('❌ Sessions query error:', sessionsError);
    } else {
      console.log(`   Sessions query: Found ${sessions.length} COMPLETED sessions`);
    }
    
    // Fills query
    const { data: fills, error: fillsError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'FUEL_FILL_COMPLETED')
      .eq('session_date', targetDate)
      .in('branch', ['BRAAMFONTE']);
    
    if (fillsError) {
      console.error('❌ Fills query error:', fillsError);
    } else {
      console.log(`   Fills query: Found ${fills.length} FUEL_FILL_COMPLETED sessions`);
    }
    
    // 4. Check all statuses for BRAAMFONTE
    console.log('\n4️⃣ All BRAAMFONTE sessions by status:');
    const { data: allStatuses, error: statusError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('session_status, count(*)')
      .eq('branch', 'BRAAMFONTE')
      .eq('session_date', targetDate);
    
    if (statusError) {
      console.error('❌ Status query error:', statusError);
    } else {
      console.log('   Status breakdown:', allStatuses);
    }
    
    console.log('\n💡 SOLUTION:');
    console.log('The issue is likely that BRAAMFONTE sessions have different statuses');
    console.log('We need to check what statuses exist and include them in the report');
    
  } catch (error) {
    console.error('❌ Debug error:', error);
  }
}

debugSingleSiteFiltering();