require('dotenv').config();
const { supabase } = require('./supabase-client');

async function debugExcelSiteFiltering() {
  try {
    console.log('🔍 Debugging Excel Site Filtering Logic\n');
    
    const targetDate = '2025-12-12';
    const siteId = 'BRAAMFONTE';
    
    // Simulate the Excel generator logic
    console.log('1️⃣ Simulating Excel generator site filtering logic:');
    
    let allSites = [];
    
    // Single site filtering (from Excel generator)
    console.log(`📊 Excel Report - Single site filter: ${siteId}`);
    const { data: siteData, error: siteError } = await supabase
      .from('energyrite_vehicle_lookup')
      .select('plate, cost_code')
      .eq('plate', siteId)
      .single();
    
    if (siteError || !siteData) {
      console.log(`⚠️ Site ${siteId} not found, falling back to cost_code filtering`);
    } else {
      allSites = [siteData.plate];
      console.log(`📊 Excel Report - Single site: ${siteData.plate}`);
      console.log(`📊 allSites array: ${JSON.stringify(allSites)}`);
    }
    
    // Test the queries with the allSites filter
    console.log('\n2️⃣ Testing queries with site filtering:');
    
    const startDate = new Date(targetDate);
    startDate.setHours(0, 0, 0, 0);
    
    // Sessions query
    let sessionsQuery = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .eq('session_date', startDate.toISOString().split('T')[0]);
    
    if (allSites.length > 0) {
      sessionsQuery = sessionsQuery.in('branch', allSites);
    }
    
    const { data: sessions, error: sessionsError } = await sessionsQuery.order('session_start_time', { ascending: false });
    
    if (sessionsError) {
      console.error('❌ Sessions error:', sessionsError);
    } else {
      console.log(`   Sessions: Found ${sessions.length} COMPLETED sessions`);
      sessions.forEach(s => console.log(`     - ${s.branch}: ${s.id}`));
    }
    
    // Fills query
    let fillsQuery = supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'FUEL_FILL_COMPLETED')
      .eq('session_date', startDate.toISOString().split('T')[0]);
    
    if (allSites.length > 0) {
      fillsQuery = fillsQuery.in('branch', allSites);
    }
    
    const { data: fills, error: fillsError } = await fillsQuery.order('session_start_time', { ascending: false });
    
    if (fillsError) {
      console.error('❌ Fills error:', fillsError);
    } else {
      console.log(`   Fills: Found ${fills.length} FUEL_FILL_COMPLETED sessions`);
      fills.forEach(f => console.log(`     - ${f.branch}: ${f.id}`));
    }
    
    // 3. Test the site grouping logic
    console.log('\n3️⃣ Testing site grouping logic:');
    
    const siteGroups = {};
    
    // Initialize site
    allSites.forEach(siteName => {
      siteGroups[siteName] = {
        branch: siteName,
        company: 'KFC',
        cost_code: null,
        sessions: [],
        fills: [],
        total_sessions: 0,
        total_operating_hours: 0,
        total_fuel_usage: 0,
        total_fuel_filled: 0,
        total_cost: 0,
        avg_efficiency: 0
      };
    });
    
    console.log(`   Initialized ${Object.keys(siteGroups).length} sites`);
    
    // Add sessions
    sessions.forEach(session => {
      if (siteGroups[session.branch]) {
        siteGroups[session.branch].sessions.push(session);
        siteGroups[session.branch].total_sessions += 1;
        siteGroups[session.branch].total_operating_hours += parseFloat(session.operating_hours || 0);
        siteGroups[session.branch].total_fuel_usage += parseFloat(session.total_usage || 0);
        siteGroups[session.branch].total_cost += parseFloat(session.cost_for_usage || 0);
      }
    });
    
    // Add fills
    fills.forEach(fill => {
      if (siteGroups[fill.branch]) {
        siteGroups[fill.branch].fills.push(fill);
        siteGroups[fill.branch].total_fuel_filled += parseFloat(fill.total_fill || 0);
      }
    });
    
    // Show results
    Object.keys(siteGroups).forEach(siteName => {
      const site = siteGroups[siteName];
      console.log(`   ${siteName}:`);
      console.log(`     - Sessions: ${site.sessions.length}`);
      console.log(`     - Fills: ${site.fills.length}`);
      console.log(`     - Total Hours: ${site.total_operating_hours}`);
      console.log(`     - Total Usage: ${site.total_fuel_usage}L`);
      console.log(`     - Total Filled: ${site.total_fuel_filled}L`);
    });
    
    console.log('\n✅ The logic appears to be working correctly!');
    console.log('The issue might be in the return values or stats calculation.');
    
  } catch (error) {
    console.error('❌ Debug error:', error);
  }
}

debugExcelSiteFiltering();