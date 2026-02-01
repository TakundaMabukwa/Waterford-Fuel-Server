#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkDashboardAccuracy() {
  console.log('🔍 Checking Executive Dashboard Accuracy...\n');
  
  try {
    // Get last 30 days of data
    const endDate = new Date();
    const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const start = startDate.toISOString().split('T')[0];
    const end = endDate.toISOString().split('T')[0];
    
    console.log(`📅 Checking period: ${start} to ${end}`);
    
    // Get all operating sessions
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', start)
      .lte('session_date', end)
      .eq('session_status', 'COMPLETED');
    
    if (error) throw error;
    
    console.log(`📊 Total sessions found: ${sessions.length}`);
    
    // Check for duplicates or inflated data
    const sessionsByDate = {};
    const sessionsBySite = {};
    let totalOperatingHours = 0;
    let totalFuelUsage = 0;
    let totalCost = 0;
    
    sessions.forEach(session => {
      const date = session.session_date;
      const site = session.branch;
      const hours = parseFloat(session.operating_hours || 0);
      const fuel = parseFloat(session.total_usage || 0);
      const cost = parseFloat(session.cost_for_usage || 0);
      
      // Track by date
      if (!sessionsByDate[date]) sessionsByDate[date] = [];
      sessionsByDate[date].push(session);
      
      // Track by site
      if (!sessionsBySite[site]) sessionsBySite[site] = [];
      sessionsBySite[site].push(session);
      
      totalOperatingHours += hours;
      totalFuelUsage += fuel;
      totalCost += cost;
    });
    
    console.log('\n📈 CALCULATED TOTALS:');
    console.log(`Total Operating Hours: ${totalOperatingHours.toFixed(2)}`);
    console.log(`Total Fuel Usage: ${totalFuelUsage.toFixed(2)} L`);
    console.log(`Total Cost: R${totalCost.toFixed(2)}`);
    console.log(`Average Cost per Hour: R${totalOperatingHours > 0 ? (totalCost / totalOperatingHours).toFixed(2) : 0}`);
    console.log(`Average Fuel per Hour: ${totalOperatingHours > 0 ? (totalFuelUsage / totalOperatingHours).toFixed(2) : 0} L/h`);
    
    // Check for potential issues
    console.log('\n🔍 POTENTIAL ISSUES:');
    
    // 1. Check for dates with excessive sessions
    const datesWithManySessions = Object.entries(sessionsByDate)
      .filter(([date, sessions]) => sessions.length > 50)
      .sort((a, b) => b[1].length - a[1].length);
    
    if (datesWithManySessions.length > 0) {
      console.log('\n⚠️ Dates with high session counts (potential duplicates):');
      datesWithManySessions.slice(0, 5).forEach(([date, sessions]) => {
        console.log(`  ${date}: ${sessions.length} sessions`);
      });
    }
    
    // 2. Check for sites with excessive sessions
    const sitesWithManySessions = Object.entries(sessionsBySite)
      .filter(([site, sessions]) => sessions.length > 20)
      .sort((a, b) => b[1].length - a[1].length);
    
    if (sitesWithManySessions.length > 0) {
      console.log('\n⚠️ Sites with high session counts:');
      sitesWithManySessions.slice(0, 10).forEach(([site, sessions]) => {
        console.log(`  ${site}: ${sessions.length} sessions`);
      });
    }
    
    // 3. Check for unrealistic values
    const unrealisticSessions = sessions.filter(s => 
      parseFloat(s.operating_hours || 0) > 24 || 
      parseFloat(s.total_usage || 0) > 1000 ||
      parseFloat(s.cost_for_usage || 0) > 10000
    );
    
    if (unrealisticSessions.length > 0) {
      console.log('\n⚠️ Sessions with unrealistic values:');
      unrealisticSessions.slice(0, 5).forEach(session => {
        console.log(`  ${session.branch} (${session.session_date}): ${session.operating_hours}h, ${session.total_usage}L, R${session.cost_for_usage}`);
      });
    }
    
    // 4. Check data sources
    const dataByCompany = {};
    sessions.forEach(session => {
      const company = session.company || 'Unknown';
      if (!dataByCompany[company]) {
        dataByCompany[company] = { count: 0, hours: 0, fuel: 0, cost: 0 };
      }
      dataByCompany[company].count++;
      dataByCompany[company].hours += parseFloat(session.operating_hours || 0);
      dataByCompany[company].fuel += parseFloat(session.total_usage || 0);
      dataByCompany[company].cost += parseFloat(session.cost_for_usage || 0);
    });
    
    console.log('\n📊 DATA BY COMPANY:');
    Object.entries(dataByCompany).forEach(([company, data]) => {
      console.log(`  ${company}: ${data.count} sessions, ${data.hours.toFixed(1)}h, ${data.fuel.toFixed(1)}L, R${data.cost.toFixed(2)}`);
    });
    
    // 5. Check recent vs historical data
    const recentDate = new Date();
    recentDate.setDate(recentDate.getDate() - 7);
    const recentSessions = sessions.filter(s => new Date(s.session_date) > recentDate);
    const historicalSessions = sessions.filter(s => new Date(s.session_date) <= recentDate);
    
    console.log('\n📅 DATA BREAKDOWN:');
    console.log(`Recent (last 7 days): ${recentSessions.length} sessions`);
    console.log(`Historical (older): ${historicalSessions.length} sessions`);
    
    if (historicalSessions.length > recentSessions.length * 5) {
      console.log('⚠️ WARNING: Historical data significantly outweighs recent data - this may inflate totals');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkDashboardAccuracy();