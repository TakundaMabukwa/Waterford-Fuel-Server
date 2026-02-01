require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testActivityReport() {
  try {
    console.log('📊 Testing Activity Report Data...\n');
    
    // Get all sessions to see what data we have
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED')
      .order('session_date', { ascending: false });
    
    if (error) throw error;
    
    console.log(`Found ${sessions.length} completed sessions:\n`);
    
    // Analyze the data for activity report requirements
    let totalUsage = 0;
    let totalFill = 0;
    let totalOperatingHours = 0;
    let peakUsageDay = null;
    let peakUsageAmount = 0;
    const dailyUsage = {};
    const siteData = {};
    
    sessions.forEach(session => {
      console.log(`📋 Session: ${session.branch} (${session.cost_code})`);
      console.log(`   Date: ${session.session_date}`);
      console.log(`   Operating Hours: ${session.operating_hours}`);
      console.log(`   Fuel Usage: ${session.total_usage} liters`);
      console.log(`   Fuel Fill: ${session.total_fill || 'N/A'} liters`);
      console.log(`   Cost: R${session.cost_for_usage}`);
      console.log(`   Generator: ${session.branch} (Branch/Site)`);
      console.log('');
      
      // Aggregate data
      totalUsage += parseFloat(session.total_usage || 0);
      totalFill += parseFloat(session.total_fill || 0);
      totalOperatingHours += parseFloat(session.operating_hours || 0);
      
      // Track daily usage
      const date = session.session_date;
      if (!dailyUsage[date]) {
        dailyUsage[date] = 0;
      }
      dailyUsage[date] += parseFloat(session.total_usage || 0);
      
      // Find peak usage day
      if (dailyUsage[date] > peakUsageAmount) {
        peakUsageAmount = dailyUsage[date];
        peakUsageDay = date;
      }
      
      // Track by site/generator
      if (!siteData[session.branch]) {
        siteData[session.branch] = {
          sessions: 0,
          totalHours: 0,
          totalUsage: 0,
          totalFill: 0,
          totalCost: 0
        };
      }
      
      siteData[session.branch].sessions++;
      siteData[session.branch].totalHours += parseFloat(session.operating_hours || 0);
      siteData[session.branch].totalUsage += parseFloat(session.total_usage || 0);
      siteData[session.branch].totalFill += parseFloat(session.total_fill || 0);
      siteData[session.branch].totalCost += parseFloat(session.cost_for_usage || 0);
    });
    
    console.log('📊 ACTIVITY REPORT SUMMARY:');
    console.log('================================');
    console.log(`🔥 Peak Usage Day: ${peakUsageDay} (${peakUsageAmount.toFixed(2)} liters)`);
    console.log(`⏰ Total Operating Hours: ${totalOperatingHours.toFixed(2)} hours`);
    console.log(`⛽ Total Usage: ${totalUsage.toFixed(2)} liters`);
    console.log(`🚛 Total Fill: ${totalFill.toFixed(2)} liters`);
    console.log('');
    
    console.log('🏭 BY GENERATOR/SITE:');
    Object.entries(siteData).forEach(([site, data]) => {
      console.log(`   ${site}:`);
      console.log(`     Sessions: ${data.sessions}`);
      console.log(`     Operating Hours: ${data.totalHours.toFixed(2)}`);
      console.log(`     Usage: ${data.totalUsage.toFixed(2)} liters`);
      console.log(`     Fill: ${data.totalFill.toFixed(2)} liters`);
      console.log(`     Cost: R${data.totalCost.toFixed(2)}`);
      console.log('');
    });
    
    console.log('📅 DAILY BREAKDOWN:');
    Object.entries(dailyUsage)
      .sort(([a], [b]) => new Date(b) - new Date(a))
      .forEach(([date, usage]) => {
        console.log(`   ${date}: ${usage.toFixed(2)} liters`);
      });
    
  } catch (error) {
    console.error('❌ Error:', error);
  }
}

testActivityReport();