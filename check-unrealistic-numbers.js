#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function checkUnrealisticNumbers() {
  console.log('🔍 Checking for Unrealistic Dashboard Numbers...\n');
  
  try {
    // Check last 7 days only (recent data)
    const endDate = new Date();
    const startDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const start = startDate.toISOString().split('T')[0];
    const end = endDate.toISOString().split('T')[0];
    
    console.log(`📅 Checking recent period: ${start} to ${end}`);
    
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', start)
      .lte('session_date', end)
      .eq('session_status', 'COMPLETED');
    
    if (error) throw error;
    
    console.log(`📊 Recent sessions: ${sessions.length}`);
    
    // Check individual session values
    console.log('\n🔍 CHECKING INDIVIDUAL SESSION VALUES:');
    
    const suspiciousSessions = sessions.filter(s => {
      const hours = parseFloat(s.operating_hours || 0);
      const fuel = parseFloat(s.total_usage || 0);
      const cost = parseFloat(s.cost_for_usage || 0);
      const costPerHour = hours > 0 ? cost / hours : 0;
      const fuelPerHour = hours > 0 ? fuel / hours : 0;
      
      return (
        hours > 20 ||           // More than 20 hours per day
        fuel > 500 ||           // More than 500L per session
        cost > 5000 ||          // More than R5000 per session
        costPerHour > 200 ||    // More than R200 per hour
        fuelPerHour > 30        // More than 30L per hour
      );
    });
    
    if (suspiciousSessions.length > 0) {
      console.log(`⚠️ Found ${suspiciousSessions.length} suspicious sessions:`);
      suspiciousSessions.forEach(s => {
        const hours = parseFloat(s.operating_hours || 0);
        const fuel = parseFloat(s.total_usage || 0);
        const cost = parseFloat(s.cost_for_usage || 0);
        const costPerHour = hours > 0 ? cost / hours : 0;
        const fuelPerHour = hours > 0 ? fuel / hours : 0;
        
        console.log(`  ${s.branch} (${s.session_date}): ${hours}h, ${fuel}L, R${cost} (R${costPerHour.toFixed(0)}/h, ${fuelPerHour.toFixed(1)}L/h)`);
      });
    } else {
      console.log('✅ No suspicious individual sessions found');
    }
    
    // Check totals
    console.log('\n📊 CHECKING TOTAL VALUES:');
    
    const totalHours = sessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0);
    const totalFuel = sessions.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0);
    const totalCost = sessions.reduce((sum, s) => sum + parseFloat(s.cost_for_usage || 0), 0);
    
    console.log(`Total Hours: ${totalHours.toFixed(1)}h`);
    console.log(`Total Fuel: ${totalFuel.toFixed(1)}L`);
    console.log(`Total Cost: R${totalCost.toFixed(2)}`);
    
    const avgCostPerHour = totalHours > 0 ? totalCost / totalHours : 0;
    const avgFuelPerHour = totalHours > 0 ? totalFuel / totalHours : 0;
    
    console.log(`Average Cost/Hour: R${avgCostPerHour.toFixed(2)}`);
    console.log(`Average Fuel/Hour: ${avgFuelPerHour.toFixed(2)}L`);
    
    // Check if totals are realistic for 7 days
    const avgHoursPerDay = totalHours / 7;
    const avgCostPerDay = totalCost / 7;
    
    console.log(`\nDaily Averages:`);
    console.log(`Hours per day: ${avgHoursPerDay.toFixed(1)}h`);
    console.log(`Cost per day: R${avgCostPerDay.toFixed(2)}`);
    
    // Realistic checks
    console.log('\n🎯 REALISM CHECKS:');
    
    if (avgCostPerHour > 100) {
      console.log(`❌ UNREALISTIC: Average cost per hour (R${avgCostPerHour.toFixed(2)}) is very high`);
    } else {
      console.log(`✅ Average cost per hour seems reasonable`);
    }
    
    if (avgFuelPerHour > 20) {
      console.log(`❌ UNREALISTIC: Average fuel per hour (${avgFuelPerHour.toFixed(2)}L) is very high`);
    } else {
      console.log(`✅ Average fuel per hour seems reasonable`);
    }
    
    if (avgHoursPerDay > 200) {
      console.log(`❌ UNREALISTIC: Average hours per day (${avgHoursPerDay.toFixed(1)}h) across all sites is too high`);
    } else {
      console.log(`✅ Average hours per day seems reasonable`);
    }
    
    // Check for data source issues
    console.log('\n📋 DATA SOURCE BREAKDOWN:');
    const byCompany = {};
    sessions.forEach(s => {
      const company = s.company || 'Unknown';
      if (!byCompany[company]) {
        byCompany[company] = { count: 0, hours: 0, cost: 0 };
      }
      byCompany[company].count++;
      byCompany[company].hours += parseFloat(s.operating_hours || 0);
      byCompany[company].cost += parseFloat(s.cost_for_usage || 0);
    });
    
    Object.entries(byCompany).forEach(([company, data]) => {
      const avgCost = data.hours > 0 ? data.cost / data.hours : 0;
      console.log(`${company}: ${data.count} sessions, ${data.hours.toFixed(1)}h, R${avgCost.toFixed(2)}/h`);
      
      if (avgCost > 150) {
        console.log(`  ⚠️ High cost per hour for ${company}`);
      }
    });
    
    // Check for historical vs recent data mixing
    const today = new Date().toISOString().split('T')[0];
    const todaySessions = sessions.filter(s => s.session_date === today);
    const olderSessions = sessions.filter(s => s.session_date !== today);
    
    console.log(`\n📅 DATA RECENCY:`);
    console.log(`Today's sessions: ${todaySessions.length}`);
    console.log(`Older sessions (last 7 days): ${olderSessions.length}`);
    
    if (olderSessions.length > todaySessions.length * 10) {
      console.log(`⚠️ WARNING: Much more historical data than recent data`);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

checkUnrealisticNumbers();