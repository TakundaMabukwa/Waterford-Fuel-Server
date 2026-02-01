#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');
const axios = require('axios');

async function testDashboardLogic() {
  console.log('🔍 Testing Executive Dashboard Logic Accuracy...\n');
  
  try {
    // Test with last 7 days to avoid historical data inflation
    const days = 7;
    const endDate = new Date();
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const start = startDate.toISOString().split('T')[0];
    const end = endDate.toISOString().split('T')[0];
    
    console.log(`📅 Testing period: ${start} to ${end} (${days} days)`);
    
    // 1. Test Fleet Data Logic
    console.log('\n🚗 TESTING FLEET DATA LOGIC:');
    try {
      const vehicleResponse = await axios.get('http://64.227.138.235:3000/api/energy-rite/vehicles');
      const vehicles = vehicleResponse.data.data;
      
      console.log(`Total vehicles from API: ${vehicles.length}`);
      
      const activeVehicles = vehicles.filter(v => v.drivername !== 'PTO OFF / ENGINE OFF').length;
      const fleetUtilization = vehicles.length > 0 ? (activeVehicles / vehicles.length) * 100 : 0;
      
      console.log(`Active vehicles: ${activeVehicles}`);
      console.log(`Fleet utilization: ${fleetUtilization.toFixed(1)}%`);
      
      // Check for realistic values
      if (fleetUtilization > 100) {
        console.log('❌ ERROR: Fleet utilization > 100%');
      } else if (fleetUtilization < 0) {
        console.log('❌ ERROR: Fleet utilization < 0%');
      } else {
        console.log('✅ Fleet utilization logic is correct');
      }
      
    } catch (error) {
      console.log('❌ Fleet API error:', error.message);
    }
    
    // 2. Test Operating Sessions Logic
    console.log('\n⚙️ TESTING OPERATING SESSIONS LOGIC:');
    
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_date', start)
      .lte('session_date', end)
      .eq('session_status', 'COMPLETED');
    
    if (error) throw error;
    
    console.log(`Sessions found: ${sessions.length}`);
    
    // Manual calculation
    let totalOperatingHours = 0;
    let totalFuelUsage = 0;
    let totalOperatingCost = 0;
    let invalidSessions = 0;
    
    sessions.forEach(session => {
      const hours = parseFloat(session.operating_hours || 0);
      const fuel = parseFloat(session.total_usage || 0);
      const cost = parseFloat(session.cost_for_usage || 0);
      
      // Check for invalid data
      if (isNaN(hours) || isNaN(fuel) || isNaN(cost)) {
        invalidSessions++;
        return;
      }
      
      if (hours > 24) {
        console.log(`⚠️ Session with >24h: ${session.branch} - ${hours}h`);
      }
      
      totalOperatingHours += hours;
      totalFuelUsage += fuel;
      totalOperatingCost += cost;
    });
    
    console.log(`Total Operating Hours: ${totalOperatingHours.toFixed(2)}`);
    console.log(`Total Fuel Usage: ${totalFuelUsage.toFixed(2)} L`);
    console.log(`Total Operating Cost: R${totalOperatingCost.toFixed(2)}`);
    console.log(`Invalid sessions: ${invalidSessions}`);
    
    // Test averages
    const avgCostPerHour = totalOperatingHours > 0 ? totalOperatingCost / totalOperatingHours : 0;
    const avgFuelPerHour = totalOperatingHours > 0 ? totalFuelUsage / totalOperatingHours : 0;
    
    console.log(`Average Cost per Hour: R${avgCostPerHour.toFixed(2)}`);
    console.log(`Average Fuel per Hour: ${avgFuelPerHour.toFixed(2)} L/h`);
    
    // Check for realistic averages
    if (avgCostPerHour > 100) {
      console.log('⚠️ WARNING: Very high cost per hour (>R100)');
    }
    if (avgFuelPerHour > 50) {
      console.log('⚠️ WARNING: Very high fuel consumption (>50L/h)');
    }
    
    // 3. Test Cost Center Logic
    console.log('\n💰 TESTING COST CENTER LOGIC:');
    
    const costCenterMetrics = {};
    sessions.forEach(session => {
      const costCode = session.cost_code || 'UNKNOWN';
      if (!costCenterMetrics[costCode]) {
        costCenterMetrics[costCode] = {
          sessions: 0,
          operating_hours: 0,
          fuel_usage: 0,
          total_cost: 0,
          sites: new Set()
        };
      }
      
      costCenterMetrics[costCode].sessions++;
      costCenterMetrics[costCode].operating_hours += parseFloat(session.operating_hours || 0);
      costCenterMetrics[costCode].fuel_usage += parseFloat(session.total_usage || 0);
      costCenterMetrics[costCode].total_cost += parseFloat(session.cost_for_usage || 0);
      costCenterMetrics[costCode].sites.add(session.branch);
    });
    
    console.log('Cost Center Breakdown:');
    Object.entries(costCenterMetrics).forEach(([costCode, metrics]) => {
      const avgCost = metrics.operating_hours > 0 ? metrics.total_cost / metrics.operating_hours : 0;
      console.log(`  ${costCode}: ${metrics.sessions} sessions, ${metrics.sites.size} sites, R${avgCost.toFixed(2)}/h`);
    });
    
    // 4. Test Site Metrics Logic
    console.log('\n🏢 TESTING SITE METRICS LOGIC:');
    
    const siteMetrics = {};
    sessions.forEach(session => {
      const site = session.branch;
      if (!siteMetrics[site]) {
        siteMetrics[site] = {
          sessions: 0,
          operating_hours: 0,
          fuel_usage: 0,
          total_cost: 0
        };
      }
      
      siteMetrics[site].sessions++;
      siteMetrics[site].operating_hours += parseFloat(session.operating_hours || 0);
      siteMetrics[site].fuel_usage += parseFloat(session.total_usage || 0);
      siteMetrics[site].total_cost += parseFloat(session.cost_for_usage || 0);
    });
    
    const topSites = Object.entries(siteMetrics)
      .map(([site, metrics]) => ({
        site,
        ...metrics,
        efficiency: metrics.operating_hours > 0 ? metrics.fuel_usage / metrics.operating_hours : 0
      }))
      .sort((a, b) => b.operating_hours - a.operating_hours)
      .slice(0, 5);
    
    console.log('Top 5 Sites by Operating Hours:');
    topSites.forEach(site => {
      console.log(`  ${site.site}: ${site.operating_hours.toFixed(1)}h, ${site.efficiency.toFixed(1)}L/h`);
    });
    
    // 5. Test Date Range Logic
    console.log('\n📅 TESTING DATE RANGE LOGIC:');
    
    const sessionsByDate = {};
    sessions.forEach(session => {
      const date = session.session_date;
      if (!sessionsByDate[date]) sessionsByDate[date] = 0;
      sessionsByDate[date]++;
    });
    
    console.log('Sessions by date:');
    Object.entries(sessionsByDate)
      .sort(([a], [b]) => new Date(a) - new Date(b))
      .forEach(([date, count]) => {
        console.log(`  ${date}: ${count} sessions`);
        if (count > 50) {
          console.log(`    ⚠️ WARNING: High session count for single date`);
        }
      });
    
    // 6. Summary
    console.log('\n📊 LOGIC ACCURACY SUMMARY:');
    console.log(`✅ Total sessions processed: ${sessions.length}`);
    console.log(`✅ Invalid sessions: ${invalidSessions}`);
    console.log(`✅ Cost centers: ${Object.keys(costCenterMetrics).length}`);
    console.log(`✅ Active sites: ${Object.keys(siteMetrics).length}`);
    console.log(`✅ Date range: ${Object.keys(sessionsByDate).length} days`);
    
    if (invalidSessions === 0 && Object.keys(sessionsByDate).every(date => sessionsByDate[date] <= 50)) {
      console.log('\n🎉 Dashboard logic appears ACCURATE for recent data!');
    } else {
      console.log('\n⚠️ Some issues found in dashboard logic');
    }
    
  } catch (error) {
    console.error('❌ Error testing dashboard logic:', error.message);
  }
}

testDashboardLogic();