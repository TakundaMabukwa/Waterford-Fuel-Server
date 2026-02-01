require('dotenv').config();
const { supabase } = require('./supabase-client');

async function testFuelFillAccuracy() {
  console.log('\n🔍 FUEL FILL ACCURACY TEST');
  console.log('=====================================');
  
  const today = new Date().toISOString().split('T')[0];
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  
  try {
    // 1. Check which tables store fuel fill data
    console.log('\n📊 FUEL FILL DATA SOURCES:');
    console.log('1️⃣ Primary Table: energy_rite_fuel_fills');
    console.log('2️⃣ Session Data: energy_rite_operating_sessions (total_fill column)');
    console.log('3️⃣ Raw Data: energy_rite_fuel_data (level changes)');
    
    // 2. Get fuel fills from dedicated table
    const { data: fuelFills, error: fillsError } = await supabase
      .from('energy_rite_fuel_fills')
      .select('*')
      .gte('fill_date', yesterday)
      .order('fill_date', { ascending: false })
      .limit(10);
    
    if (fillsError) {
      console.log('❌ Error getting fuel fills:', fillsError.message);
    } else {
      console.log(`\n🛢️ RECENT FUEL FILLS (Last 2 days):`);
      console.log(`Found ${fuelFills.length} fuel fill events`);
      
      if (fuelFills.length > 0) {
        fuelFills.forEach((fill, index) => {
          console.log(`\n${index + 1}. Site: ${fill.plate}`);
          console.log(`   Date: ${new Date(fill.fill_date).toLocaleString()}`);
          console.log(`   Day: ${fill.fill_day}`);
          console.log(`   Amount: ${fill.fill_amount}L`);
          console.log(`   Before: ${fill.fuel_before}L → After: ${fill.fuel_after}L`);
          console.log(`   Method: ${fill.detection_method}`);
          console.log(`   Company: ${fill.company || 'N/A'}`);
          console.log(`   Cost Code: ${fill.cost_code || 'N/A'}`);
        });
      } else {
        console.log('   No fuel fills found in last 2 days');
      }
    }
    
    // 3. Check accuracy by site and day
    console.log('\n📅 ACCURACY BY SITE AND DAY:');
    
    // Group by site and day
    const fillsByDay = {};
    if (fuelFills) {
      fuelFills.forEach(fill => {
        const day = fill.fill_day;
        const site = fill.plate;
        
        if (!fillsByDay[day]) {
          fillsByDay[day] = {};
        }
        if (!fillsByDay[day][site]) {
          fillsByDay[day][site] = [];
        }
        fillsByDay[day][site].push(fill);
      });
      
      Object.keys(fillsByDay).forEach(day => {
        console.log(`\n📆 ${day}:`);
        Object.keys(fillsByDay[day]).forEach(site => {
          const fills = fillsByDay[day][site];
          const totalFilled = fills.reduce((sum, fill) => sum + parseFloat(fill.fill_amount || 0), 0);
          console.log(`   🚛 ${site}: ${fills.length} fills, ${totalFilled.toFixed(2)}L total`);
          fills.forEach(fill => {
            console.log(`      ⏰ ${new Date(fill.fill_date).toLocaleTimeString()}: +${fill.fill_amount}L (${fill.detection_method})`);
          });
        });
      });
    }
    
    // 4. Compare with session data
    console.log('\n🔄 SESSION DATA COMPARISON:');
    const { data: sessions, error: sessionError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('branch, total_fill, fill_events, session_date')
      .gte('session_date', yesterday)
      .gt('total_fill', 0)
      .order('session_date', { ascending: false });
    
    if (sessionError) {
      console.log('❌ Error getting session data:', sessionError.message);
    } else if (sessions && sessions.length > 0) {
      console.log(`Found ${sessions.length} sessions with fuel fills:`);
      sessions.forEach(session => {
        console.log(`   📅 ${session.session_date} - ${session.branch}: ${session.total_fill}L (${session.fill_events || 0} events)`);
      });
    } else {
      console.log('No session data with fuel fills found');
    }
    
    // 5. Detection method statistics
    const methodStats = {};
    if (fuelFills) {
      fuelFills.forEach(fill => {
        const method = fill.detection_method || 'UNKNOWN';
        if (!methodStats[method]) {
          methodStats[method] = { count: 0, totalAmount: 0 };
        }
        methodStats[method].count++;
        methodStats[method].totalAmount += parseFloat(fill.fill_amount || 0);
      });
      
      console.log('\n📈 DETECTION METHOD STATISTICS:');
      Object.keys(methodStats).forEach(method => {
        const stats = methodStats[method];
        console.log(`   ${method}: ${stats.count} fills, ${stats.totalAmount.toFixed(2)}L total`);
      });
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  }
}

// Run the test
testFuelFillAccuracy();