#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function monitorNewSessions() {
  console.log('🔍 Monitoring new sessions for proper fuel data...\n');
  
  try {
    // Get sessions created in the last hour
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    
    const { data: recentSessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gte('session_start_time', oneHourAgo)
      .order('session_start_time', { ascending: false })
      .limit(10);
    
    if (error) throw error;
    
    console.log(`📊 Found ${recentSessions.length} sessions in the last hour:\n`);
    
    let sessionsWithData = 0;
    let sessionsWithZeros = 0;
    
    recentSessions.forEach(session => {
      const hasRealData = session.opening_fuel > 0 || session.opening_percentage > 0;
      
      console.log(`🏢 ${session.branch} - ${session.session_start_time}`);
      console.log(`   Opening: ${session.opening_fuel || 0}L (${session.opening_percentage || 0}%)`);
      console.log(`   Temperature: ${session.opening_temperature || 0}°C`);
      console.log(`   Volume: ${session.opening_volume || 0}L`);
      console.log(`   Status: ${session.session_status}`);
      console.log(`   ${hasRealData ? '✅ HAS REAL DATA' : '❌ ZERO VALUES'}\n`);
      
      if (hasRealData) {
        sessionsWithData++;
      } else {
        sessionsWithZeros++;
      }
    });
    
    console.log('📈 Summary:');
    console.log(`   ✅ Sessions with real fuel data: ${sessionsWithData}`);
    console.log(`   ❌ Sessions with zero values: ${sessionsWithZeros}`);
    
    if (sessionsWithData > 0) {
      console.log('\n🎉 SUCCESS: New sessions are being created with proper fuel data!');
    } else if (recentSessions.length === 0) {
      console.log('\n⏳ No new sessions found in the last hour. Wait for engine events to occur.');
    } else {
      console.log('\n⚠️ WARNING: New sessions still have zero fuel values. Check the WebSocket connection and external API.');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

monitorNewSessions();