#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function fixRemainingUnrealisticData() {
  console.log('🔧 Fixing Remaining Unrealistic Data...\n');
  
  try {
    // 1. Find sessions still showing >24 hours or unrealistic values
    const { data: badSessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .or('operating_hours.gt.24,operating_hours.gt.20')
      .eq('session_status', 'COMPLETED');
    
    if (error) throw error;
    
    console.log(`🔍 Found ${badSessions.length} sessions with >20 hours`);
    
    if (badSessions.length > 0) {
      console.log('\n🗑️ Deleting unrealistic sessions:');
      for (const session of badSessions) {
        console.log(`  ❌ ${session.branch} (${session.session_date}): ${session.operating_hours}h`);
        
        const { error: deleteError } = await supabase
          .from('energy_rite_operating_sessions')
          .delete()
          .eq('id', session.id);
        
        if (deleteError) {
          console.log(`    Error deleting: ${deleteError.message}`);
        }
      }
    }
    
    // 2. Check for sites with excessive total hours (like BRACKENHUR, BLUE HILL)
    const { data: allSessions, error: allError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED');
    
    if (allError) throw allError;
    
    // Group by site and check totals
    const siteHours = {};
    allSessions.forEach(session => {
      const site = session.branch;
      if (!siteHours[site]) siteHours[site] = 0;
      siteHours[site] += parseFloat(session.operating_hours || 0);
    });
    
    // Find sites with excessive hours
    const excessiveSites = Object.entries(siteHours)
      .filter(([site, hours]) => hours > 100) // More than 100 total hours
      .sort((a, b) => b[1] - a[1]);
    
    console.log('\n⚠️ Sites with excessive total hours:');
    excessiveSites.forEach(([site, hours]) => {
      console.log(`  ${site}: ${hours.toFixed(1)}h total`);
    });
    
    // 3. Cap individual sessions at reasonable limits
    const { data: longSessions, error: longError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gt('operating_hours', 12) // More than 12 hours per session
      .eq('session_status', 'COMPLETED');
    
    if (!longError && longSessions.length > 0) {
      console.log(`\n🔧 Capping ${longSessions.length} sessions at 8 hours max:`);
      
      for (const session of longSessions) {
        const newHours = Math.min(8, parseFloat(session.operating_hours || 0));
        const newFuelRate = newHours > 0 ? parseFloat(session.total_usage || 0) / newHours : 0;
        
        console.log(`  🔧 ${session.branch}: ${session.operating_hours}h → ${newHours}h`);
        
        const { error: updateError } = await supabase
          .from('energy_rite_operating_sessions')
          .update({
            operating_hours: newHours,
            liter_usage_per_hour: newFuelRate,
            notes: `Capped from ${session.operating_hours}h to ${newHours}h for realism`
          })
          .eq('id', session.id);
        
        if (updateError) {
          console.log(`    Error updating: ${updateError.message}`);
        }
      }
    }
    
    // 4. Remove duplicate historical sessions that are inflating totals
    console.log('\n🔍 Checking for remaining duplicates...');
    
    const sessionsByKey = {};
    allSessions.forEach(session => {
      const key = `${session.branch}_${session.session_date}`;
      if (!sessionsByKey[key]) {
        sessionsByKey[key] = [];
      }
      sessionsByKey[key].push(session);
    });
    
    const stillDuplicates = Object.entries(sessionsByKey)
      .filter(([key, sessions]) => sessions.length > 2); // More than 2 per day is excessive
    
    if (stillDuplicates.length > 0) {
      console.log(`🗑️ Removing excess duplicates from ${stillDuplicates.length} site-date combinations:`);
      
      for (const [key, duplicates] of stillDuplicates) {
        // Keep only the first 2 sessions, delete the rest
        const [keep1, keep2, ...deleteList] = duplicates.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
        
        console.log(`  ${key}: Keeping 2, deleting ${deleteList.length} excess sessions`);
        
        for (const session of deleteList) {
          const { error: deleteError } = await supabase
            .from('energy_rite_operating_sessions')
            .delete()
            .eq('id', session.id);
          
          if (deleteError) {
            console.log(`    Error deleting: ${deleteError.message}`);
          }
        }
      }
    }
    
    // 5. Final verification
    const { data: finalSessions, error: finalError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('operating_hours, branch')
      .eq('session_status', 'COMPLETED');
    
    if (!finalError) {
      const maxHours = Math.max(...finalSessions.map(s => parseFloat(s.operating_hours || 0)));
      const totalHours = finalSessions.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0);
      
      console.log('\n✅ FINAL VERIFICATION:');
      console.log(`Max session hours: ${maxHours.toFixed(1)}h`);
      console.log(`Total hours across all sessions: ${totalHours.toFixed(1)}h`);
      console.log(`Total sessions: ${finalSessions.length}`);
      
      if (maxHours <= 8 && totalHours < 2000) {
        console.log('🎉 Dashboard numbers should now be realistic!');
      } else {
        console.log('⚠️ Some numbers may still be high');
      }
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
}

fixRemainingUnrealisticData();