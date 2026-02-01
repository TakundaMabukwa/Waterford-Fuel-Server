#!/usr/bin/env node
require('dotenv').config();
const { supabase } = require('./supabase-client');

async function cleanupInflatedData() {
  console.log('🧹 Cleaning up inflated dashboard data...\n');
  
  try {
    // Get all sessions to analyze
    const { data: sessions, error } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .eq('session_status', 'COMPLETED');
    
    if (error) throw error;
    
    console.log(`📊 Found ${sessions.length} total sessions`);
    
    let deletedCount = 0;
    let updatedCount = 0;
    
    // 1. Delete sessions with unrealistic operating hours (>24h)
    const unrealisticSessions = sessions.filter(s => parseFloat(s.operating_hours || 0) > 24);
    
    if (unrealisticSessions.length > 0) {
      console.log(`\n🗑️ Deleting ${unrealisticSessions.length} sessions with >24h operating hours...`);
      
      for (const session of unrealisticSessions) {
        const { error: deleteError } = await supabase
          .from('energy_rite_operating_sessions')
          .delete()
          .eq('id', session.id);
        
        if (!deleteError) {
          deletedCount++;
          console.log(`  ❌ Deleted: ${session.branch} (${session.session_date}) - ${session.operating_hours}h`);
        }
      }
    }
    
    // 2. Find and consolidate duplicate sessions (same site + same date)
    const sessionsByKey = {};
    const remainingSessions = sessions.filter(s => parseFloat(s.operating_hours || 0) <= 24);
    
    remainingSessions.forEach(session => {
      const key = `${session.branch}_${session.session_date}`;
      if (!sessionsByKey[key]) {
        sessionsByKey[key] = [];
      }
      sessionsByKey[key].push(session);
    });
    
    // Find duplicates (multiple sessions for same site on same date)
    const duplicateGroups = Object.entries(sessionsByKey)
      .filter(([key, sessions]) => sessions.length > 1);
    
    if (duplicateGroups.length > 0) {
      console.log(`\n🔄 Consolidating ${duplicateGroups.length} groups of duplicate sessions...`);
      
      for (const [key, duplicates] of duplicateGroups) {
        // Keep the first session, consolidate data from others
        const [keepSession, ...deleteList] = duplicates.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
        
        // Aggregate data from all duplicates
        const totalHours = duplicates.reduce((sum, s) => sum + parseFloat(s.operating_hours || 0), 0);
        const totalUsage = duplicates.reduce((sum, s) => sum + parseFloat(s.total_usage || 0), 0);
        const totalFill = duplicates.reduce((sum, s) => sum + parseFloat(s.total_fill || 0), 0);
        const totalCost = duplicates.reduce((sum, s) => sum + parseFloat(s.cost_for_usage || 0), 0);
        
        // Cap at 24 hours max per day
        const cappedHours = Math.min(totalHours, 24);
        
        // Update the kept session with consolidated data
        const { error: updateError } = await supabase
          .from('energy_rite_operating_sessions')
          .update({
            operating_hours: cappedHours,
            total_usage: totalUsage,
            total_fill: totalFill,
            cost_for_usage: totalCost,
            liter_usage_per_hour: cappedHours > 0 ? totalUsage / cappedHours : 0,
            notes: `Consolidated from ${duplicates.length} sessions`
          })
          .eq('id', keepSession.id);
        
        if (!updateError) {
          updatedCount++;
          console.log(`  ✅ Consolidated: ${keepSession.branch} (${keepSession.session_date}) - ${duplicates.length} sessions → ${cappedHours.toFixed(1)}h`);
        }
        
        // Delete the duplicate sessions
        for (const duplicate of deleteList) {
          const { error: deleteError } = await supabase
            .from('energy_rite_operating_sessions')
            .delete()
            .eq('id', duplicate.id);
          
          if (!deleteError) {
            deletedCount++;
          }
        }
      }
    }
    
    // 3. Cap any remaining sessions at 24 hours
    const { data: longSessions, error: longError } = await supabase
      .from('energy_rite_operating_sessions')
      .select('*')
      .gt('operating_hours', 24)
      .eq('session_status', 'COMPLETED');
    
    if (!longError && longSessions.length > 0) {
      console.log(`\n⏰ Capping ${longSessions.length} sessions at 24 hours...`);
      
      for (const session of longSessions) {
        const { error: capError } = await supabase
          .from('energy_rite_operating_sessions')
          .update({
            operating_hours: 24,
            liter_usage_per_hour: parseFloat(session.total_usage || 0) / 24,
            notes: `Capped from ${session.operating_hours}h to 24h`
          })
          .eq('id', session.id);
        
        if (!capError) {
          updatedCount++;
          console.log(`  🔧 Capped: ${session.branch} (${session.session_date}) - ${session.operating_hours}h → 24h`);
        }
      }
    }
    
    console.log('\n✅ CLEANUP COMPLETED:');
    console.log(`🗑️ Deleted: ${deletedCount} sessions`);
    console.log(`🔄 Updated: ${updatedCount} sessions`);
    console.log('\n📊 Dashboard numbers should now be accurate!');
    
  } catch (error) {
    console.error('❌ Error during cleanup:', error.message);
  }
}

cleanupInflatedData();