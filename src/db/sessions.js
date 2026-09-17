const { createClient } = require('@supabase/supabase-js');

const supabase = process.env.SUPABASE_URL && (process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY)
  : null;

if (!supabase) {
  console.error('[db] *** CRITICAL: Supabase credentials not set - fill/theft sessions will NOT be recorded ***');
} else {
  console.log('[db] Supabase client ready (URL: ' + (process.env.SUPABASE_URL || 'missing') + ')');
}

const insertFillSession = async (session) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot insert fill session ***');
    return;
  }
  const { data, error } = await supabase.from('energy_rite_operating_sessions').insert(session).select();
  if (error) {
    console.error(`[db] Supabase fill insert FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
  } else {
    console.log(`[db] Fill session inserted to Supabase: ${session.branch} ${session.session_date}`);
  }
};

const insertTheftSession = async (session) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot insert theft session ***');
    return;
  }
  const { data, error } = await supabase.from('energy_rite_operating_sessions').insert(session).select();
  if (error) {
    console.error(`[db] Supabase theft insert FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
  } else {
    console.log(`[db] Theft session inserted to Supabase: ${session.branch} ${session.session_date}`);
  }
};

const getOngoingSession = async (plate) => {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from('energy_rite_operating_sessions')
    .select('id, session_start_time, opening_fuel')
    .eq('branch', plate)
    .eq('session_status', 'ONGOING')
    .order('session_start_time', { ascending: false })
    .limit(1);
  if (error) {
    console.error(`[db] getOngoingSession error: ${error.message}`);
    return null;
  }
  return data && data.length > 0 ? data[0] : null;
};

const insertOperatingSession = async (session) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot insert session ***');
    return null;
  }
  const { data, error } = await supabase.from('energy_rite_operating_sessions').insert(session).select('id');
  if (error) {
    console.error(`[db] Supabase session insert FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
    return null;
  }
  return data && data.length > 0 ? data[0].id : null;
};

const closeOperatingSession = async (sessionId, closingData) => {
  if (!supabase) {
    console.error('[db] *** BLOCKED: Supabase not configured - cannot close session ***');
    return;
  }
  const { error } = await supabase.from('energy_rite_operating_sessions')
    .update(closingData)
    .eq('id', sessionId);
  if (error) {
    console.error(`[db] Supabase session close FAILED: ${error.message}`);
    console.error(`[db] Details: ${error.details || 'none'} | hint: ${error.hint || 'none'} | code: ${error.code || 'none'}`);
  }
};

module.exports = { insertFillSession, insertTheftSession, getOngoingSession, insertOperatingSession, closeOperatingSession };