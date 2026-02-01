/**
 * Combine consecutive fuel fills within a time window
 * Takes multiple fill sessions and combines them into one session
 * using the earliest start fuel and latest end fuel
 */

/**
 * Combine fuel fill sessions within a time window (default 1 hour)
 * @param {Array} fillSessions - Array of fuel fill session objects
 * @param {Number} timeWindowHours - Time window in hours to combine fills (default 1)
 * @returns {Array} - Combined fuel fill sessions
 */
function combineFuelFills(fillSessions, timeWindowHours = 1) {
  if (!fillSessions || fillSessions.length === 0) {
    return [];
  }

  // Sort sessions by start time
  const sortedSessions = [...fillSessions].sort((a, b) => 
    new Date(a.session_start_time) - new Date(b.session_start_time)
  );

  const combined = [];
  let currentGroup = [sortedSessions[0]];

  for (let i = 1; i < sortedSessions.length; i++) {
    const current = sortedSessions[i];
    const lastInGroup = currentGroup[currentGroup.length - 1];
    
    const timeDiff = (new Date(current.session_start_time) - new Date(lastInGroup.session_start_time)) / (1000 * 60 * 60);
    
    // If within time window, add to current group
    if (timeDiff <= timeWindowHours) {
      currentGroup.push(current);
    } else {
      // Process current group and start new one
      combined.push(combineGroup(currentGroup));
      currentGroup = [current];
    }
  }

  // Process last group
  if (currentGroup.length > 0) {
    combined.push(combineGroup(currentGroup));
  }

  return combined;
}

/**
 * Combine a group of fill sessions into one
 * @param {Array} group - Array of fill sessions to combine
 * @returns {Object} - Combined fill session
 */
function combineGroup(group) {
  if (group.length === 1) {
    return { ...group[0], is_combined: false, fill_count: 1 };
  }

  // Get earliest and latest sessions
  const earliest = group[0];
  const latest = group[group.length - 1];

  // Calculate combined values
  const startFuel = parseFloat(earliest.opening_fuel || 0);
  const endFuel = parseFloat(latest.closing_fuel || 0);
  const totalFilled = Math.max(0, endFuel - startFuel);

  // Calculate total duration
  const startTime = new Date(earliest.session_start_time);
  const endTime = new Date(latest.session_end_time || latest.session_start_time);
  const durationMs = endTime - startTime;
  const durationSeconds = durationMs / 1000;
  const durationHours = durationMs / (1000 * 60 * 60);

  // Format duration as "H hours M minutes S seconds"
  const hours = Math.floor(durationSeconds / 3600);
  const minutes = Math.floor((durationSeconds % 3600) / 60);
  const seconds = Math.floor(durationSeconds % 60);
  const durationFormatted = `${hours} hours ${minutes} minutes ${seconds} seconds`;

  return {
    branch: earliest.branch,
    cost_code: earliest.cost_code,
    company: earliest.company,
    session_start_time: earliest.session_start_time,
    session_end_time: latest.session_end_time || latest.session_start_time,
    opening_fuel: startFuel,
    opening_percentage: parseFloat(earliest.opening_percentage || 0),
    closing_fuel: endFuel,
    closing_percentage: parseFloat(latest.closing_percentage || 0),
    total_fill: totalFilled,
    operating_hours: durationHours,
    duration_formatted: durationFormatted,
    duration_seconds: durationSeconds,
    is_combined: true,
    fill_count: group.length,
    combined_fills: group.map(f => ({
      time: f.session_start_time,
      opening_fuel: f.opening_fuel,
      closing_fuel: f.closing_fuel,
      fill_amount: f.total_fill
    })),
    session_status: 'FUEL_FILL_COMPLETED',
    notes: `Combined ${group.length} fuel fills. Total filled: ${totalFilled.toFixed(2)}L over ${durationFormatted}`
  };
}

/**
 * Format duration from seconds to readable string
 * @param {Number} seconds - Duration in seconds
 * @returns {String} - Formatted duration string
 */
function formatDuration(seconds) {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  return `${hours} hours ${minutes} minutes ${secs} seconds`;
}

module.exports = {
  combineFuelFills,
  combineGroup,
  formatDuration
};
