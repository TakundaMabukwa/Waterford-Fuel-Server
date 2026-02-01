const pendingFuelDb = require('./pending-fuel-db');

(async () => {
  console.log('🔍 Checking KROONSTAD2 fuel data around 06:02:23...\n');

  await pendingFuelDb.initDatabase();
  const db = pendingFuelDb.getDb();

  // Session end: 2026-01-23 06:02:23 UTC = 2026-01-23 08:02:23 +02 (LocTime)
  // Check 05:50 to 06:15 (LocTime)
  const startTime = new Date('2026-01-23T07:50:00Z').getTime(); // 05:50 LocTime
  const endTime = new Date('2026-01-23T08:15:00Z').getTime();   // 06:15 LocTime

  const stmt = db.prepare(`
    SELECT plate, fuel_volume, fuel_percentage, loc_time, timestamp 
    FROM fuel_history 
    WHERE plate = 'KROONSTAD2' 
    AND timestamp >= ? 
    AND timestamp <= ?
    ORDER BY timestamp ASC
  `);
  
  stmt.bind([startTime, endTime]);
  
  const rows = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();

  if (rows.length === 0) {
    console.log('❌ No fuel data found for KROONSTAD2 between 05:50 and 06:15');
  } else {
    console.log(`✅ Found ${rows.length} fuel entries:\n`);
    rows.forEach((row, idx) => {
      const date = new Date(row.timestamp);
      console.log(`${idx + 1}. ${row.loc_time} - ${row.fuel_volume}L (${row.fuel_percentage}%)`);
    });
    
    console.log('\n📊 Session: 05:46:06 to 06:02:23');
    console.log('Last fuel BEFORE 06:02:23:', rows.filter(r => r.loc_time < '2026-01-23 06:02:23').pop()?.loc_time || 'NONE');
    console.log('First fuel AFTER 06:02:23:', rows.find(r => r.loc_time > '2026-01-23 06:02:23')?.loc_time || 'NONE');
  }

  pendingFuelDb.closeDatabase();
})();
