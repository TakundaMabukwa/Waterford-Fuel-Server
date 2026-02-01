const pendingFuelDb = require('./pending-fuel-db');

(async () => {
  console.log('🔍 Checking SQLite for KROONSTAD2 fuel data...\n');

  // Initialize database
  await pendingFuelDb.initDatabase();
  const db = pendingFuelDb.getDb();

  // Get last 10 fuel entries for KROONSTAD2
  const stmt = db.prepare(`
    SELECT plate, fuel_volume, fuel_percentage, loc_time, timestamp 
    FROM fuel_history 
    WHERE plate = 'FARRAMERE' 
    ORDER BY timestamp DESC 
    LIMIT 10
  `);
  
  const rows = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();

  if (rows.length === 0) {
    console.log('❌ No fuel data found for KROONSTAD2 in SQLite');
  } else {
    console.log(`✅ Found ${rows.length} fuel entries for KROONSTAD2:\n`);
    rows.forEach((row, idx) => {
      const date = new Date(row.timestamp);
      console.log(`${idx + 1}. ${row.loc_time} - ${row.fuel_volume}L (${row.fuel_percentage}%) - ${date.toISOString()}`);
    });
  }

  // Close database
  pendingFuelDb.closeDatabase();
})();
