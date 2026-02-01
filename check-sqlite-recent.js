const pendingFuelDb = require('./pending-fuel-db');

(async () => {
  console.log('🔍 Checking SQLite for recent fuel data...\n');

  await pendingFuelDb.initDatabase();
  const db = pendingFuelDb.getDb();

  // Get last 20 fuel entries from ANY vehicle
  const stmt = db.prepare(`
    SELECT plate, fuel_volume, fuel_percentage, loc_time, timestamp 
    FROM fuel_history 
    ORDER BY timestamp DESC 
    LIMIT 20
  `);
  
  const rows = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();

  if (rows.length === 0) {
    console.log('❌ No fuel data found in SQLite at all!');
  } else {
    console.log(`✅ Found ${rows.length} recent fuel entries:\n`);
    rows.forEach((row, idx) => {
      const date = new Date(row.timestamp);
      console.log(`${idx + 1}. ${row.plate} - ${row.loc_time} - ${row.fuel_volume}L (${row.fuel_percentage}%)`);
    });
  }

  pendingFuelDb.closeDatabase();
})();
