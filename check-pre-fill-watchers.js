const pendingFuelDb = require('./pending-fuel-db');

(async () => {
  console.log('🔍 Checking pre_fill_watchers in SQLite...\n');

  await pendingFuelDb.initDatabase();
  const db = pendingFuelDb.getDb();

  // Count total records
  const countStmt = db.prepare('SELECT COUNT(*) as count FROM pre_fill_watchers');
  countStmt.step();
  const total = countStmt.getAsObject().count;
  countStmt.free();

  console.log(`📊 Total pre_fill_watchers: ${total}\n`);

  if (total > 0) {
    // Get all records
    const stmt = db.prepare(`
      SELECT plate, lowest_fuel, lowest_percentage, lowest_loc_time, last_update 
      FROM pre_fill_watchers 
      ORDER BY last_update DESC
    `);
    
    const rows = [];
    while (stmt.step()) {
      rows.push(stmt.getAsObject());
    }
    stmt.free();

    console.log('📋 Pre-fill watchers:\n');
    rows.forEach((row, idx) => {
      const date = new Date(row.last_update);
      const age = Math.floor((Date.now() - row.last_update) / 1000 / 60);
      console.log(`${idx + 1}. ${row.plate}`);
      console.log(`   Lowest: ${row.lowest_fuel}L (${row.lowest_percentage}%)`);
      console.log(`   Time: ${row.lowest_loc_time}`);
      console.log(`   Age: ${age} minutes ago\n`);
    });
  }

  pendingFuelDb.closeDatabase();
})();
