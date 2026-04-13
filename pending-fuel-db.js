/**
 * SQLite database for pending fuel states
 * This is used to persist watcher data locally to survive server restarts
 * without using Supabase API calls
 */

const initSqlJs = require('sql.js');
const fs = require('fs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'pending-fuel-states.db');

let db = null;
let SQL = null;

function getTableColumns(tableName) {
  if (!db) return new Set();

  const columns = new Set();
  const stmt = db.prepare(`PRAGMA table_info(${tableName})`);
  while (stmt.step()) {
    const row = stmt.getAsObject();
    columns.add(row.name);
  }
  stmt.free();
  return columns;
}

function ensureTableColumns(tableName, columnDefinitions) {
  if (!db) return;

  const existingColumns = getTableColumns(tableName);
  for (const [columnName, definition] of Object.entries(columnDefinitions)) {
    if (existingColumns.has(columnName)) continue;
    db.run(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
}

async function initDatabase() {
  if (db) return db;
  
  try {
    SQL = await initSqlJs();
    
    // Load existing database or create new one
    if (fs.existsSync(DB_PATH)) {
      const fileBuffer = fs.readFileSync(DB_PATH);
      db = new SQL.Database(fileBuffer);
      console.log('📂 Loaded existing pending fuel states database');
    } else {
      db = new SQL.Database();
      console.log('📂 Created new pending fuel states database');
    }
    
    // Create tables
    db.run(`
      CREATE TABLE IF NOT EXISTS pre_fill_watchers (
        plate TEXT PRIMARY KEY,
        lowest_fuel REAL,
        lowest_percentage REAL,
        lowest_fuel_probe_1 REAL,
        lowest_fuel_probe_2 REAL,
        lowest_percentage_probe_1 REAL,
        lowest_percentage_probe_2 REAL,
        lowest_loc_time TEXT,
        last_update INTEGER
      )
    `);
    
    db.run(`
      CREATE TABLE IF NOT EXISTS pending_fuel_fills (
        plate TEXT PRIMARY KEY,
        start_time TEXT,
        start_loc_time TEXT,
        opening_fuel REAL,
        opening_percentage REAL,
        opening_fuel_probe_1 REAL,
        opening_fuel_probe_2 REAL,
        opening_percentage_probe_1 REAL,
        opening_percentage_probe_2 REAL,
        waiting_for_opening_fuel INTEGER DEFAULT 0
      )
    `);
    
    db.run(`
      CREATE TABLE IF NOT EXISTS fuel_fill_watchers (
        plate TEXT PRIMARY KEY,
        start_time TEXT,
        start_loc_time TEXT,
        opening_fuel REAL,
        opening_percentage REAL,
        opening_fuel_probe_1 REAL,
        opening_fuel_probe_2 REAL,
        opening_percentage_probe_1 REAL,
        opening_percentage_probe_2 REAL,
        highest_fuel REAL,
        highest_percentage REAL,
        highest_fuel_probe_1 REAL,
        highest_fuel_probe_2 REAL,
        highest_percentage_probe_1 REAL,
        highest_percentage_probe_2 REAL,
        highest_loc_time TEXT,
        lowest_fuel REAL,
        lowest_percentage REAL,
        lowest_fuel_probe_1 REAL,
        lowest_fuel_probe_2 REAL,
        lowest_percentage_probe_1 REAL,
        lowest_percentage_probe_2 REAL,
        lowest_loc_time TEXT,
        watcher_type TEXT DEFAULT 'FILL',
        timeout_at INTEGER,
        last_increased_at INTEGER
      )
    `);
    
    // Fuel history table - stores recent readings for lookup
    db.run(`
      CREATE TABLE IF NOT EXISTS fuel_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        plate TEXT NOT NULL,
        fuel_volume REAL,
        fuel_percentage REAL,
        fuel_volume_probe_1 REAL,
        fuel_volume_probe_2 REAL,
        fuel_percentage_probe_1 REAL,
        fuel_percentage_probe_2 REAL,
        fuel_diff REAL DEFAULT 0,
        loc_time TEXT,
        timestamp INTEGER,
        UNIQUE(plate, loc_time)
      )
    `);
    
    // Create index for faster lookups
    db.run(`CREATE INDEX IF NOT EXISTS idx_fuel_history_plate_time ON fuel_history(plate, timestamp)`);

    // Migrate older local DB files in place. Existing SQLite tables are not
    // changed by CREATE TABLE IF NOT EXISTS, so we add any missing columns.
    ensureTableColumns('pre_fill_watchers', {
      lowest_fuel: 'REAL',
      lowest_percentage: 'REAL',
      lowest_fuel_probe_1: 'REAL',
      lowest_fuel_probe_2: 'REAL',
      lowest_percentage_probe_1: 'REAL',
      lowest_percentage_probe_2: 'REAL',
      lowest_loc_time: 'TEXT',
      last_update: 'INTEGER'
    });

    ensureTableColumns('pending_fuel_fills', {
      start_time: 'TEXT',
      start_loc_time: 'TEXT',
      opening_fuel: 'REAL',
      opening_percentage: 'REAL',
      opening_fuel_probe_1: 'REAL',
      opening_fuel_probe_2: 'REAL',
      opening_percentage_probe_1: 'REAL',
      opening_percentage_probe_2: 'REAL',
      waiting_for_opening_fuel: 'INTEGER DEFAULT 0'
    });

    ensureTableColumns('fuel_fill_watchers', {
      start_time: 'TEXT',
      start_loc_time: 'TEXT',
      opening_fuel: 'REAL',
      opening_percentage: 'REAL',
      opening_fuel_probe_1: 'REAL',
      opening_fuel_probe_2: 'REAL',
      opening_percentage_probe_1: 'REAL',
      opening_percentage_probe_2: 'REAL',
      highest_fuel: 'REAL',
      highest_percentage: 'REAL',
      highest_fuel_probe_1: 'REAL',
      highest_fuel_probe_2: 'REAL',
      highest_percentage_probe_1: 'REAL',
      highest_percentage_probe_2: 'REAL',
      highest_loc_time: 'TEXT',
      lowest_fuel: 'REAL',
      lowest_percentage: 'REAL',
      lowest_fuel_probe_1: 'REAL',
      lowest_fuel_probe_2: 'REAL',
      lowest_percentage_probe_1: 'REAL',
      lowest_percentage_probe_2: 'REAL',
      lowest_loc_time: 'TEXT',
      watcher_type: "TEXT DEFAULT 'FILL'",
      timeout_at: 'INTEGER',
      last_increased_at: 'INTEGER'
    });

    ensureTableColumns('fuel_history', {
      fuel_volume: 'REAL',
      fuel_percentage: 'REAL',
      fuel_volume_probe_1: 'REAL',
      fuel_volume_probe_2: 'REAL',
      fuel_percentage_probe_1: 'REAL',
      fuel_percentage_probe_2: 'REAL',
      fuel_diff: 'REAL DEFAULT 0',
      loc_time: 'TEXT',
      timestamp: 'INTEGER'
    });
    
    saveDatabase();
    console.log('✅ Pending fuel states database initialized');
    
    return db;
  } catch (error) {
    console.error('❌ Error initializing SQLite database:', error.message);
    throw error;
  }
}

function saveDatabase() {
  if (!db) return;
  try {
    const data = db.export();
    const buffer = Buffer.from(data);
    fs.writeFileSync(DB_PATH, buffer);
  } catch (error) {
    console.error('❌ Error saving database:', error.message);
  }
}

// ==================== PRE-FILL WATCHERS ====================

function getPreFillWatcher(plate) {
  if (!db) return null;
  const stmt = db.prepare('SELECT * FROM pre_fill_watchers WHERE plate = ?');
  stmt.bind([plate]);
  if (stmt.step()) {
    const row = stmt.getAsObject();
    stmt.free();
    return row;
  }
  stmt.free();
  return null;
}

function setPreFillWatcher(plate, data) {
  if (!db) return;
  db.run(`
    INSERT OR REPLACE INTO pre_fill_watchers 
    (plate, lowest_fuel, lowest_percentage, lowest_fuel_probe_1, lowest_fuel_probe_2,
     lowest_percentage_probe_1, lowest_percentage_probe_2, lowest_loc_time, last_update)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    plate,
    data.lowestFuel,
    data.lowestPercentage,
    data.lowestFuelProbe1 ?? null,
    data.lowestFuelProbe2 ?? null,
    data.lowestPercentageProbe1 ?? null,
    data.lowestPercentageProbe2 ?? null,
    data.lowestLocTime,
    Date.now()
  ]);
  saveDatabase();
}

function deletePreFillWatcher(plate) {
  if (!db) return;
  db.run('DELETE FROM pre_fill_watchers WHERE plate = ?', [plate]);
  saveDatabase();
}

function cleanupOldPreFillWatchers(maxAgeMs = 30 * 60 * 1000) {
  if (!db) return;
  const cutoff = Date.now() - maxAgeMs;
  db.run('DELETE FROM pre_fill_watchers WHERE last_update < ?', [cutoff]);
  saveDatabase();
}

// ==================== PENDING FUEL FILLS ====================

function getPendingFuelFill(plate) {
  if (!db) return null;
  const stmt = db.prepare('SELECT * FROM pending_fuel_fills WHERE plate = ?');
  stmt.bind([plate]);
  if (stmt.step()) {
    const row = stmt.getAsObject();
    stmt.free();
    // Convert integer to boolean
    row.waitingForOpeningFuel = row.waiting_for_opening_fuel === 1;
    return row;
  }
  stmt.free();
  return null;
}

function setPendingFuelFill(plate, data) {
  if (!db) return;
  db.run(`
    INSERT OR REPLACE INTO pending_fuel_fills 
    (plate, start_time, start_loc_time, opening_fuel, opening_percentage,
     opening_fuel_probe_1, opening_fuel_probe_2, opening_percentage_probe_1, opening_percentage_probe_2,
     waiting_for_opening_fuel)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    plate, 
    data.startTime, 
    data.startLocTime, 
    data.openingFuel || null, 
    data.openingPercentage || null,
    data.openingFuelProbe1 ?? null,
    data.openingFuelProbe2 ?? null,
    data.openingPercentageProbe1 ?? null,
    data.openingPercentageProbe2 ?? null,
    data.waitingForOpeningFuel ? 1 : 0
  ]);
  saveDatabase();
}

function updatePendingFuelFillOpening(plate, openingFuel, openingPercentage, openingFuelProbe1 = null, openingFuelProbe2 = null, openingPercentageProbe1 = null, openingPercentageProbe2 = null) {
  if (!db) return;
  db.run(`
    UPDATE pending_fuel_fills 
    SET opening_fuel = ?, opening_percentage = ?, opening_fuel_probe_1 = ?, opening_fuel_probe_2 = ?,
        opening_percentage_probe_1 = ?, opening_percentage_probe_2 = ?, waiting_for_opening_fuel = 0
    WHERE plate = ?
  `, [openingFuel, openingPercentage, openingFuelProbe1, openingFuelProbe2, openingPercentageProbe1, openingPercentageProbe2, plate]);
  saveDatabase();
}

function deletePendingFuelFill(plate) {
  if (!db) return;
  db.run('DELETE FROM pending_fuel_fills WHERE plate = ?', [plate]);
  saveDatabase();
}

// ==================== FUEL FILL WATCHERS ====================

function getFuelFillWatcher(plate) {
  if (!db) return null;
  const stmt = db.prepare('SELECT * FROM fuel_fill_watchers WHERE plate = ?');
  stmt.bind([plate]);
  if (stmt.step()) {
    const row = stmt.getAsObject();
    stmt.free();
    return row;
  }
  stmt.free();
  return null;
}

function setFuelFillWatcher(plate, data) {
  if (!db) return;
  const now = Date.now();
  db.run(`
    INSERT OR REPLACE INTO fuel_fill_watchers 
    (plate, start_time, start_loc_time, opening_fuel, opening_percentage,
     opening_fuel_probe_1, opening_fuel_probe_2, opening_percentage_probe_1, opening_percentage_probe_2,
     highest_fuel, highest_percentage, highest_fuel_probe_1, highest_fuel_probe_2, highest_percentage_probe_1, highest_percentage_probe_2, highest_loc_time,
     lowest_fuel, lowest_percentage, lowest_fuel_probe_1, lowest_fuel_probe_2, lowest_percentage_probe_1, lowest_percentage_probe_2, lowest_loc_time,
     watcher_type, timeout_at, last_increased_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    plate,
    data.startTime,
    data.startLocTime,
    data.openingFuel,
    data.openingPercentage,
    data.openingFuelProbe1 ?? null,
    data.openingFuelProbe2 ?? null,
    data.openingPercentageProbe1 ?? null,
    data.openingPercentageProbe2 ?? null,
    data.highestFuel || null,
    data.highestPercentage || null,
    data.highestFuelProbe1 ?? null,
    data.highestFuelProbe2 ?? null,
    data.highestPercentageProbe1 ?? null,
    data.highestPercentageProbe2 ?? null,
    data.highestLocTime || null,
    data.lowestFuel || null,
    data.lowestPercentage || null,
    data.lowestFuelProbe1 ?? null,
    data.lowestFuelProbe2 ?? null,
    data.lowestPercentageProbe1 ?? null,
    data.lowestPercentageProbe2 ?? null,
    data.lowestLocTime || null,
    data.watcherType || 'FILL',
    now + (10 * 60 * 1000), // Max 10 minutes timeout
    now // Last changed at creation time
  ]);
  saveDatabase();
}

function updateFuelFillWatcherHighest(plate, highestFuel, highestPercentage, highestLocTime, highestFuelProbe1 = null, highestFuelProbe2 = null, highestPercentageProbe1 = null, highestPercentageProbe2 = null) {
  if (!db) return;
  db.run(`
    UPDATE fuel_fill_watchers 
    SET highest_fuel = ?, highest_percentage = ?, highest_fuel_probe_1 = ?, highest_fuel_probe_2 = ?,
        highest_percentage_probe_1 = ?, highest_percentage_probe_2 = ?, highest_loc_time = ?, last_increased_at = ?
    WHERE plate = ?
  `, [highestFuel, highestPercentage, highestFuelProbe1, highestFuelProbe2, highestPercentageProbe1, highestPercentageProbe2, highestLocTime, Date.now(), plate]);
  saveDatabase();
}

function updateFuelFillWatcherLowest(plate, lowestFuel, lowestPercentage, lowestLocTime, lowestFuelProbe1 = null, lowestFuelProbe2 = null, lowestPercentageProbe1 = null, lowestPercentageProbe2 = null) {
  if (!db) return;
  db.run(`
    UPDATE fuel_fill_watchers 
    SET lowest_fuel = ?, lowest_percentage = ?, lowest_fuel_probe_1 = ?, lowest_fuel_probe_2 = ?,
        lowest_percentage_probe_1 = ?, lowest_percentage_probe_2 = ?, lowest_loc_time = ?, last_increased_at = ?
    WHERE plate = ?
  `, [lowestFuel, lowestPercentage, lowestFuelProbe1, lowestFuelProbe2, lowestPercentageProbe1, lowestPercentageProbe2, lowestLocTime, Date.now(), plate]);
  saveDatabase();
}

// Get watchers where fuel hasn't increased for specified duration (fill completed)
function getStabilizedFuelFillWatchers(stableMs = 2 * 60 * 1000) {
  if (!db) return [];
  const results = [];
  const cutoff = Date.now() - stableMs;
  const stmt = db.prepare('SELECT * FROM fuel_fill_watchers WHERE last_increased_at <= ?');
  stmt.bind([cutoff]);
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

function deleteFuelFillWatcher(plate) {
  if (!db) return;
  db.run('DELETE FROM fuel_fill_watchers WHERE plate = ?', [plate]);
  saveDatabase();
}

function getAllFuelFillWatchers() {
  if (!db) return [];
  const results = [];
  const stmt = db.prepare('SELECT * FROM fuel_fill_watchers');
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

function getExpiredFuelFillWatchers() {
  if (!db) return [];
  const results = [];
  const stmt = db.prepare('SELECT * FROM fuel_fill_watchers WHERE timeout_at <= ?');
  stmt.bind([Date.now()]);
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

// ==================== FUEL HISTORY ====================

function storeFuelHistory(plate, fuelVolume, fuelPercentage, locTime, timestamp) {
  if (!db) return;
  try {
    const snapshot = typeof fuelVolume === 'object' && fuelVolume !== null
      ? fuelVolume
      : {
          combinedFuelVolume: fuelVolume,
          combinedFuelPercentage: fuelPercentage
        };

    const combinedFuelVolume = snapshot.combinedFuelVolume ?? snapshot.fuelVolume ?? 0;
    const combinedFuelPercentage = snapshot.combinedFuelPercentage ?? snapshot.fuelPercentage ?? 0;
    const probe1Volume = snapshot.fuelVolumeProbe1 ?? null;
    const probe2Volume = snapshot.fuelVolumeProbe2 ?? null;
    const probe1Percentage = snapshot.fuelPercentageProbe1 ?? null;
    const probe2Percentage = snapshot.fuelPercentageProbe2 ?? null;

    // Get previous reading to calculate diff
    const prevStmt = db.prepare(`
      SELECT fuel_volume FROM fuel_history 
      WHERE plate = ? 
      ORDER BY timestamp DESC 
      LIMIT 1
    `);
    prevStmt.bind([plate]);
    let fuelDiff = 0;
    if (prevStmt.step()) {
      const prev = prevStmt.getAsObject();
      fuelDiff = combinedFuelVolume - prev.fuel_volume;
    }
    prevStmt.free();
    
    db.run(`
      INSERT OR REPLACE INTO fuel_history 
      (plate, fuel_volume, fuel_percentage, fuel_volume_probe_1, fuel_volume_probe_2,
       fuel_percentage_probe_1, fuel_percentage_probe_2, fuel_diff, loc_time, timestamp)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [plate, combinedFuelVolume, combinedFuelPercentage, probe1Volume, probe2Volume, probe1Percentage, probe2Percentage, fuelDiff, locTime, timestamp]);
    
    // Keep only last 100 entries per plate to prevent bloat
    db.run(`
      DELETE FROM fuel_history WHERE plate = ? AND id NOT IN (
        SELECT id FROM fuel_history WHERE plate = ? ORDER BY timestamp DESC LIMIT 100
      )
    `, [plate, plate]);
    
    saveDatabase();
  } catch (error) {
    // Ignore duplicate key errors
  }
}

function getFuelHistoryBefore(plate, targetTimestamp, limit = 10) {
  if (!db) return [];
  const results = [];
  const stmt = db.prepare(`
    SELECT * FROM fuel_history 
    WHERE plate = ? AND timestamp < ? 
    ORDER BY timestamp DESC 
    LIMIT ?
  `);
  stmt.bind([plate, targetTimestamp, limit]);
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

function getFuelHistoryAfter(plate, targetTimestamp, limit = 10) {
  if (!db) return [];
  const results = [];
  const stmt = db.prepare(`
    SELECT * FROM fuel_history 
    WHERE plate = ? AND timestamp > ? 
    ORDER BY timestamp ASC 
    LIMIT ?
  `);
  stmt.bind([plate, targetTimestamp, limit]);
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

function getFuelHistoryInRange(plate, startTimestamp, endTimestamp) {
  if (!db) return [];
  const results = [];
  const stmt = db.prepare(`
    SELECT * FROM fuel_history 
    WHERE plate = ? AND timestamp >= ? AND timestamp <= ? 
    ORDER BY timestamp ASC
  `);
  stmt.bind([plate, startTimestamp, endTimestamp]);
  while (stmt.step()) {
    results.push(stmt.getAsObject());
  }
  stmt.free();
  return results;
}

function getLowestFuelInRange(plate, startTimestamp, endTimestamp) {
  if (!db) return null;
  const stmt = db.prepare(`
    SELECT * FROM fuel_history 
    WHERE plate = ? AND timestamp >= ? AND timestamp <= ? 
    ORDER BY fuel_volume ASC 
    LIMIT 1
  `);
  stmt.bind([plate, startTimestamp, endTimestamp]);
  if (stmt.step()) {
    const row = stmt.getAsObject();
    stmt.free();
    return row;
  }
  stmt.free();
  return null;
}

function cleanupOldFuelHistory(maxAgeMs = 60 * 60 * 1000) {
  if (!db) return;
  const cutoff = Date.now() - maxAgeMs;
  db.run('DELETE FROM fuel_history WHERE timestamp < ?', [cutoff]);
  saveDatabase();
}

// ==================== UTILITIES ====================

function getAllPendingStates() {
  if (!db) return { preFill: [], pendingFills: [], watchers: [] };
  
  const preFill = [];
  const pendingFills = [];
  const watchers = [];
  
  let stmt = db.prepare('SELECT * FROM pre_fill_watchers');
  while (stmt.step()) preFill.push(stmt.getAsObject());
  stmt.free();
  
  stmt = db.prepare('SELECT * FROM pending_fuel_fills');
  while (stmt.step()) pendingFills.push(stmt.getAsObject());
  stmt.free();
  
  stmt = db.prepare('SELECT * FROM fuel_fill_watchers');
  while (stmt.step()) watchers.push(stmt.getAsObject());
  stmt.free();
  
  return { preFill, pendingFills, watchers };
}

function clearAllPendingStates() {
  if (!db) return;
  db.run('DELETE FROM pre_fill_watchers');
  db.run('DELETE FROM pending_fuel_fills');
  db.run('DELETE FROM fuel_fill_watchers');
  saveDatabase();
  console.log('🗑️ Cleared all pending fuel states');
}

function closeDatabase() {
  if (db) {
    saveDatabase();
    db.close();
    db = null;
    console.log('📂 Closed pending fuel states database');
  }
}

// For testing purposes - get direct database reference
function getDb() {
  return db;
}

module.exports = {
  initDatabase,
  saveDatabase,
  closeDatabase,
  getDb,
  
  // Pre-fill watchers
  getPreFillWatcher,
  setPreFillWatcher,
  deletePreFillWatcher,
  cleanupOldPreFillWatchers,
  
  // Pending fuel fills
  getPendingFuelFill,
  setPendingFuelFill,
  updatePendingFuelFillOpening,
  deletePendingFuelFill,
  
  // Fuel fill watchers
  getFuelFillWatcher,
  setFuelFillWatcher,
  updateFuelFillWatcherHighest,
  updateFuelFillWatcherLowest,
  deleteFuelFillWatcher,
  getAllFuelFillWatchers,
  getExpiredFuelFillWatchers,
  getStabilizedFuelFillWatchers,
  
  // Fuel history
  storeFuelHistory,
  getFuelHistoryBefore,
  getFuelHistoryAfter,
  getFuelHistoryInRange,
  getLowestFuelInRange,
  cleanupOldFuelHistory,
  
  // Utilities
  getAllPendingStates,
  clearAllPendingStates
};
