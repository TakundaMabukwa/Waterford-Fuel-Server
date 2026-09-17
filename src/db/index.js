const { Pool } = require('pg');

let pool = null;

const createPool = () => {
  const p = new Pool({
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432', 10),
    database: process.env.PGDATABASE || 'fuel_table',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD
  });
  p.on('error', (err) => console.error('[db] Pool error:', err.message));
  return p;
};

const getPool = () => {
  if (!pool) pool = createPool();
  return pool;
};

const query = (text, values = []) => getPool().query(text, values);

const waitForDatabase = async (retries = 30, delayMs = 2000) => {
  for (let i = 1; i <= retries; i++) {
    try {
      await query('SELECT 1');
      console.log('[db] PostgreSQL connected');
      return;
    } catch {
      console.log(`[db] Waiting for PostgreSQL... ${i}/${retries}`);
      if (i === retries) throw new Error('PostgreSQL connection timeout');
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
};

const close = async () => {
  if (pool) {
    await pool.end();
    pool = null;
  }
};

module.exports = { query, waitForDatabase, close, getPool };