const { Pool } = require('pg');
const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;

const pool = new Pool({
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'fuel_table',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD,
});

const PLATE = process.argv[2] || 'MZ08FNGP';
const START_DATE = process.argv[3] || '2026-09-17';
const END_DATE = process.argv[4] || START_DATE;

async function run() {
  const { rows: msgs } = await pool.query(`
    SELECT plate, loc_time, latitude, longitude, fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank, status
    FROM vehicle_history
    WHERE plate = $1 AND loc_time >= $2 AND loc_time <= $3 || ' 23:59:59'
    ORDER BY loc_time::timestamptz ASC
  `, [PLATE, START_DATE, END_DATE]);

  console.log(`=== ${PLATE} messages ${START_DATE} to ${END_DATE} ===`);
  console.log('Total:', msgs.length);
  if (msgs.length === 0) { await pool.end(); return; }

  msgs.slice(0, 5).forEach(r => console.log(r.loc_time, 'lat=' + r.latitude, 'lon=' + r.longitude, 'p1=' + r.fuel_probe_1_volume_in_tank, 'p2=' + r.fuel_probe_2_volume_in_tank));
  if (msgs.length > 10) console.log('...');
  msgs.slice(-5).forEach(r => console.log(r.loc_time, 'lat=' + r.latitude, 'lon=' + r.longitude, 'p1=' + r.fuel_probe_1_volume_in_tank, 'p2=' + r.fuel_probe_2_volume_in_tank));

  const { rows: stops } = await pool.query('SELECT id, name, coordinates FROM fuel_stops WHERE coordinates IS NOT NULL');
  console.log('\nChecking against', stops.length, 'fuel stops...');

  let zoneHits = [];
  for (const msg of msgs) {
    if (!msg.latitude || !msg.longitude || msg.latitude == 0 || msg.longitude == 0) continue;
    for (const stop of stops) {
      let polygon = stop.coordinates;
      if (typeof polygon === 'string') polygon = JSON.parse(polygon);
      if (!Array.isArray(polygon) || polygon.length < 3) continue;
      const closedRing = [...polygon, polygon[0]];
      const turfPolygon = { type: 'Feature', geometry: { type: 'Polygon', coordinates: [closedRing] }, properties: {} };
      if (booleanPointInPolygon([msg.longitude, msg.latitude], turfPolygon)) {
        zoneHits.push({ time: msg.loc_time, zone: stop.name, lat: msg.latitude, lon: msg.longitude, p1: msg.fuel_probe_1_volume_in_tank, p2: msg.fuel_probe_2_volume_in_tank });
      }
    }
  }

  if (zoneHits.length === 0) {
    console.log('\nNO ZONE ENTRIES - coordinates do not fall inside any fuel stop polygon');
    console.log('Sample coordinates:');
    const unique = [...new Set(msgs.filter(m => m.latitude && m.latitude != 0).map(m => m.latitude + ',' + m.longitude))];
    unique.slice(0, 10).forEach(c => console.log('  ' + c));
  } else {
    console.log('\nZONE HITS:');
    zoneHits.forEach(h => console.log(h.time, h.zone, 'lat=' + h.lat, 'lon=' + h.lon, 'p1=' + h.p1, 'p2=' + h.p2));
  }

  await pool.end();
  process.exit(0);
}

run().catch(err => { console.error('ERROR:', err.message); process.exit(1); });
