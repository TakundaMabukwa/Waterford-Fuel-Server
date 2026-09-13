// Historical replay tool — feeds real vehicle data through fill detection logic.
// Run on droplet: node replay-fills.js [plate] [start] [end]
// Examples:
//   node replay-fills.js KP48NCGP 2026-09-10 2026-09-13
//   node replay-fills.js 2026-09-01 2026-09-30  (all vehicles, date range)
//   node replay-fills.js (all vehicles, Sep 1-30)

const { Pool } = require('pg');
const { createClient } = require('@supabase/supabase-js');
const booleanPointInPolygon = require('@turf/boolean-point-in-polygon').default;

const pool = new Pool({
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'fuel_table',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD,
});

const WATERFORD_URL = process.env.WATERFORD_SUPABASE_URL;
const WATERFORD_KEY = process.env.WATERFORD_SUPABASE_SERVICE_ROLE_KEY || process.env.WATERFORD_SUPABASE_ANON_KEY;
const waterfordSupabase = (WATERFORD_URL && WATERFORD_KEY)
  ? createClient(WATERFORD_URL, WATERFORD_KEY, { auth: { persistSession: false } })
  : null;

const DRY_RUN = process.argv.includes('--dry-run');
const VERBOSE = process.argv.includes('--verbose') || process.argv.includes('-v');

const isDate = (s) => /^\d{4}-\d{2}-\d{2}/.test(s);
let TARGET_PLATE = null;
let START_DATE = '2026-09-01';
let END_DATE = '2026-09-30';

const rawArgs = process.argv.slice(2).filter(a => !a.startsWith('-'));
if (rawArgs.length === 0) {
  // all vehicles, default dates
} else if (rawArgs.length === 1) {
  if (isDate(rawArgs[0])) { START_DATE = rawArgs[0]; }
  else { TARGET_PLATE = rawArgs[0]; }
} else if (rawArgs.length === 2) {
  if (isDate(rawArgs[0]) && isDate(rawArgs[1])) {
    START_DATE = rawArgs[0]; END_DATE = rawArgs[1];
  } else if (isDate(rawArgs[1])) {
    TARGET_PLATE = rawArgs[0]; START_DATE = rawArgs[1];
  } else {
    TARGET_PLATE = rawArgs[0]; END_DATE = rawArgs[1];
  }
} else {
  TARGET_PLATE = rawArgs[0]; START_DATE = rawArgs[1]; END_DATE = rawArgs[2];
}
const MIN_FILL = 10;

const insertFuelReviewAction = async (plate, amount, locTime, zoneName, preFill, postFill) => {
  if (!waterfordSupabase) return;
  if (DRY_RUN) {
    console.log(`  [DRY RUN] Would insert: ${plate} | ${locTime.split('T')[0]} | fill | ${amount.toFixed(1)}L`);
    return;
  }
  try {
    const reviewDate = locTime ? locTime.split('T')[0] : new Date().toISOString().split('T')[0];
    const { error } = await waterfordSupabase
      .from('fuel_review_actions')
      .upsert({
        vehicle_reg: plate,
        review_date: reviewDate,
        action_type: 'fill',
        probe_value: `${amount.toFixed(1)}L`,
        notes: `loc_time: ${locTime} | zone: ${zoneName} | pre: ${preFill}L | post: ${postFill}L | detection: replay-backfill`,
      }, { onConflict: 'vehicle_reg,review_date,action_type' });
    if (error) throw error;
    console.log(`  INSERTED: ${plate} on ${reviewDate} - ${amount.toFixed(1)}L`);
  } catch (err) {
    console.error(`  FAILED to insert ${plate}: ${err.message}`);
  }
};

const combinedFuel = (row) =>
  (row.fuel_probe_1_volume_in_tank || 0) + (row.fuel_probe_2_volume_in_tank || 0);

const locTimeToISO = (t) => {
  if (!t) return null;
  if (t.includes('T')) return t;
  return t + '+00:00';
};

function pointInPolygon(lat, lon, polygon) {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i][0], yi = polygon[i][1];
    const xj = polygon[j][0], yj = polygon[j][1];
    if ((yi > lat) !== (yj > lat) && lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

function findFuelStop(lat, lon, fuelStops) {
  for (const stop of fuelStops) {
    let polygon = stop.coordinates;
    if (typeof polygon === 'string') {
      try { polygon = JSON.parse(polygon); } catch { continue; }
    }
    if (!Array.isArray(polygon) || polygon.length < 3) continue;
    const closedRing = [...polygon, polygon[0]];
    const turfPolygon = {
      type: 'Feature',
      geometry: { type: 'Polygon', coordinates: [closedRing] },
      properties: {}
    };
    if (booleanPointInPolygon([lon, lat], turfPolygon)) return stop;
  }
  return null;
}

async function replayVehicle(plate, fuelStops) {
  let where = `plate = $1`;
  const params = [plate];

  if (START_DATE) {
    where += ` AND loc_time >= $2 AND loc_time <= $3`;
    params.push(START_DATE + ' 00:00:00', END_DATE + ' 23:59:59');
  }

  const { rows: messages } = await pool.query(`
    SELECT id, plate, loc_time, latitude, longitude,
      fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank,
      status, message_type
    FROM vehicle_history
    WHERE ${where}
      AND latitude IS NOT NULL AND longitude IS NOT NULL
      AND latitude != 0 AND longitude != 0
    ORDER BY created_at ASC
  `, params);

  if (messages.length === 0) return null;

  let tracking = null;
  const results = { plate, totalMessages: messages.length, fills: [], enters: 0, exits: 0 };

  for (const msg of messages) {
    const locTime = locTimeToISO(msg.loc_time);
    const fuel = combinedFuel(msg);
    const fuelStop = findFuelStop(msg.latitude, msg.longitude, fuelStops);
    const isInZone = fuelStop !== null;
    const wasInZone = tracking !== null;

    // Zone entry
    if (!wasInZone && isInZone) {
      let preFill = fuel > 0 ? fuel : null;
      let preFillLocTime = fuel > 0 ? locTime : null;

      // DB fallback: last fuel reading before entry
      if (!preFill) {
        const { rows: fb } = await pool.query(`
          SELECT fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank, loc_time
          FROM vehicle_history
          WHERE plate = $1 AND loc_time::timestamptz < $2::timestamptz
            AND (fuel_probe_1_volume_in_tank > 0 OR fuel_probe_2_volume_in_tank > 0)
          ORDER BY loc_time::timestamptz DESC LIMIT 1
        `, [plate, locTime]);
        if (fb.length > 0) {
          preFill = combinedFuel(fb[0]);
          preFillLocTime = locTimeToISO(fb[0].loc_time);
        }
      }

      tracking = {
        fuelStopId: fuelStop.id,
        zoneName: fuelStop.name || fuelStop.geozone_name || 'Unknown',
        zoneEnterTime: locTime,
        preFill,
        preFillLocTime,
        exitLatitude: null,
        exitLongitude: null,
      };
      results.enters++;
      if (VERBOSE) console.log(`  ENTER: ${plate} into "${tracking.zoneName}" at ${msg.loc_time} fuel=${fuel}L preFill=${preFill}L`);
      continue;
    }

    // Zone exit
    if (wasInZone && !isInZone) {
      let postFill = fuel > 0 ? fuel : null;
      let postFillLocTime = fuel > 0 ? locTime : null;

      // DB fallback: last fuel reading before exit
      if (!postFill) {
        const { rows: fallback } = await pool.query(`
          SELECT fuel_probe_1_volume_in_tank, fuel_probe_2_volume_in_tank, loc_time
          FROM vehicle_history
          WHERE plate = $1 AND loc_time::timestamptz < $2::timestamptz
            AND (fuel_probe_1_volume_in_tank > 0 OR fuel_probe_2_volume_in_tank > 0)
          ORDER BY loc_time::timestamptz DESC LIMIT 1
        `, [plate, locTime]);
        if (fallback.length > 0) {
          postFill = combinedFuel(fallback[0]);
          postFillLocTime = locTimeToISO(fallback[0].loc_time);
        }
      }

      results.exits++;

      if (tracking.preFill !== null && postFill !== null) {
        const fill = postFill - tracking.preFill;

        if (fill >= MIN_FILL) {
          const entry = {
            zone: tracking.zoneName,
            zoneId: tracking.fuelStopId,
            enterTime: tracking.zoneEnterTime,
            exitTime: locTime,
            preFill: tracking.preFill,
            postFill,
            fill: parseFloat(fill.toFixed(1)),
          };
          results.fills.push(entry);
          console.log(`  FILL: ${plate} at "${tracking.zoneName}" - ${tracking.preFill}L -> ${postFill}L = ${fill.toFixed(1)}L`);
          await insertFuelReviewAction(plate, fill, tracking.postFillLocTime || locTime, tracking.zoneName, tracking.preFill, postFill);
        } else if (VERBOSE) {
          if (fill > 0) {
            console.log(`  SKIP: ${plate} at "${tracking.zoneName}" - ${tracking.preFill}L -> ${postFill}L = ${fill.toFixed(1)}L (below ${MIN_FILL}L)`);
          } else {
            console.log(`  SKIP: ${plate} at "${tracking.zoneName}" - ${tracking.preFill}L -> ${postFill}L = ${fill.toFixed(1)}L (no fill)`);
          }
        }
      } else if (VERBOSE) {
        console.log(`  SKIP: ${plate} at "${tracking.zoneName}" - pre=${tracking.preFill} post=${postFill} (incomplete data)`);
      }

      tracking = null;
      continue;
    }

    // Inside zone — update preFill if still null
    if (wasInZone && isInZone && tracking.preFill === null && fuel > 0) {
      tracking.preFill = fuel;
      tracking.preFillLocTime = locTime;
      if (VERBOSE) console.log(`  PRE-FILL SET: ${plate} - ${fuel}L at ${msg.loc_time}`);
    }
  }

  return results;
}

async function run() {
  console.log('=== FILL DETECTION REPLAY ===');
  console.log(`Plate: ${TARGET_PLATE || 'ALL'} | Date: ${START_DATE} to ${END_DATE}`);
  console.log();

  // Load fuel stops
  const { rows: fuelStops } = await pool.query('SELECT * FROM fuel_stops WHERE coordinates IS NOT NULL');
  console.log(`Loaded ${fuelStops.length} fuel stops`);

  // Get plates to replay
  let plates;
  if (TARGET_PLATE) {
    plates = [TARGET_PLATE.toUpperCase()];
  } else {
    const { rows } = await pool.query(`
      SELECT DISTINCT plate FROM vehicle_history
      WHERE (fuel_probe_1_volume_in_tank > 0 OR fuel_probe_2_volume_in_tank > 0)
      ORDER BY plate
    `);
    plates = rows.map(r => r.plate);
  }
  console.log(`Replaying ${plates.length} vehicles\n`);

  let totalFills = 0;
  let totalEnters = 0;
  let totalExits = 0;
  const allResults = [];

  for (const plate of plates) {
    const result = await replayVehicle(plate, fuelStops);
    if (result) {
      allResults.push(result);
      totalFills += result.fills.length;
      totalEnters += result.enters;
      totalExits += result.exits;
      if (result.fills.length > 0 || VERBOSE) {
        console.log(`  => ${plate}: ${result.fills.length} fills (${result.totalMessages} msgs, ${result.enters} enters, ${result.exits} exits)`);
      }
    }
  }

  console.log('=== SUMMARY ===');
  console.log(`Vehicles: ${plates.length} | Enters: ${totalEnters} | Exits: ${totalExits} | Fills detected: ${totalFills}`);

  if (totalFills > 0) {
    console.log('\nDetected fills:');
    for (const r of allResults) {
      for (const f of r.fills) {
        console.log(`  ${r.plate} | ${f.zone} | ${f.enterTime} -> ${f.exitTime} | ${f.preFill}L -> ${f.postFill}L = ${f.fill}L`);
      }
    }
  }

  await pool.end();
  process.exit(0);
}

run().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
