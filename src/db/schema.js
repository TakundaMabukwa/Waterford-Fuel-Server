const { query } = require('./index');

const createTables = async () => {
  await query(`
    CREATE TABLE IF NOT EXISTS vehicles (
      id SERIAL PRIMARY KEY,
      plate VARCHAR(50) UNIQUE NOT NULL,
      cost_code VARCHAR(50),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS vehicle_latest (
      plate VARCHAR(50) PRIMARY KEY,
      cost_code VARCHAR(50),
      speed DOUBLE PRECISION,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      loc_time TEXT,
      mileage BIGINT,
      pocsagstr TEXT,
      status TEXT,
      message_type INTEGER,
      fuel_probe_1_level DOUBLE PRECISION,
      fuel_probe_1_volume_in_tank DOUBLE PRECISION,
      fuel_probe_1_temperature DOUBLE PRECISION,
      fuel_probe_1_level_percentage DOUBLE PRECISION,
      fuel_probe_2_level DOUBLE PRECISION,
      fuel_probe_2_volume_in_tank DOUBLE PRECISION,
      fuel_probe_2_temperature DOUBLE PRECISION,
      fuel_probe_2_level_percentage DOUBLE PRECISION,
      item_installed TEXT,
      geozone TEXT,
      driver_name TEXT,
      raw_fuel_data TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS vehicle_history (
      id BIGSERIAL PRIMARY KEY,
      plate VARCHAR(50) NOT NULL,
      cost_code VARCHAR(50),
      speed DOUBLE PRECISION,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      loc_time TEXT,
      mileage BIGINT,
      pocsagstr TEXT,
      status TEXT,
      message_type INTEGER,
      fuel_probe_1_level DOUBLE PRECISION,
      fuel_probe_1_volume_in_tank DOUBLE PRECISION,
      fuel_probe_1_temperature DOUBLE PRECISION,
      fuel_probe_1_level_percentage DOUBLE PRECISION,
      fuel_probe_2_level DOUBLE PRECISION,
      fuel_probe_2_volume_in_tank DOUBLE PRECISION,
      fuel_probe_2_temperature DOUBLE PRECISION,
      fuel_probe_2_level_percentage DOUBLE PRECISION,
      item_installed TEXT,
      geozone TEXT,
      driver_name TEXT,
      raw_fuel_data TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_vehicle_history_plate_time
      ON vehicle_history (plate, created_at DESC)
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS energy_rite_operating_sessions (
      id BIGSERIAL PRIMARY KEY,
      branch VARCHAR(50),
      company VARCHAR(100),
      cost_code VARCHAR(50),
      session_date DATE,
      session_start_time TIMESTAMPTZ,
      session_end_time TIMESTAMPTZ,
      operating_hours DOUBLE PRECISION DEFAULT 0,
      opening_fuel DOUBLE PRECISION,
      opening_percentage DOUBLE PRECISION,
      opening_fuel_probe_1 DOUBLE PRECISION,
      opening_fuel_probe_2 DOUBLE PRECISION,
      opening_percentage_probe_1 DOUBLE PRECISION,
      opening_percentage_probe_2 DOUBLE PRECISION,
      closing_fuel DOUBLE PRECISION,
      closing_percentage DOUBLE PRECISION,
      closing_fuel_probe_1 DOUBLE PRECISION,
      closing_fuel_probe_2 DOUBLE PRECISION,
      closing_percentage_probe_1 DOUBLE PRECISION,
      closing_percentage_probe_2 DOUBLE PRECISION,
      total_fill DOUBLE PRECISION DEFAULT 0,
      total_theft DOUBLE PRECISION DEFAULT 0,
      total_usage DOUBLE PRECISION DEFAULT 0,
      session_status VARCHAR(50) DEFAULT 'ONGOING',
      notes TEXT,
      fill_events INTEGER DEFAULT 0,
      fill_amount_during_session DOUBLE PRECISION DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS fuel_stops (
      id BIGINT PRIMARY KEY,
      name TEXT,
      coordinates JSONB,
      geozone_name TEXT,
      geozone_coordinates JSONB,
      location_coordinates JSONB,
      radius NUMERIC(10,2) DEFAULT 100,
      type TEXT DEFAULT 'warehouse',
      address TEXT,
      city TEXT,
      state TEXT,
      country TEXT,
      contact_person TEXT,
      contact_phone TEXT,
      operating_hours TEXT,
      capacity TEXT,
      notes TEXT,
      prescribed_value NUMERIC,
      fuel_type TEXT,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS fuel_type TEXT
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS geozone_events (
      id BIGSERIAL PRIMARY KEY,
      plate VARCHAR(50) NOT NULL,
      fuel_stop_id BIGINT REFERENCES fuel_stops(id),
      geozone_name TEXT,
      event_type VARCHAR(20) NOT NULL,
      loc_time TEXT,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      fuel_before DOUBLE PRECISION,
      fuel_after DOUBLE PRECISION,
      fill_amount DOUBLE PRECISION,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_geozone_events_plate
      ON geozone_events (plate, created_at DESC)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_geozone_events_type
      ON geozone_events (event_type)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_fuel_stops_name
      ON fuel_stops (name)
  `);

  // NEW: zones table - ALL fuel_stops (no type filter)
  await query(`
    CREATE TABLE IF NOT EXISTS zones (
      id VARCHAR(50) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      coordinates JSONB NOT NULL,
      geozone_name VARCHAR(255),
      type VARCHAR(50),
      source_type VARCHAR(50),
      location_lat DOUBLE PRECISION,
      location_lng DOUBLE PRECISION,
      radius INTEGER DEFAULT 100,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_zones_name ON zones(name)
  `);

  // NEW: trips table - active trips only
  await query(`
    CREATE TABLE IF NOT EXISTS trips (
      trip_id VARCHAR(50) PRIMARY KEY,
      vehicle_reg VARCHAR(50) NOT NULL,
      status VARCHAR(50) NOT NULL,
      selected_stop_points JSONB NOT NULL,
      driver_info JSONB,
      trailer_info JSONB,
      origin VARCHAR(255),
      destination VARCHAR(255),
      created_at TIMESTAMPTZ DEFAULT NOW(),
      synced_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_trips_vehicle ON trips(vehicle_reg)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status)
  `);

  // NEW: trip_zone_events - ENTER/EXIT per trip per zone
  await query(`
    CREATE TABLE IF NOT EXISTS trip_zone_events (
      id BIGSERIAL PRIMARY KEY,
      trip_id VARCHAR(50) REFERENCES trips(trip_id),
      plate VARCHAR(50) NOT NULL,
      zone_id VARCHAR(50) REFERENCES zones(id),
      zone_name VARCHAR(255) NOT NULL,
      event_type VARCHAR(10) NOT NULL,
      loc_time TEXT NOT NULL,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      sequence_order INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_trip_zone_events_trip ON trip_zone_events(trip_id)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_trip_zone_events_plate ON trip_zone_events(plate)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_trip_zone_events_zone ON trip_zone_events(zone_id)
  `);

  console.log('[db] Tables ready');
};

module.exports = { createTables };