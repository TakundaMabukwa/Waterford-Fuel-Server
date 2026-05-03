CREATE TABLE IF NOT EXISTS public.energy_rite_realtime_messages (
  id BIGSERIAL PRIMARY KEY,
  plate TEXT NOT NULL,
  loc_time TEXT NOT NULL,
  message_time TIMESTAMPTZ NOT NULL,
  driver_name TEXT,
  speed DOUBLE PRECISION,
  fuel_probe_1_level DOUBLE PRECISION,
  fuel_probe_2_level DOUBLE PRECISION,
  fuel_probe_1_level_percentage DOUBLE PRECISION,
  fuel_probe_2_level_percentage DOUBLE PRECISION,
  fuel_probe_1_volume_in_tank DOUBLE PRECISION,
  fuel_probe_2_volume_in_tank DOUBLE PRECISION,
  combined_fuel_volume_in_tank DOUBLE PRECISION,
  combined_fuel_percentage DOUBLE PRECISION,
  raw_message JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_energy_rite_realtime_messages_plate_time
  ON public.energy_rite_realtime_messages (plate, message_time DESC);
