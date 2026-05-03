#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo bash scripts/setup-droplet-postgres.sh
#
# Optional overrides:
#   POSTGRES_DB=fuel
#   POSTGRES_USER=postgres
#   POSTGRES_PASSWORD='SRS@981234#*'
#   POSTGRES_MESSAGE_TABLE=energy_rite_realtime_messages

POSTGRES_DB="${POSTGRES_DB:-fuel}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-SRS@981234#*}"
POSTGRES_MESSAGE_TABLE="${POSTGRES_MESSAGE_TABLE:-energy_rite_realtime_messages}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash $0"
  exit 1
fi

if [[ ! "${POSTGRES_MESSAGE_TABLE}" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
  echo "Invalid POSTGRES_MESSAGE_TABLE: ${POSTGRES_MESSAGE_TABLE}"
  exit 1
fi

echo "Installing PostgreSQL..."
apt-get update -y
apt-get install -y postgresql postgresql-contrib

echo "Starting PostgreSQL service..."
systemctl enable postgresql
systemctl restart postgresql

echo "Configuring postgres user password..."
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
ALTER USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}';
SQL

echo "Creating database (${POSTGRES_DB}) if missing..."
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
SELECT 'CREATE DATABASE ${POSTGRES_DB}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_DB}')\\gexec
SQL

echo "Creating realtime message table (${POSTGRES_MESSAGE_TABLE})..."
sudo -u postgres psql -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE IF NOT EXISTS public.${POSTGRES_MESSAGE_TABLE} (
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

CREATE INDEX IF NOT EXISTS idx_${POSTGRES_MESSAGE_TABLE}_plate_time
  ON public.${POSTGRES_MESSAGE_TABLE} (plate, message_time DESC);
SQL

echo "Postgres setup complete."
echo "DB: ${POSTGRES_DB}"
echo "Table: public.${POSTGRES_MESSAGE_TABLE}"
