-- CREATE Database
-- psql -h timescaledb -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'opal'" | grep -q 1 || psql -U postgres -c "CREATE DATABASE opal" ;

-- Extend the database with TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Define the two ENUMs
--create types
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'interaction_type') THEN
        CREATE TYPE interaction_type AS ENUM ('call', 'text');
    END IF;
    --more types here...
END$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'interaction_direction') THEN
        CREATE TYPE interaction_direction AS ENUM ('in', 'out');
    END IF;
    --more types here...
END$$;

-- create the table for the CDRs
CREATE TABLE IF NOT EXISTS opal (
  event_time          TIMESTAMPTZ       NOT NULL,
  interaction_type    interaction_type   NOT NULL,
  interaction_direction  interaction_direction NOT NULL,
  emiter_id           int NOT NULL,
  receiver_id         int NOT NULL,
  duration            int NOT NULL,
  antenna_id          int NOT NULL
--   UNIQUE(event_time, interaction_type, emiter_id, receiver_id)
);

-- This creates a hypertable that is partitioned by time
--   using the values in the `time` column.
SELECT create_hypertable('opal', 'event_time', if_not_exists => TRUE, chunk_time_interval => interval '36 hours');

-- for efficient user search
-- CREATE INDEX IF NOT EXISTS emiter_id_event_time_idx ON opal(emiter_id, event_time desc);

-- Define user to id map
CREATE TABLE IF NOT EXISTS user2id_map (
  id SERIAL,
  userid uuid,
  country int NOT NULL,
  weight float default 1.0 NOT NULL,
  CONSTRAINT user2id_map_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS userid_country_idx ON user2id_map(userid, country) WHERE userid is NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS userid_null_country_idx ON user2id_map(country) WHERE userid is NULL;

-- Some demo inserts
-- INSERT INTO opal(event_time, interaction_type, interaction_direction, emiter_id, receiver_id, duration, antenna_id)
-- VALUES (TIMESTAMP '2012-05-20 20:30:37', 'call', 'in', 98678, 212312, 137, 5214);

-- Define antenna name id map
CREATE TABLE IF NOT EXISTS antenna_name2id_map (
  id SERIAL,
  antenna_name varchar(32) NOT NULL UNIQUE,
  CONSTRAINT antenna_name2id_map_pkey PRIMARY KEY (id)
);

-- Define antenna_records table
CREATE TABLE IF NOT EXISTS antenna_records (
  id int NOT NULL,
  date_from timestamp with time zone NOT NULL,
  date_to timestamp with time zone NOT NULL,
  latitude double precision NOT NULL,
  longitude double precision NOT NULL,
  location_level_1 varchar(30) COLLATE pg_catalog."default" NOT NULL,
  location_level_2 varchar(30) COLLATE pg_catalog."default" NOT NULL,
  CONSTRAINT antenna_records_pkey PRIMARY KEY (id),
  -- UNIQUE (date_from, date_to, latitude, longitude), -- TODO Later Stage
  FOREIGN KEY (id) REFERENCES antenna_name2id_map(id) ON DELETE CASCADE
);