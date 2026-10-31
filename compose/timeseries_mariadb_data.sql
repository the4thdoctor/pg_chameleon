-- Simulated data for the timeseries_db database, loaded after timeseries_mariadb_schema.sql
--
-- Usage (after the schema has been loaded):
--   mariadb -uroot -p < timeseries_mariadb_data.sql
--
-- Contents:
--   ts_devices       device rows
--   ts_events        event log rows
--   ts_geo_samples   geo and blob sample rows
--   ts_metrics       simulated readings, two days per device, one per minute

USE timeseries_db;

-- ---------------------------------------------------------------------------
-- Device dimension
-- ---------------------------------------------------------------------------
INSERT INTO ts_devices
    (device_uuid, device_code, device_name, serial_hash, mac_address, short_note, description, manual, changelog, status, capabilities, config)
VALUES
    (UUID(), 'DEV00001', 'Boiler room sensor A', UNHEX(MD5('dev1')), UNHEX('0A1B2C3D4E01'),
     'rack 1', 'Temperature and pressure on the boiler line', REPEAT('Install and calibrate. ', 20),
     'v1.0 initial release', 'online', 'temperature,pressure',
     '{"interval_s": 60, "unit": "celsius", "calibrated": true}'),
    (UUID(), 'DEV00002', 'Cold storage probe', UNHEX(MD5('dev2')), UNHEX('0A1B2C3D4E02'),
     'rack 1', 'Humidity and temperature in cold storage', REPEAT('Keep the probe dry. ', 20),
     'v1.1 humidity support', 'online', 'temperature,humidity',
     '{"interval_s": 60, "unit": "celsius", "calibrated": true}'),
    (UUID(), 'DEV00003', 'Compressor vibration node', UNHEX(MD5('dev3')), UNHEX('0A1B2C3D4E03'),
     'rack 2', 'Vibration and power draw of the compressor', REPEAT('Mount on the base plate. ', 20),
     'v2.0 vibration sampling', 'maintenance', 'vibration,power',
     '{"interval_s": 10, "unit": "g", "calibrated": false}'),
    (UUID(), 'DEV00004', 'Outdoor weather station', UNHEX(MD5('dev4')), UNHEX('0A1B2C3D4E04'),
     'roof', 'Full weather station', REPEAT('Weatherproof housing. ', 20),
     'v3.0 solar powered', 'online', 'temperature,humidity,pressure,power',
     '{"interval_s": 300, "unit": "mixed", "calibrated": true}'),
    (UUID(), 'DEV00005', 'Legacy gateway', UNHEX(MD5('dev5')), UNHEX('0A1B2C3D4E05'),
     NULL, 'Decommissioned gateway kept for history', NULL,
     'retired in 2025', 'retired', NULL,
     NULL);

-- ---------------------------------------------------------------------------
-- Event log
-- ---------------------------------------------------------------------------
INSERT INTO ts_events (device_id, event_date, event_time, event_datetime, fiscal_year, event_code, event_text)
VALUES
    (1, '2026-10-28', '08:15:00.000000', '2026-10-28 08:15:00.000', 2026, 100, 'boiler start'),
    (1, '2026-10-28', '17:45:30.500000', '2026-10-28 17:45:30.500', 2026, 101, 'boiler stop'),
    (2, '2026-10-28', '06:00:00.000000', '2026-10-28 06:00:00.000', 2026, 200, 'door open alarm'),
    (3, '2026-10-29', '12:30:10.250000', '2026-10-29 12:30:10.250', 2026, 300, 'vibration threshold'),
    (4, '2026-10-29', '00:00:00.000000', NULL, 2026, 400, 'rain sensor reset');

-- ---------------------------------------------------------------------------
-- Geo and blob samples
-- ---------------------------------------------------------------------------
INSERT INTO ts_geo_samples
    (device_id, sampled_at, location, route, coverage_area, waypoints, multi_route, zones, mixed_shapes, any_shape,
     embedding, raw_chunk, payload_blob, payload_medium, payload_long)
VALUES
    (4, '2026-10-29 09:00:00.000000',
     ST_GeomFromText('POINT(-73.9857 40.7484)', 4326),
     ST_GeomFromText('LINESTRING(-73.9857 40.7484, -73.9800 40.7510)', 4326),
     ST_GeomFromText('POLYGON((-74.00 40.70, -73.97 40.70, -73.97 40.73, -74.00 40.73, -74.00 40.70))', 4326),
     ST_GeomFromText('MULTIPOINT((-73.99 40.71), (-73.98 40.72))', 4326),
     ST_GeomFromText('MULTILINESTRING((-74.00 40.70, -73.98 40.72), (-73.97 40.71, -73.96 40.73))', 4326),
     ST_GeomFromText('MULTIPOLYGON(((-74.00 40.70, -73.99 40.70, -73.99 40.71, -74.00 40.70)))', 4326),
     ST_GeomFromText('GEOMETRYCOLLECTION(POINT(-73.98 40.75), LINESTRING(-73.98 40.75, -73.97 40.76))', 4326),
     ST_GeomFromText('POINT(-73.9857 40.7484)', 4326),
     VEC_FromText('[0.12, -0.53, 0.88, 0.01, -0.27, 0.64, -0.09, 0.45]'),
     0x0102030405,
     X'DEADBEEFCAFE',
     REPEAT(X'AB', 1024),
     REPEAT(X'CD', 2048));

-- ---------------------------------------------------------------------------
-- Simulator: two days, one reading per minute, for every device.
-- ---------------------------------------------------------------------------
CALL ts_generate_metrics('2026-10-28 00:00:00.000000', 2880, 60);
