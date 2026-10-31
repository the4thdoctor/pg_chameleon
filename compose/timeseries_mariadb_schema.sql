-- Time series simulation and data type coverage for MariaDB 11.8
--
-- Usage (inside the compose stack or any 11.8 server):
--   mariadb -uroot -p < timeseries_mariadb_schema.sql
--
-- The schema creates the tables, functions, procedures and views only.
-- Data comes from load_timeseries.py, which generates random rows at a --scale.
-- timeseries_mariadb_data.sql is a static sample and is not loaded by the loader.
--
-- Layout:
--   ts_metrics       time series fact table, RANGE partitioned by month
--   ts_devices       device dimension, text / enum / set / json / uuid / binary types
--   ts_events        event log, serial and temporal types
--   ts_geo_samples   spatial, vector and blob types
--
-- Helpers:
--   ts_time_bucket()           function, bucket a timestamp to N seconds
--   ts_add_month_partition()   procedure, split pmax to add a month partition
--   ts_drop_partition()        procedure, drop a month partition (retention)
--   ts_generate_metrics()      procedure, fill ts_metrics with simulated readings
--   ts_metrics_5min            view, 5 minute downsampling
--   ts_latest                  view, last reading per device

DROP DATABASE IF EXISTS timeseries_db;
CREATE DATABASE timeseries_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE timeseries_db;

-- ---------------------------------------------------------------------------
-- Device dimension
-- ---------------------------------------------------------------------------
CREATE TABLE ts_devices (
    device_id       INT UNSIGNED NOT NULL AUTO_INCREMENT,
    device_uuid     UUID NOT NULL,
    device_code     CHAR(8) NOT NULL,
    device_name     VARCHAR(120) NOT NULL,
    serial_hash     BINARY(16) NULL,
    mac_address     VARBINARY(6) NULL,
    short_note      TINYTEXT NULL,
    description     TEXT NULL,
    manual          MEDIUMTEXT NULL,
    changelog       LONGTEXT NULL,
    status          ENUM('online', 'offline', 'maintenance', 'retired') NOT NULL DEFAULT 'online',
    capabilities    SET('temperature', 'humidity', 'pressure', 'vibration', 'power') NULL,
    config          JSON NULL,
    PRIMARY KEY (device_id),
    UNIQUE KEY uq_device_uuid (device_uuid),
    UNIQUE KEY uq_device_code (device_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Time series fact table, numeric and temporal types, partitioned by month.
-- Partitioning requires the partition key in every unique key, so the primary
-- key is (device_id, ts). The partition expression is TO_DAYS(ts).
-- ---------------------------------------------------------------------------
CREATE TABLE ts_metrics (
    device_id           INT UNSIGNED NOT NULL,
    ts                  DATETIME(6) NOT NULL,
    tinyint_val         TINYINT NULL,
    smallint_val        SMALLINT NULL,
    mediumint_val       MEDIUMINT NULL,
    int_val             INT NULL,
    bigint_val          BIGINT UNSIGNED NULL,
    decimal_val         DECIMAL(12, 4) NULL,
    numeric_val         NUMERIC(10, 2) NULL,
    dec_val             DEC(8, 3) NULL,
    fixed_val           FIXED(6, 2) NULL,
    float_val           FLOAT NULL,
    double_val          DOUBLE NULL,
    real_val            REAL NULL,
    double_precision_val DOUBLE PRECISION NULL,
    bit_flags           BIT(8) NULL,
    is_alarm            BOOL NULL,
    is_ok               BOOLEAN NULL,
    PRIMARY KEY (device_id, ts),
    KEY idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(ts)) (
    PARTITION p202610 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p202611 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p202612 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION pmax    VALUES LESS THAN MAXVALUE
);

-- ---------------------------------------------------------------------------
-- Event log, serial and remaining date and time types
-- ---------------------------------------------------------------------------
CREATE TABLE ts_events (
    event_id        SERIAL PRIMARY KEY,
    device_id       INT UNSIGNED NOT NULL,
    event_date      DATE NOT NULL,
    event_time      TIME(6) NOT NULL,
    event_datetime  DATETIME(3) NULL,
    recorded_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    fiscal_year     YEAR NULL,
    event_code      INTEGER NULL,
    event_text      VARCHAR(255) NULL,
    KEY idx_event_device_date (device_id, event_date),
    CONSTRAINT fk_events_device FOREIGN KEY (device_id) REFERENCES ts_devices (device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Geo and blob samples: spatial, VECTOR and BLOB types.
-- Spatial values use SRID 4326 (WGS 84) in ST_GeomFromText; MariaDB has no per-column SRID clause.
-- ---------------------------------------------------------------------------
CREATE TABLE ts_geo_samples (
    sample_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    device_id           INT UNSIGNED NOT NULL,
    sampled_at          DATETIME(6) NOT NULL,
    location            POINT NOT NULL,
    route               LINESTRING,
    coverage_area       POLYGON,
    waypoints           MULTIPOINT,
    multi_route         MULTILINESTRING,
    zones               MULTIPOLYGON,
    mixed_shapes        GEOMETRYCOLLECTION,
    any_shape           GEOMETRY,
    embedding           VECTOR(8) NULL,
    raw_chunk           TINYBLOB NULL,
    payload_blob        BLOB NULL,
    payload_medium      MEDIUMBLOB NULL,
    payload_long        LONGBLOB NULL,
    PRIMARY KEY (sample_id),
    SPATIAL KEY sp_location (location),
    KEY idx_geo_device_time (device_id, sampled_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Helper function: bucket a timestamp to a fixed number of seconds,
-- the MariaDB equivalent of TimescaleDB time_bucket().
-- ---------------------------------------------------------------------------
DELIMITER //

CREATE FUNCTION ts_time_bucket(p_ts DATETIME(6), p_seconds INT)
RETURNS DATETIME
DETERMINISTIC
NO SQL
BEGIN
    RETURN FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(p_ts) / p_seconds) * p_seconds);
END //

-- ---------------------------------------------------------------------------
-- Partition management. Splits pmax to create the partition for one month.
-- ---------------------------------------------------------------------------
CREATE PROCEDURE ts_add_month_partition(IN p_month DATE)
BEGIN
    SET @ts_part_name = CONCAT('p', DATE_FORMAT(p_month, '%Y%m'));
    SET @ts_part_limit = DATE_FORMAT(p_month + INTERVAL 1 MONTH, '%Y-%m-01');
    SET @ts_sql = CONCAT(
        'ALTER TABLE ts_metrics REORGANIZE PARTITION pmax INTO (',
        'PARTITION ', @ts_part_name, ' VALUES LESS THAN (TO_DAYS(''', @ts_part_limit, ''')),',
        'PARTITION pmax VALUES LESS THAN MAXVALUE)');
    PREPARE ts_stmt FROM @ts_sql;
    EXECUTE ts_stmt;
    DEALLOCATE PREPARE ts_stmt;
END //

-- Retention: dropping a partition removes a whole month of data at once.
CREATE PROCEDURE ts_drop_partition(IN p_month DATE)
BEGIN
    SET @ts_sql = CONCAT('ALTER TABLE ts_metrics DROP PARTITION ',
                         CONCAT('p', DATE_FORMAT(p_month, '%Y%m')));
    PREPARE ts_stmt FROM @ts_sql;
    EXECUTE ts_stmt;
    DEALLOCATE PREPARE ts_stmt;
END //

-- ---------------------------------------------------------------------------
-- Simulator: writes p_steps readings per device, p_step_seconds apart,
-- starting at p_start. Each device gets its own waveform.
-- ---------------------------------------------------------------------------
CREATE PROCEDURE ts_generate_metrics(IN p_start DATETIME(6), IN p_steps INT, IN p_step_seconds INT)
BEGIN
    DECLARE v_dev INT DEFAULT 1;
    DECLARE v_dev_count INT;
    DECLARE v_step INT;
    DECLARE v_ts DATETIME(6);
    DECLARE v_wave DOUBLE;
    DECLARE v_temp DOUBLE;

    SELECT COUNT(*) INTO v_dev_count FROM ts_devices;

    START TRANSACTION;
    WHILE v_dev <= v_dev_count DO
        SET v_step = 0;
        WHILE v_step < p_steps DO
            SET v_ts = p_start + INTERVAL (v_step * p_step_seconds) SECOND;
            SET v_wave = SIN(v_step / 200 + v_dev);
            SET v_temp = 20 + 5 * v_wave + RAND() * 0.5;

            INSERT INTO ts_metrics (
                device_id, ts, tinyint_val, smallint_val, mediumint_val, int_val, bigint_val,
                decimal_val, numeric_val, dec_val, fixed_val, float_val, double_val, real_val,
                double_precision_val, bit_flags, is_alarm, is_ok)
            VALUES (
                v_dev,
                v_ts,
                FLOOR(50 + 40 * v_wave),
                FLOOR(1500 + RAND() * 200),
                FLOOR(RAND() * 1000000),
                v_step,
                v_step * 1000000000,
                ROUND(v_temp, 4),
                ROUND(RAND() * 100, 2),
                ROUND(v_wave, 3),
                ROUND(RAND() * 10, 2),
                v_temp,
                v_temp * 1000,
                v_temp / 2,
                v_temp * 2,
                v_step % 256,
                v_temp > 24,
                v_temp <= 24);

            SET v_step = v_step + 1;
        END WHILE;
        SET v_dev = v_dev + 1;
    END WHILE;
    COMMIT;
END //

DELIMITER ;

-- ---------------------------------------------------------------------------
-- Views: downsampling and last reading per device
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ts_metrics_5min AS
SELECT
    device_id,
    ts_time_bucket(ts, 300) AS bucket,
    COUNT(*) AS samples,
    AVG(double_val) AS avg_double,
    MIN(decimal_val) AS min_decimal,
    MAX(decimal_val) AS max_decimal,
    SUM(is_alarm) AS alarm_count
FROM ts_metrics
GROUP BY device_id, ts_time_bucket(ts, 300);

CREATE OR REPLACE VIEW ts_latest AS
SELECT m.*
FROM ts_metrics AS m
JOIN (
    SELECT device_id, MAX(ts) AS max_ts
    FROM ts_metrics
    GROUP BY device_id
) AS l ON l.device_id = m.device_id AND l.max_ts = m.ts;

-- Example queries:
--   SELECT * FROM ts_metrics_5min WHERE device_id = 1 ORDER BY bucket LIMIT 10;
--   SELECT * FROM ts_latest;
--   CALL ts_add_month_partition('2027-01-01');   -- add a January 2027 partition
--   CALL ts_drop_partition('2026-10-01');         -- retention, drop October 2026
