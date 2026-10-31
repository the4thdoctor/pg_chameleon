#!/usr/bin/env python3
"""Load the timeseries MariaDB schema and fill it with generated data.

Runs every statement in the schema file over a PyMySQL connection, honouring
DELIMITER changes so stored procedures and functions are sent whole. Then
generates random rows for every table, sized by --scale. The data is produced
here, not read from timeseries_mariadb_data.sql.

Sizes at scale 1: 5 devices, 20 events, 5 geo samples, and 2880 readings per
device (two days, one per minute). Each scale step adds 5 devices, 20 events and
5 geo samples, and the readings grow with the device count.
"""

import argparse
import hashlib
import json
import math
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

import pymysql

DB_NAME = "timeseries_db"
DEVICES_PER_SCALE = 5
EVENTS_PER_SCALE = 20
GEO_PER_SCALE = 5
READINGS_PER_DEVICE = 2880
START = datetime(2026, 10, 28)
STEP_SECONDS = 60
BATCH_SIZE = 5000

STATUSES = ["online", "offline", "maintenance", "retired"]
STATUS_WEIGHTS = [70, 10, 10, 10]
CAPABILITIES = ["temperature", "humidity", "pressure", "vibration", "power"]
EVENTS = [
    (100, "boiler start"),
    (101, "boiler stop"),
    (200, "door open alarm"),
    (300, "vibration threshold"),
    (400, "rain sensor reset"),
]


def split_statements(text):
    """Split a MariaDB script into statements, honouring DELIMITER directives."""
    delimiter = ";"
    statements = []
    buf = []

    for line in text.splitlines():
        stripped = line.strip()

        if not buf and (not stripped or stripped.startswith("--")):
            continue

        if stripped.upper().startswith("DELIMITER "):
            delimiter = stripped.split(None, 1)[1]
            continue

        if stripped.endswith(delimiter):
            buf.append(stripped[: -len(delimiter)])
            statements.append("\n".join(buf).strip())
            buf = []
        else:
            buf.append(line)

    if buf and "\n".join(buf).strip():
        statements.append("\n".join(buf).strip())

    return [s for s in statements if s]


def device_rows(rng, count):
    """Yield one tuple per device, matching the ts_devices column list."""
    for n in range(1, count + 1):
        caps = sorted(rng.sample(CAPABILITIES, rng.randint(1, 4)))
        config = {
            "interval_s": rng.choice([10, 60, 300]),
            "unit": rng.choice(["celsius", "g", "mixed"]),
            "calibrated": rng.random() < 0.8,
        }
        yield (
            str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            f"DEV{n:05d}",
            f"Simulated device {n}",
            hashlib.md5(f"dev{n}".encode()).digest(),
            rng.getrandbits(48).to_bytes(6, "big"),
            f"rack {rng.randint(1, 4)}",
            f"Random device with {', '.join(caps)}",
            rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
            ",".join(caps),
            json.dumps(config),
        )


def event_rows(rng, device_ids, count):
    """Yield one tuple per event, matching the ts_events column list."""
    for _ in range(count):
        event_day = START + timedelta(days=rng.randint(0, 1))
        event_time = timedelta(seconds=rng.randint(0, 86399), microseconds=rng.randrange(0, 1000000, 1000))
        event_datetime = None
        if rng.random() < 0.9:
            event_datetime = datetime.combine(event_day, datetime.min.time()) + event_time
        code, text = rng.choice(EVENTS)
        yield (
            rng.choice(device_ids),
            event_day.date(),
            _time_text(event_time),
            event_datetime,
            2026,
            code,
            text,
        )


def _time_text(delta):
    """Format a timedelta as HH:MM:SS.ffffff for a TIME(6) column."""
    total = delta.seconds
    return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}.{delta.microseconds:06d}"


def _point(lon, lat):
    return f"{lon:.6f} {lat:.6f}"


def _box(rng, lon, lat):
    """Four corners of a small square, closed back on the first corner."""
    d = rng.uniform(0.005, 0.02)
    corners = [(lon, lat), (lon + d, lat), (lon + d, lat + d), (lon, lat + d)]
    return corners + [corners[0]]


def geo_rows(rng, device_ids, count):
    """Yield one tuple per geo sample, matching the ts_geo_samples column list."""
    for _ in range(count):
        lon = rng.uniform(-74.00, -73.95)
        lat = rng.uniform(40.70, 40.76)
        p = _point(lon, lat)
        q = _point(lon + 0.004, lat + 0.003)
        ring = ", ".join(_point(x, y) for x, y in _box(rng, lon, lat))
        triangle = f"{p}, {q}, {_point(lon + 0.002, lat + 0.006)}, {p}"
        other = _point(lon - 0.001, lat + 0.002)
        sampled_at = START + timedelta(seconds=rng.randint(0, 2 * 86400 - 1))
        yield (
            rng.choice(device_ids),
            sampled_at,
            f"POINT({p})",
            f"LINESTRING({p}, {q})",
            f"POLYGON(({ring}))",
            f"MULTIPOINT(({p}), ({q}))",
            f"MULTILINESTRING(({p}, {q}), ({other}, {q}))",
            f"MULTIPOLYGON((({triangle})))",
            f"GEOMETRYCOLLECTION(POINT({p}), LINESTRING({p}, {q}))",
            f"POINT({p})",
            "[" + ", ".join(f"{rng.uniform(-1, 1):.2f}" for _ in range(8)) + "]",
            rng.randbytes(8),
            rng.randbytes(6),
            rng.randbytes(1024),
            rng.randbytes(2048),
        )


def metric_rows(rng, device_ids):
    """Yield one tuple per reading, matching the ts_metrics column list.

    Each device gets its own sine waveform, as in ts_generate_metrics().
    """
    for device_id in device_ids:
        for step in range(READINGS_PER_DEVICE):
            ts = START + timedelta(seconds=step * STEP_SECONDS)
            wave = math.sin(step / 200 + device_id)
            temp = 20 + 5 * wave + rng.random() * 0.5
            yield (
                device_id,
                ts,
                math.floor(50 + 40 * wave),
                math.floor(1500 + rng.random() * 200),
                math.floor(rng.random() * 1000000),
                step,
                step * 1000000000,
                round(temp, 4),
                round(rng.random() * 100, 2),
                round(wave, 3),
                round(rng.random() * 10, 2),
                temp,
                temp * 1000,
                temp / 2,
                temp * 2,
                step % 256,
                int(temp > 24),
                int(temp <= 24),
            )


def insert_rows(conn, sql, rows, label):
    """Insert rows in batches inside one transaction and report progress."""
    total = 0
    batch = []
    with conn.cursor() as cur:
        for row in rows:
            batch.append(row)
            if len(batch) == BATCH_SIZE:
                cur.executemany(sql, batch)
                total += len(batch)
                batch = []
        if batch:
            cur.executemany(sql, batch)
            total += len(batch)
    conn.commit()
    print(f"{label}: {total} rows", flush=True)
    return total


def run_schema(conn, path):
    with open(path, encoding="utf-8") as fh:
        statements = split_statements(fh.read())

    with conn.cursor() as cur:
        for number, statement in enumerate(statements, start=1):
            cur.execute(statement)
            print(f"[{number}/{len(statements)}] ok", flush=True)

    print(f"loaded {len(statements)} statements from {path}")


def generate(conn, scale, seed):
    rng = random.Random(seed)
    devices = DEVICES_PER_SCALE * scale

    conn.select_db(DB_NAME)
    insert_rows(
        conn,
        "INSERT INTO ts_devices (device_uuid, device_code, device_name, serial_hash, mac_address, "
        "short_note, description, status, capabilities, config) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        device_rows(rng, devices),
        "ts_devices",
    )

    with conn.cursor() as cur:
        cur.execute("SELECT device_id FROM ts_devices ORDER BY device_id")
        device_ids = [row[0] for row in cur.fetchall()]

    insert_rows(
        conn,
        "INSERT INTO ts_events (device_id, event_date, event_time, event_datetime, fiscal_year, "
        "event_code, event_text) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        event_rows(rng, device_ids, EVENTS_PER_SCALE * scale),
        "ts_events",
    )

    insert_rows(
        conn,
        "INSERT INTO ts_geo_samples (device_id, sampled_at, location, route, coverage_area, waypoints, "
        "multi_route, zones, mixed_shapes, any_shape, embedding, raw_chunk, payload_blob, payload_medium, "
        "payload_long) VALUES (%s, %s, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), "
        "ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), "
        "ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), "
        "VEC_FromText(%s), %s, %s, %s, %s)",
        geo_rows(rng, device_ids, GEO_PER_SCALE * scale),
        "ts_geo_samples",
    )

    insert_rows(
        conn,
        "INSERT INTO ts_metrics (device_id, ts, tinyint_val, smallint_val, mediumint_val, int_val, "
        "bigint_val, decimal_val, numeric_val, dec_val, fixed_val, float_val, double_val, real_val, "
        "double_precision_val, bit_flags, is_alarm, is_ok) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        metric_rows(rng, device_ids),
        "ts_metrics",
    )


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("scale must be at least 1")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("schema", help="path to timeseries_mariadb_schema.sql")
    parser.add_argument("--scale", type=positive_int, default=1,
                        help="dataset scale factor, 1 gives the sample size (default: 1)")
    parser.add_argument("--seed", type=int, default=None, help="random seed for reproducible data")
    parser.add_argument("--socket", default="/var/lib/mysql/mysql.sock")
    parser.add_argument("--host", default=None, help="use TCP instead of the socket")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=os.environ.get("MARIADB_ROOT_PASSWORD", ""))
    args = parser.parse_args()

    conn_args = {"user": args.user, "password": args.password, "autocommit": True}
    if args.host:
        conn_args.update(host=args.host, port=args.port)
    else:
        conn_args.update(unix_socket=args.socket)

    conn = pymysql.connect(**conn_args)
    try:
        run_schema(conn, args.schema)
        conn.autocommit(False)
        generate(conn, args.scale, args.seed)
    finally:
        conn.close()

    print(f"generated data at scale {args.scale}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
