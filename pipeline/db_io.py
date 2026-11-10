#!/usr/bin/env python3
"""SQLite helpers for the pipeline's database mode.

The pipeline's default is CSV in / CSV out. With ``--db path/to.db`` it
reads raw records from a table and writes the enriched report and the
run log to tables in the same SQLite file instead:

    # 1. Load raw records into the database (once per dataset)
    python3 pipeline/db_io.py load --csv big_tickets.csv --db tickets.db

    # 2. Run the pipeline against the database
    python3 pipeline/process_data.py --db tickets.db --output output/
    #    (--output is still required: the run log is written to the DB,
    #     but the flag is kept so CSV-mode invocations don't change.)

    # 3. Dump the report table back to CSV (e.g. to compare with CSV mode)
    python3 pipeline/db_io.py dump --db tickets.db --csv report_from_db.csv

Default table names: raw_tickets (input), structured_report (output),
run_log (audit). Override with --table / --report-table / --log-table.

Design notes
------------
* SQLite is the default backend because it is serverless and needs no
  install — the whole "large database" mode runs on this host with the
  standard library only. Reads stream with fetchmany() and writes use
  executemany() inside one transaction per batch, so memory stays flat
  as the database grows.
* The report table stores ai_confidence as REAL and the two boolean
  columns as TEXT ('True'/'False'), matching the CSV output exactly
  when dumped back (confidence only ever takes 0.95 / 0.55).
* PostgreSQL: the SQL here is standard except for two things —
  placeholders (SQLite uses ``?``, PostgreSQL uses ``%s``) and the
  INTEGER PRIMARY KEY AUTOINCREMENT column (PostgreSQL: SERIAL / 
  GENERATED ALWAYS AS IDENTITY). Types, table shapes and the
  read-in-rowid-order / write-per-batch structure transfer as-is.
  PostgreSQL support was NOT tested — this module is SQLite only.
"""

import argparse
import csv
import os
import sqlite3
import sys

RAW_FIELDS = ["ticket_id", "subject", "description", "region", "channel"]
REPORT_FIELDS = [
    "ticket_id", "subject", "description", "region", "channel",
    "ai_category", "ai_sentiment", "ai_priority",
    "ai_action_required", "ai_confidence", "ai_valid",
]
LOG_FIELDS = [
    "run_timestamp_utc", "input_file", "records_in",
    "records_clean", "records_out", "records_dropped",
    "errors", "status",
]

DEFAULT_INPUT_TABLE = "raw_tickets"
DEFAULT_REPORT_TABLE = "structured_report"
DEFAULT_LOG_TABLE = "run_log"

# Bulk-load pragmas: WAL + NORMAL is the safe middle ground (a crash
# loses at most the in-flight batch, which is re-runnable). synchronous=OFF
# is faster still but can corrupt the file on a power cut — not used.
PRAGMAS = [
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("cache_size", "-64000"),  # 64 MB page cache
    ("temp_store", "MEMORY"),
]


def connect(db_path, pragmas=True):
    """Open the SQLite file, creating it (and its directory) if needed."""
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path)
    if pragmas:
        for key, value in PRAGMAS:
            conn.execute(f"PRAGMA {key}={value}")
    return conn


def create_raw_table(conn, table=DEFAULT_INPUT_TABLE):
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "ticket_id TEXT, subject TEXT, description TEXT, "
        "region TEXT, channel TEXT)"
    )
    conn.commit()


def create_report_table(conn, table=DEFAULT_REPORT_TABLE):
    """Create the report table, dropping any previous run's rows first
    (mirrors CSV mode, which overwrites structured_report.csv)."""
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(
        f"CREATE TABLE {table} ("
        "ticket_id TEXT, subject TEXT, description TEXT, "
        "region TEXT, channel TEXT, ai_category TEXT, "
        "ai_sentiment TEXT, ai_priority TEXT, "
        "ai_action_required TEXT, ai_confidence REAL, ai_valid TEXT)"
    )
    conn.commit()


def create_log_table(conn, table=DEFAULT_LOG_TABLE):
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "run_timestamp_utc TEXT, input_file TEXT, records_in INTEGER, "
        "records_clean INTEGER, records_out INTEGER, "
        "records_dropped INTEGER, errors INTEGER, status TEXT)"
    )
    conn.commit()


def load_csv(conn, csv_path, table=DEFAULT_INPUT_TABLE, batch_size=20000):
    """Bulk-load a raw CSV into `table`. Returns the row count loaded."""
    create_raw_table(conn, table)
    conn.execute(f"DELETE FROM {table}")
    placeholders = ", ".join("?" * len(RAW_FIELDS))
    sql = (f"INSERT INTO {table} ({', '.join(RAW_FIELDS)}) "
           f"VALUES ({placeholders})")
    loaded = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        batch = []
        for row in reader:
            batch.append([row.get(f) for f in RAW_FIELDS])
            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                conn.commit()
                loaded += len(batch)
                batch = []
        if batch:
            conn.executemany(sql, batch)
            conn.commit()
            loaded += len(batch)
    return loaded


def iter_db_records(conn, table, batch_size, stats):
    """Yield batches of raw record dicts from `table`, in rowid order
    (insertion order — the same order the CSV rows were loaded in).
    Malformed rows cannot occur here: the table only holds TEXT."""
    cols = ", ".join(RAW_FIELDS)
    cursor = conn.execute(f"SELECT {cols} FROM {table} ORDER BY rowid")
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            return
        yield [dict(zip(RAW_FIELDS, r)) for r in rows]


def write_report_batch(conn, table, rows):
    """Insert one enriched batch. Caller owns the transaction."""
    placeholders = ", ".join("?" * len(REPORT_FIELDS))
    sql = (f"INSERT INTO {table} ({', '.join(REPORT_FIELDS)}) "
           f"VALUES ({placeholders})")
    conn.executemany(sql, [[r.get(f) for f in REPORT_FIELDS] for r in rows])


def append_run_log_db(conn, table, stats):
    create_log_table(conn, table)
    placeholders = ", ".join("?" * len(LOG_FIELDS))
    sql = (f"INSERT INTO {table} ({', '.join(LOG_FIELDS)}) "
           f"VALUES ({placeholders})")
    conn.execute(sql, [stats.get(f) for f in LOG_FIELDS])
    conn.commit()


def count_rows(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def dump_table(conn, table, csv_path):
    """Dump a report table to CSV with the same field order and value
    formatting as CSV mode (confidence as '0.95'/'0.55', booleans as
    'True'/'False'), so dumps compare byte-for-byte with CSV output."""
    cols = ", ".join(REPORT_FIELDS)
    cursor = conn.execute(f"SELECT {cols} FROM {table} ORDER BY rowid")
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(REPORT_FIELDS)
        for row in cursor:
            writer.writerow([
                (repr(v) if isinstance(v, float) else v) for v in row
            ])


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="SQLite helpers: load raw CSVs into the pipeline "
                    "database, or dump a report table back to CSV.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_load = sub.add_parser("load", help="Load a raw CSV into a table.")
    p_load.add_argument("--csv", required=True)
    p_load.add_argument("--db", required=True)
    p_load.add_argument("--table", default=DEFAULT_INPUT_TABLE)

    p_dump = sub.add_parser("dump", help="Dump a report table to CSV.")
    p_dump.add_argument("--db", required=True)
    p_dump.add_argument("--table", default=DEFAULT_REPORT_TABLE)
    p_dump.add_argument("--csv", required=True)

    args = parser.parse_args(argv)
    if args.command == "load":
        if not os.path.isfile(args.csv):
            parser.error(f"csv file not found: {args.csv}")
        conn = connect(args.db)
        try:
            n = load_csv(conn, args.csv, args.table)
        finally:
            conn.close()
        print(f"loaded {n} rows into {args.table} in {args.db}")
    else:
        conn = connect(args.db, pragmas=False)
        try:
            dump_table(conn, args.table, args.csv)
        finally:
            conn.close()
        print(f"dumped {args.table} from {args.db} to {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
