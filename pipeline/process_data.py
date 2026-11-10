#!/usr/bin/env python3
"""Autonomous data-processing pipeline — optimised for large volumes.

Stages:
  1. EXTRACT  — stream raw records from CSV in configurable batches.
               Memory stays flat no matter how large the input is.
  2. CLEAN    — normalise fields: missing values, deduplication,
                casing, whitespace, type coercion. Dedup uses a
                128-bit hash of the normalised ticket_id (case-insensitive),
                so the dedup index costs ~50 bytes per unique key instead
                of holding full strings.
  3. ENRICH   — run each record through the custom AI-prompt pipeline
                (pipeline/ai_prompts.py) in parallel worker processes.
                Order of records is preserved.
  4. LOAD     — stream enriched rows straight to structured_report.csv
                and append one audit row to run_log.csv.

Usage:
    python3 pipeline/process_data.py --input data/sample_input.csv --output output/
    python3 pipeline/process_data.py --input big.csv --output output/ \\
        --workers 8 --batch-size 20000

    # Database mode (SQLite): read from a table, write report + run log
    # to tables in the same file — see pipeline/db_io.py
    python3 pipeline/db_io.py load --csv big.csv --db tickets.db
    python3 pipeline/process_data.py --db tickets.db

Standard library only — runs anywhere Python 3.8+ is installed.
In production this script is executed by the n8n "Run Python Pipeline"
Execute Command node on an AWS EC2 host; locally it runs the same way.
The n8n node only passes --input/--output; --workers/--batch-size fall
back to safe defaults, so the workflow needs no changes.
"""

import argparse
import csv
import hashlib
import logging
import multiprocessing
import os
import sys
import time
from datetime import datetime, timezone

# Make `pipeline/` importable regardless of the working directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ai_prompts import analyse_record, load_prompts
from db_io import (DEFAULT_INPUT_TABLE, DEFAULT_LOG_TABLE,
                   DEFAULT_REPORT_TABLE, append_run_log_db,
                   connect as db_connect, create_report_table,
                   iter_db_records, write_report_batch)

# Expected schema of the raw input. Every record is normalised to this.
EXPECTED_FIELDS = ["ticket_id", "subject", "description", "region", "channel"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Stage 1 — EXTRACT (streaming)
# ---------------------------------------------------------------------------

def iter_raw_records(input_path, stats):
    """Yield raw record dicts one at a time; malformed rows are counted
    in stats['extract_errors'] and skipped.

    Rows are yielded as-is (no per-value strip here): RecordCleaner
    normalises every field it uses, so stripping at extract time was
    duplicate work. Iterating with next() inside try/except means a
    genuinely malformed row is skipped and counted instead of aborting
    the run — the old version only guarded the dict comprehension,
    so a csv parsing error would have crashed the run.
    """
    with open(input_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        line_no = 1  # header is line 1
        while True:
            try:
                row = next(reader)
            except StopIteration:
                return
            except Exception as exc:  # malformed row — log and keep going
                line_no += 1
                log.warning("Skipping malformed row %d: %s", line_no, exc)
                stats["extract_errors"] += 1
                continue
            line_no += 1
            yield row


def batched(iterable, size):
    """Yield lists of up to `size` items from an iterable."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


# ---------------------------------------------------------------------------
# Stage 2 — CLEAN
# ---------------------------------------------------------------------------

def _normalise_text(value):
    """Collapse whitespace and strip; empty/blank becomes None.

    str.split() (no args) splits on any whitespace run and drops
    empties, so " ".join(...) collapses + strips in one C-level pass —
    ~2x faster than re.sub(r"\\s+", " ", ...) per field at scale.
    Verified identical on edge cases (tabs, newlines, NBSP, em-space).
    """
    if value is None:
        return None
    cleaned = " ".join(str(value).split())
    return cleaned if cleaned else None


class RecordCleaner:
    """Stateful cleaner: normalises records and drops junk/duplicates.

    Deduplication is case-insensitive on ticket_id and keeps the first
    occurrence. The seen-set stores 16-byte hashes rather than raw
    strings, so memory grows ~50 bytes per unique key however long the
    ids are.
    """

    def __init__(self):
        self._seen = set()
        self.dropped = 0

    @staticmethod
    def _key(ticket_id):
        return hashlib.blake2b(ticket_id.lower().encode("utf-8"),
                               digest_size=16).digest()

    def clean_one(self, rec):
        """Return the normalised record, or None if it should be dropped."""
        ticket_id = _normalise_text(rec.get("ticket_id"))
        if not ticket_id:
            self.dropped += 1
            return None
        key = self._key(ticket_id)
        if key in self._seen:
            self.dropped += 1
            return None
        self._seen.add(key)

        return {
            "ticket_id": ticket_id,
            "subject": _normalise_text(rec.get("subject")) or "[no subject]",
            "description": _normalise_text(rec.get("description")) or "[no description]",
            "region": (_normalise_text(rec.get("region")) or "Unknown").title(),
            "channel": (_normalise_text(rec.get("channel")) or "unknown").lower(),
        }

    def clean_batch(self, batch):
        cleaned = []
        for rec in batch:
            out = self.clean_one(rec)
            if out is not None:
                cleaned.append(out)
        return cleaned


# ---------------------------------------------------------------------------
# Stage 3 — ENRICH (custom AI-prompt pipeline, parallelised)
# ---------------------------------------------------------------------------

_WORKER_PROMPTS = None


def _worker_init(prompts_path):
    """Load the prompt templates once per worker process."""
    global _WORKER_PROMPTS
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from ai_prompts import load_prompts
    _WORKER_PROMPTS = load_prompts(prompts_path)


def _enrich_one(rec):
    """Run one record through the AI prompt pipeline. Returns
    (enriched_record, None) on success or (None, (ticket_id, error)).

    render=False: the rendered prompt is discarded by the pipeline, so
    it is not built in the hot path (see analyse_record)."""
    try:
        from ai_prompts import analyse_record
        enriched, _prompt, _validation = analyse_record(
            rec, _WORKER_PROMPTS, render=False)
        return enriched, None
    except Exception as exc:  # noqa: BLE001 — keep the batch alive
        return None, (rec.get("ticket_id"), str(exc))


def enrich_batch_serial(cleaned, prompts):
    """Single-process enrichment (used when --workers 1).

    render=False: the rendered prompt is discarded by the pipeline, so
    it is not built in the hot path (see analyse_record)."""
    enriched, errors = [], 0
    for rec in cleaned:
        try:
            rec_out, _prompt, _validation = analyse_record(
                rec, prompts, render=False)
            enriched.append(rec_out)
        except Exception as exc:  # noqa: BLE001
            log.warning("AI step failed for %s: %s", rec.get("ticket_id"), exc)
            errors += 1
    return enriched, errors


def enrich_batch_parallel(cleaned, pool, workers):
    """Multi-process enrichment. pool.imap preserves record order, so the
    output is identical to the serial path."""
    enriched, errors = [], 0
    chunksize = max(1, len(cleaned) // (workers * 4))
    for rec_out, err in pool.imap(_enrich_one, cleaned, chunksize=chunksize):
        if rec_out is not None:
            enriched.append(rec_out)
        else:
            errors += 1
            log.warning("AI step failed for %s: %s", err[0], err[1])
    return enriched, errors


# ---------------------------------------------------------------------------
# Stage 4 — LOAD (streaming)
# ---------------------------------------------------------------------------

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


def append_run_log(log_path, stats):
    is_new = not os.path.exists(log_path)
    with open(log_path, "a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=LOG_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(stats)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(input_path=None, output_dir=None, prompts_path="pipeline/prompts.yaml",
        batch_size=10000, workers=None, db_path=None,
        input_table=None, report_table=None, log_table=None):
    """Run the pipeline.

    CSV mode (default): read --input CSV, write structured_report.csv +
    run_log.csv into --output. Database mode (--db path/to.db): read raw
    records from `input_table` (default raw_tickets) and write the
    enriched report + run log to `report_table` / `log_table` in the
    same SQLite file. Cleaning, enrichment and dedup are identical in
    both modes; only the source and sink differ.
    """
    workers = workers or os.cpu_count() or 1
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    prompts = load_prompts(prompts_path)
    log.info("Loaded %d prompt templates from %s", len(prompts), prompts_path)
    log.info("Streaming in batches of %d with %d worker(s)",
             batch_size, workers)

    cleaner = RecordCleaner()
    stats = {
        "run_timestamp_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_file": (os.path.basename(input_path) if input_path
                       else (input_table or DEFAULT_INPUT_TABLE)),
        "records_in": 0,
        "records_clean": 0,
        "records_out": 0,
        "records_dropped": 0,
        "extract_errors": 0,
        "errors": 0,
        "status": "success",
    }

    use_db = db_path is not None
    conn = None
    report_path = log_path = None
    if use_db:
        input_table = input_table or DEFAULT_INPUT_TABLE
        report_table = report_table or DEFAULT_REPORT_TABLE
        log_table = log_table or DEFAULT_LOG_TABLE
        conn = db_connect(db_path)
        create_report_table(conn, report_table)
        log.info("Database mode: %s -> %s.%s (report), %s.%s (log)",
                 db_path, db_path, report_table, db_path, log_table)
        source = iter_db_records(conn, input_table, batch_size, stats)
    else:
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, "structured_report.csv")
        log_path = os.path.join(output_dir, "run_log.csv")
        source = batched(iter_raw_records(input_path, stats), batch_size)

    pool = None
    if workers > 1:
        pool = multiprocessing.Pool(
            processes=workers,
            initializer=_worker_init,
            initargs=(os.path.abspath(prompts_path),),
        )
    try:
        if use_db:
            _run_batches(source, cleaner, stats, prompts, pool, workers,
                         t0, sink=("db", conn, report_table))
        else:
            with open(report_path, "w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=REPORT_FIELDS,
                                        extrasaction="ignore")
                writer.writeheader()
                _run_batches(source, cleaner, stats, prompts, pool, workers,
                             t0, sink=("csv", writer))
    finally:
        if pool is not None:
            pool.close()
            pool.join()
        if conn is not None:
            conn.close()

    stats["records_dropped"] = cleaner.dropped
    stats["errors"] += stats.pop("extract_errors")
    if stats["errors"]:
        stats["status"] = "completed_with_errors"
    if use_db:
        conn = db_connect(db_path)
        try:
            append_run_log_db(conn, log_table, stats)
        finally:
            conn.close()
    else:
        append_run_log(log_path, stats)

    elapsed = time.perf_counter() - t0
    log.info("Run complete: %d in -> %d out, %d dropped, %d errors "
             "in %.1fs (%.0f rows/s)",
             stats["records_in"], stats["records_out"],
             stats["records_dropped"], stats["errors"],
             elapsed, stats["records_out"] / elapsed if elapsed > 0 else 0)
    return stats, report_path, log_path


def _run_batches(batches, cleaner, stats, prompts, pool, workers, t0, sink):
    """Consume record batches: clean -> enrich -> write to the sink.

    `sink` is ("csv", DictWriter) or ("db", connection, report_table).
    DB writes commit once per batch, so a crash loses at most one batch.
    """
    kind = sink[0]
    for batch in batches:
        stats["records_in"] += len(batch)
        cleaned = cleaner.clean_batch(batch)
        stats["records_clean"] += len(cleaned)

        if workers > 1:
            enriched, errs = enrich_batch_parallel(cleaned, pool, workers)
        else:
            enriched, errs = enrich_batch_serial(cleaned, prompts)
        stats["errors"] += errs

        if kind == "csv":
            sink[1].writerows(enriched)
        else:
            conn, table = sink[1], sink[2]
            try:
                conn.execute("BEGIN")
                write_report_batch(conn, table, enriched)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        stats["records_out"] += len(enriched)

        elapsed = time.perf_counter() - t0
        rate = stats["records_out"] / elapsed if elapsed > 0 else 0
        log.info("Progress: %d/%d records in, %d enriched "
                 "(%.0f rows/s, %d dropped so far)",
                 stats["records_in"], stats["records_in"],
                 stats["records_out"], rate, cleaner.dropped)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Autonomous data-processing pipeline: extract, clean, "
                    "AI-enrich and load ticket records. Streams in batches "
                    "and enriches in parallel so input size is not limited "
                    "by RAM. CSV mode by default; --db switches to "
                    "SQLite database mode (see pipeline/db_io.py).")
    parser.add_argument("--input",
                        help="Path to the raw input CSV (e.g. data/sample_input.csv). "
                             "Required in CSV mode; ignored with --db.")
    parser.add_argument("--output",
                        help="Directory for output CSVs (e.g. output/). "
                             "Required in CSV mode; optional with --db.")
    parser.add_argument("--db", default=None,
                        help="SQLite database file for database mode "
                             "(e.g. tickets.db). Reads raw records from "
                             "--db-input-table and writes the report + run "
                             "log to --db-report-table / --db-log-table.")
    parser.add_argument("--db-input-table", default=None,
                        help="Input table in database mode (default: raw_tickets)")
    parser.add_argument("--db-report-table", default=None,
                        help="Report table in database mode (default: structured_report)")
    parser.add_argument("--db-log-table", default=None,
                        help="Run-log table in database mode (default: run_log)")
    parser.add_argument("--prompts", default="pipeline/prompts.yaml",
                        help="Path to the custom prompt templates YAML")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Records streamed per batch (default: 10000). "
                             "Lower it on memory-constrained hosts.")
    parser.add_argument("--workers", type=int, default=None,
                        help="Parallel enrichment workers (default: CPU count). "
                             "Use 1 to force single-process mode.")
    args = parser.parse_args(argv)

    if args.db is None and not args.input:
        parser.error("--input is required in CSV mode (omit it only with --db)")
    if args.db is None and not args.output:
        parser.error("--output is required in CSV mode (omit it only with --db)")
    if args.input and not os.path.isfile(args.input):
        parser.error(f"input file not found: {args.input}")
    if not os.path.isfile(args.prompts):
        parser.error(f"prompts file not found: {args.prompts}")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    if args.workers is not None and args.workers < 1:
        parser.error("--workers must be >= 1")

    stats, report_path, log_path = run(
        args.input, args.output, args.prompts,
        batch_size=args.batch_size, workers=args.workers,
        db_path=args.db, input_table=args.db_input_table,
        report_table=args.db_report_table, log_table=args.db_log_table,
    )
    print(f"records_in={stats['records_in']} records_out={stats['records_out']} "
          f"dropped={stats['records_dropped']} errors={stats['errors']}")
    if args.db:
        print(f"report table: {args.db_report_table or 'structured_report'} "
              f"in {args.db}")
        print(f"run log table: {args.db_log_table or 'run_log'} in {args.db}")
    else:
        print(f"report: {report_path}")
        print(f"run log: {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
