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
import re
import sys
import time
from datetime import datetime, timezone

# Make `pipeline/` importable regardless of the working directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ai_prompts import analyse_record, load_prompts

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
    in stats['extract_errors'] and skipped."""
    with open(input_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for line_no, row in enumerate(reader, start=2):  # header is line 1
            try:
                yield {k: (v.strip() if isinstance(v, str) else v)
                       for k, v in row.items()}
            except Exception as exc:  # malformed row — log and keep going
                log.warning("Skipping malformed row %d: %s", line_no, exc)
                stats["extract_errors"] += 1


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
    """Collapse whitespace and strip; empty/blank becomes None."""
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
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
    (enriched_record, None) on success or (None, (ticket_id, error))."""
    try:
        from ai_prompts import analyse_record
        enriched, _prompt, _validation = analyse_record(rec, _WORKER_PROMPTS)
        return enriched, None
    except Exception as exc:  # noqa: BLE001 — keep the batch alive
        return None, (rec.get("ticket_id"), str(exc))


def enrich_batch_serial(cleaned, prompts):
    """Single-process enrichment (used when --workers 1)."""
    enriched, errors = [], 0
    for rec in cleaned:
        try:
            rec_out, _prompt, _validation = analyse_record(rec, prompts)
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

def run(input_path, output_dir, prompts_path="pipeline/prompts.yaml",
        batch_size=10000, workers=None):
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
        "input_file": os.path.basename(input_path),
        "records_in": 0,
        "records_clean": 0,
        "records_out": 0,
        "records_dropped": 0,
        "extract_errors": 0,
        "errors": 0,
        "status": "success",
    }

    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "structured_report.csv")
    log_path = os.path.join(output_dir, "run_log.csv")

    pool = None
    if workers > 1:
        pool = multiprocessing.Pool(
            processes=workers,
            initializer=_worker_init,
            initargs=(os.path.abspath(prompts_path),),
        )
    try:
        with open(report_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=REPORT_FIELDS,
                                    extrasaction="ignore")
            writer.writeheader()

            for batch in batched(iter_raw_records(input_path, stats), batch_size):
                stats["records_in"] += len(batch)
                cleaned = cleaner.clean_batch(batch)
                stats["records_clean"] += len(cleaned)

                if workers > 1:
                    enriched, errs = enrich_batch_parallel(cleaned, pool, workers)
                else:
                    enriched, errs = enrich_batch_serial(cleaned, prompts)
                stats["errors"] += errs

                writer.writerows(enriched)
                stats["records_out"] += len(enriched)

                elapsed = time.perf_counter() - t0
                rate = stats["records_out"] / elapsed if elapsed > 0 else 0
                log.info("Progress: %d/%d records in, %d enriched "
                         "(%.0f rows/s, %d dropped so far)",
                         stats["records_in"], stats["records_in"],
                         stats["records_out"], rate, cleaner.dropped)
    finally:
        if pool is not None:
            pool.close()
            pool.join()

    stats["records_dropped"] = cleaner.dropped
    stats["errors"] += stats.pop("extract_errors")
    if stats["errors"]:
        stats["status"] = "completed_with_errors"
    append_run_log(log_path, stats)

    elapsed = time.perf_counter() - t0
    log.info("Run complete: %d in -> %d out, %d dropped, %d errors "
             "in %.1fs (%.0f rows/s)",
             stats["records_in"], stats["records_out"],
             stats["records_dropped"], stats["errors"],
             elapsed, stats["records_out"] / elapsed if elapsed > 0 else 0)
    return stats, report_path, log_path


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Autonomous data-processing pipeline: extract, clean, "
                    "AI-enrich and load ticket records. Streams in batches "
                    "and enriches in parallel so input size is not limited "
                    "by RAM.")
    parser.add_argument("--input", required=True,
                        help="Path to the raw input CSV (e.g. data/sample_input.csv)")
    parser.add_argument("--output", required=True,
                        help="Directory for output CSVs (e.g. output/)")
    parser.add_argument("--prompts", default="pipeline/prompts.yaml",
                        help="Path to the custom prompt templates YAML")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Records streamed per batch (default: 10000). "
                             "Lower it on memory-constrained hosts.")
    parser.add_argument("--workers", type=int, default=None,
                        help="Parallel enrichment workers (default: CPU count). "
                             "Use 1 to force single-process mode.")
    args = parser.parse_args(argv)

    if not os.path.isfile(args.input):
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
    )
    print(f"records_in={stats['records_in']} records_out={stats['records_out']} "
          f"dropped={stats['records_dropped']} errors={stats['errors']}")
    print(f"report: {report_path}")
    print(f"run log: {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
