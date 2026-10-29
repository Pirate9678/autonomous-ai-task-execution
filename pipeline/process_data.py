#!/usr/bin/env python3
"""Autonomous data-processing pipeline.

Stages:
  1. EXTRACT  — read raw records from a CSV (data source pull).
  2. CLEAN    — normalise fields: missing values, deduplication, casing,
                whitespace, type coercion.
  3. ENRICH   — run each record through the custom AI-prompt pipeline
                (pipeline/ai_prompts.py), adding category, sentiment,
                priority, validation flags.
  4. LOAD     — write the structured spreadsheet artefacts:
                output/structured_report.csv (one row per clean record)
                output/run_log.csv           (run-level audit log)

Usage:
    python3 pipeline/process_data.py --input data/sample_input.csv --output output/

Standard library only — runs anywhere Python 3.8+ is installed.
In production this script is executed by the n8n "Run Python Pipeline"
Execute Command node on an AWS EC2 host; locally it runs the same way.
"""

import argparse
import csv
import logging
import os
import re
import sys
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
# Stage 1 — EXTRACT
# ---------------------------------------------------------------------------

def extract_records(input_path):
    """Read raw records from CSV. Returns (list_of_dicts, error_count)."""
    records, errors = [], 0
    with open(input_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for line_no, row in enumerate(reader, start=2):  # header is line 1
            try:
                records.append({k: (v.strip() if isinstance(v, str) else v)
                                for k, v in row.items()})
            except Exception as exc:  # malformed row — log and keep going
                log.warning("Skipping malformed row %d: %s", line_no, exc)
                errors += 1
    log.info("Extracted %d raw records (%d malformed skipped)", len(records), errors)
    return records, errors


# ---------------------------------------------------------------------------
# Stage 2 — CLEAN
# ---------------------------------------------------------------------------

def _normalise_text(value):
    """Collapse whitespace and strip; empty/blank becomes None."""
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned if cleaned else None


def clean_records(records):
    """Normalise, deduplicate and type-coerce raw records.

    Cleaning rules:
    - Drop rows with no usable ticket_id (junk rows).
    - Deduplicate on ticket_id, keeping the first occurrence.
    - Normalise region casing ("delhi" -> "Delhi"), strip whitespace.
    - Coerce missing subject/description to a sentinel so the AI step can
      still classify the record explicitly as unknown.
    """
    seen, cleaned, dropped = set(), [], 0

    for rec in records:
        ticket_id = _normalise_text(rec.get("ticket_id"))
        if not ticket_id:
            dropped += 1
            continue
        if ticket_id.lower() in seen:  # dedupe is case-insensitive
            dropped += 1
            continue
        seen.add(ticket_id.lower())

        cleaned.append({
            "ticket_id": ticket_id,
            "subject": _normalise_text(rec.get("subject")) or "[no subject]",
            "description": _normalise_text(rec.get("description")) or "[no description]",
            "region": (_normalise_text(rec.get("region")) or "Unknown").title(),
            "channel": (_normalise_text(rec.get("channel")) or "unknown").lower(),
        })

    log.info("Cleaned: %d records kept, %d junk/duplicate rows dropped",
             len(cleaned), dropped)
    return cleaned, dropped


# ---------------------------------------------------------------------------
# Stage 3 — ENRICH (custom AI-prompt pipeline)
# ---------------------------------------------------------------------------

def enrich_records(records, prompts):
    """Run each record through the AI prompt pipeline. Returns
    (enriched_records, error_count)."""
    enriched, errors = [], 0
    for rec in records:
        try:
            enriched_rec, _prompt, _validation = analyse_record(rec, prompts)
            enriched.append(enriched_rec)
        except Exception as exc:
            log.warning("AI step failed for %s: %s", rec.get("ticket_id"), exc)
            errors += 1
    log.info("Enriched %d records with AI analysis (%d errors)", len(enriched), errors)
    return enriched, errors


# ---------------------------------------------------------------------------
# Stage 4 — LOAD
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


def write_outputs(enriched, stats, output_dir):
    """Write structured_report.csv and append a row to run_log.csv."""
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, "structured_report.csv")
    with open(report_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPORT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(enriched)
    log.info("Wrote %d rows -> %s", len(enriched), report_path)

    log_path = os.path.join(output_dir, "run_log.csv")
    is_new = not os.path.exists(log_path)
    with open(log_path, "a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=LOG_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(stats)
    log.info("Appended run entry -> %s", log_path)
    return report_path, log_path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(input_path, output_dir, prompts_path="pipeline/prompts.yaml"):
    started = datetime.now(timezone.utc)
    prompts = load_prompts(prompts_path)
    log.info("Loaded %d prompt templates from %s", len(prompts), prompts_path)

    raw, extract_errors = extract_records(input_path)
    cleaned, dropped = clean_records(raw)
    enriched, enrich_errors = enrich_records(cleaned, prompts)

    stats = {
        "run_timestamp_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_file": os.path.basename(input_path),
        "records_in": len(raw),
        "records_clean": len(cleaned),
        "records_out": len(enriched),
        "records_dropped": dropped,
        "errors": extract_errors + enrich_errors,
        "status": "success" if not enrich_errors else "completed_with_errors",
    }
    report_path, log_path = write_outputs(enriched, stats, output_dir)

    log.info("Run complete: %d in -> %d out, %d dropped, %d errors",
             stats["records_in"], stats["records_out"],
             stats["records_dropped"], stats["errors"])
    return stats, report_path, log_path


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Autonomous data-processing pipeline: extract, clean, "
                    "AI-enrich and load support-ticket records.")
    parser.add_argument("--input", required=True,
                        help="Path to the raw input CSV (e.g. data/sample_input.csv)")
    parser.add_argument("--output", required=True,
                        help="Directory for output CSVs (e.g. output/)")
    parser.add_argument("--prompts", default="pipeline/prompts.yaml",
                        help="Path to the custom prompt templates YAML")
    args = parser.parse_args(argv)

    if not os.path.isfile(args.input):
        parser.error(f"input file not found: {args.input}")
    if not os.path.isfile(args.prompts):
        parser.error(f"prompts file not found: {args.prompts}")

    stats, report_path, log_path = run(args.input, args.output, args.prompts)
    print(f"records_in={stats['records_in']} records_out={stats['records_out']} "
          f"dropped={stats['records_dropped']} errors={stats['errors']}")
    print(f"report: {report_path}")
    print(f"run log: {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
