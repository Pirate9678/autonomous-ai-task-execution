#!/usr/bin/env python3
"""Benchmark the pipeline on a large synthetic ticket dataset.

Generates N messy ticket records (seeded, reproducible — includes
duplicates, junk rows and inconsistent casing so cleaning is exercised),
then runs the real pipeline and reports wall-clock time, throughput
and peak memory. Supports CSV mode (default) and SQLite database mode.

Usage:
    python3 benchmark/benchmark.py --rows 200000
    python3 benchmark/benchmark.py --rows 1000000 --workers 1 --batch-size 20000
    python3 benchmark/benchmark.py --rows 10000000 --mode both
    python3 benchmark/benchmark.py --rows 1000000 --mode db --no-generate
    python3 benchmark/benchmark.py --verify        # classifier equivalence

Results accumulate in benchmark/results.json (one entry per
mode/rows/workers combination). Standard library only.

Notes:
- Scratch files live under benchmark/tmp/ (gitignored): /tmp is a small
  tmpfs on this host and a 10M-row CSV is ~1.1 GB.
- Throughput on this host is CPU-throttled (shared 1.5 vCPU cell), so
  absolute rows/s are lower than on a dedicated 4-vCPU host; compare
  before/after on the SAME host, not against the README's 1 Nov 2026
  numbers measured elsewhere.
"""

import argparse
import csv
import hashlib
import itertools
import json
import os
import random
import resource
import subprocess
import sys
import time

SUBJECTS = [
    "Invoice charged twice", "Login not working", "Package arrived late",
    "Refund not received", "App crashes on startup", "Password reset broken",
    "Wrong item delivered", "Subscription cancelled but billed",
    "Tracking not updating", "Feature request: dark mode",
    "Payment declined incorrectly", "Account locked out",
    "Delivery rescheduled twice", "Overcharged on renewal",
    "Server timeout on checkout", "Damaged product received",
]
DESCRIPTIONS = [
    "This is unacceptable, please fix immediately, I am very angry.",
    "Getting an error every time I try since yesterday morning.",
    "It was supposed to arrive on Monday but there is still no update.",
    "Thank you, great service, really appreciate the quick help!",
    "Tried three times, same failure, this is the worst experience.",
    "Could you please look into this when you get a chance?",
    "URGENT: system is down and we are losing orders every minute.",
    "I was charged twice for the same invoice, need a refund asap.",
    "Love the product, just a small suggestion to improve the UI.",
    "Package shows delivered but nothing arrived, please investigate.",
]
REGIONS = ["delhi", "MUMBAI", "  gurugram ", "Chennai", "KOLKATA", "pune",
           "Hyderabad", "ahmedabad", "", "Bengaluru"]
CHANNELS = ["email", "web", "phone", "chat", "EMAIL", ""]


def generate(path, n, seed=42):
    """Write n messy ticket rows. ~2% duplicates, ~1% junk rows.

    Ticket ids are formatted on the fly (TKT-100000+i); a duplicate is a
    random earlier index. This keeps memory flat — the old version built
    a list of all n ids and sliced it per duplicate draw, which is
    O(n) memory and O(n) per duplicate. RNG consumption is identical
    to the old version (one _randbelow(i) per duplicate draw), so the
    output is byte-identical to the old generator for the same seed.
    """
    rnd = random.Random(seed)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ticket_id", "subject", "description", "region", "channel"])
        for i in range(n):
            r = rnd.random()
            if r < 0.01:
                ticket_id = ""  # junk row: no ticket id
            elif r < 0.03 and i > 0:
                ticket_id = f"TKT-{100000 + rnd.randrange(i)}"  # duplicate of earlier row
            else:
                ticket_id = f"TKT-{100000 + i}"
            writer.writerow([
                ticket_id,
                rnd.choice(SUBJECTS),
                rnd.choice(DESCRIPTIONS),
                rnd.choice(REGIONS),
                rnd.choice(CHANNELS),
            ])
    return path


def run_timed(cmd, log_path):
    """Run a command, return (elapsed_s, peak_rss_mb, returncode, output).

    Peak RSS comes from wait4() rusage (the child's own maximum), so it
    is per-run, not cumulative. Output is tee'd to log_path and returned.
    """
    t0 = time.perf_counter()
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        _, status, rusage = os.wait4(proc.pid, 0)
    elapsed = time.perf_counter() - t0
    with open(log_path, encoding="utf-8") as fh:
        out = fh.read()
    return elapsed, rusage.ru_maxrss / 1024, \
        os.waitstatus_to_exitcode(status), out


def count_csv_rows(path):
    with open(path, encoding="utf-8") as fh:
        return sum(1 for _ in fh) - 1


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_classifier():
    """Prove the optimised classifier/normaliser are identical to the
    pre-optimisation behaviour on every synthetic combination plus
    edge cases. Returns True if everything matches."""
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "pipeline"))
    import importlib
    import ai_prompts
    importlib.reload(ai_prompts)
    from ai_prompts import _classify_text, validate_classification
    from process_data import _normalise_text

    # Reference: the original any()-based decisions. Recompute them here
    # from the keyword tuples (the tuples themselves never changed).
    def ref_classify(text):
        t = text.lower()
        if any(k in t for k in ai_prompts._BILLING):
            category = "billing"
        elif any(k in t for k in ai_prompts._LOGISTICS):
            category = "logistics"
        elif any(k in t for k in ai_prompts._ACCOUNT):
            category = "account"
        elif any(k in t for k in ai_prompts._TECHNICAL):
            category = "technical"
        elif any(k in t for k in ai_prompts._FEEDBACK):
            category = "feedback"
        else:
            category = "unknown"
        if any(k in t for k in ai_prompts._NEGATIVE):
            sentiment = "negative"
        elif any(k in t for k in ai_prompts._POSITIVE):
            sentiment = "positive"
        else:
            sentiment = "neutral"
        if any(k in t for k in ai_prompts._URGENT_HINTS):
            priority = "urgent"
        elif sentiment == "negative" and category in ("billing", "technical"):
            priority = "high"
        elif sentiment == "negative":
            priority = "medium"
        elif category == "feedback":
            priority = "low"
        else:
            priority = "medium"
        return {"category": category, "sentiment": sentiment,
                "priority": priority,
                "action_required": sentiment == "negative"
                or priority in ("high", "urgent")}

    texts = [f"{s} {d}" for s, d in itertools.product(SUBJECTS, DESCRIPTIONS)]
    texts += ["", "   ", "INVOICE CHARGED TWICE URGENT",
              "Login not working, charged twice, system down",
              "love it", "x" * 500, "café ünïcode  test",
              "tab\tnewline\ntext", "double  spaces   here"]
    bad = 0
    for t in texts:
        if _classify_text(t) != ref_classify(t):
            bad += 1
            print("CLASSIFIER MISMATCH:", repr(t)[:80])
        v_new = validate_classification("TKT-1", _classify_text(t))
        v_ref = validate_classification("TKT-1", ref_classify(t))
        if (v_new["valid"], v_new["confidence"]) != (
                v_ref["valid"], v_ref["confidence"]):
            bad += 1
            print("VALIDATION MISMATCH:", repr(t)[:80])
    print(f"classifier: {len(texts) - bad}/{len(texts)} combinations identical")

    def ref_normalise(value):
        import re
        if value is None:
            return None
        cleaned = re.sub(r"\s+", " ", str(value)).strip()
        return cleaned if cleaned else None

    vals = [None, "", "   ", "  a  b\tc\nd  ", "no-space", "  x ",
            "a\u00a0b", "a\u2003b", "a\rb", "  mixed \t\n whitespace  "]
    nbad = 0
    for v in vals:
        if _normalise_text(v) != ref_normalise(v):
            nbad += 1
            print("NORMALISE MISMATCH:", repr(v))
    print(f"normalise: {len(vals) - nbad}/{len(vals)} cases identical")
    return bad == 0 and nbad == 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark the pipeline.")
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--mode", choices=["csv", "db", "both"],
                        default="csv",
                        help="csv: CSV in/out (default); db: SQLite in/out; "
                             "both: run csv then db and compare outputs")
    parser.add_argument("--no-generate", action="store_true",
                        help="Reuse the existing generated CSV in "
                             "benchmark/tmp/ instead of regenerating it.")
    parser.add_argument("--verify", action="store_true",
                        help="Only run the classifier/normaliser "
                             "equivalence check, no benchmark.")
    args = parser.parse_args(argv)

    if args.verify:
        ok = verify_classifier()
        raise SystemExit(0 if ok else 1)

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tmp_dir = os.path.join(repo_root, "benchmark", "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    input_path = os.path.join(tmp_dir, f"tickets_{args.rows}.csv")
    db_path = os.path.join(tmp_dir, f"tickets_{args.rows}.db")
    results_path = os.path.join(repo_root, "benchmark", "results.json")

    if not args.no_generate or not os.path.exists(input_path):
        print(f"Generating {args.rows} synthetic ticket rows (seed={args.seed})...")
        t0 = time.perf_counter()
        generate(input_path, args.rows, args.seed)
        print(f"Generated in {time.perf_counter() - t0:.1f}s -> {input_path}")
    else:
        print(f"Reusing existing input: {input_path}")

    pipeline = os.path.join(repo_root, "pipeline", "process_data.py")
    db_io = os.path.join(repo_root, "pipeline", "db_io.py")
    results = {}
    if os.path.exists(results_path):
        results = json.load(open(results_path))

    def run_csv():
        out_dir = os.path.join(tmp_dir, f"out_csv_{args.rows}")
        cmd = [sys.executable, pipeline, "--input", input_path,
               "--output", out_dir, "--batch-size", str(args.batch_size)]
        if args.workers:
            cmd += ["--workers", str(args.workers)]
        print("Running CSV mode:", " ".join(cmd))
        elapsed, peak_mb, rc, out = run_timed(cmd, os.path.join(tmp_dir, "last_csv.log"))
        sys.stdout.write(out)
        if rc != 0:
            raise SystemExit(f"pipeline failed (exit {rc})")
        report = os.path.join(out_dir, "structured_report.csv")
        out_rows = count_csv_rows(report)
        rate = out_rows / elapsed if elapsed > 0 else 0
        print(f"\nBENCHMARK csv: rows_in={args.rows} rows_out={out_rows} "
              f"elapsed={elapsed:.1f}s throughput={rate:.0f} rows/s "
              f"peak_rss={peak_mb:.0f}MB workers={args.workers or 'default'} "
              f"batch_size={args.batch_size}")
        return {"mode": "csv", "rows_in": args.rows, "rows_out": out_rows,
                "elapsed_s": round(elapsed, 1), "rows_per_s": round(rate),
                "peak_rss_mb": round(peak_mb),
                "workers": args.workers or "default",
                "batch_size": args.batch_size,
                "report_md5": md5(report)}

    def run_db():
        # Load the CSV into SQLite (timed separately — a one-off step).
        if os.path.exists(db_path):
            os.remove(db_path)
        cmd = [sys.executable, db_io, "load", "--csv", input_path,
               "--db", db_path]
        print("Loading CSV into SQLite:", " ".join(cmd))
        elapsed, peak_mb, rc, out = run_timed(cmd, os.path.join(tmp_dir, "last_dbload.log"))
        sys.stdout.write(out)
        if rc != 0:
            raise SystemExit(f"db load failed (exit {rc})")
        print(f"DB load: {elapsed:.1f}s peak_rss={peak_mb:.0f}MB")
        load_s = round(elapsed, 1)

        cmd = [sys.executable, pipeline, "--db", db_path,
               "--batch-size", str(args.batch_size)]
        if args.workers:
            cmd += ["--workers", str(args.workers)]
        print("Running DB mode:", " ".join(cmd))
        elapsed, peak_mb, rc, out = run_timed(cmd, os.path.join(tmp_dir, "last_db.log"))
        sys.stdout.write(out)
        if rc != 0:
            raise SystemExit(f"pipeline (db mode) failed (exit {rc})")
        dump_path = os.path.join(tmp_dir, f"report_db_{args.rows}.csv")
        cmd = [sys.executable, db_io, "dump", "--db", db_path,
               "--csv", dump_path]
        subprocess.run(cmd, check=True, capture_output=True)
        out_rows = count_csv_rows(dump_path)
        rate = out_rows / elapsed if elapsed > 0 else 0
        print(f"\nBENCHMARK db: rows_in={args.rows} rows_out={out_rows} "
              f"elapsed={elapsed:.1f}s throughput={rate:.0f} rows/s "
              f"peak_rss={peak_mb:.0f}MB workers={args.workers or 'default'} "
              f"batch_size={args.batch_size} (db load: {load_s}s, not counted)")
        return {"mode": "db", "rows_in": args.rows, "rows_out": out_rows,
                "elapsed_s": round(elapsed, 1), "rows_per_s": round(rate),
                "peak_rss_mb": round(peak_mb),
                "workers": args.workers or "default",
                "batch_size": args.batch_size,
                "db_load_s": load_s,
                "report_md5": md5(dump_path)}

    key = f"{args.rows}_w{args.workers or 'default'}_b{args.batch_size}"
    entries = []
    if args.mode in ("csv", "both"):
        entries.append(run_csv())
    if args.mode in ("db", "both"):
        entries.append(run_db())
    if len(entries) == 2:
        same = entries[0]["report_md5"] == entries[1]["report_md5"]
        print(f"\nCSV vs DB report identical: {same} "
              f"(csv md5 {entries[0]['report_md5'][:12]}, "
              f"db md5 {entries[1]['report_md5'][:12]})")
        entries[0]["csv_db_identical"] = same
        entries[1]["csv_db_identical"] = same
        if not same:
            print("WARNING: CSV and DB outputs differ!")
    results[key] = entries
    json.dump(results, open(results_path, "w"), indent=2)
    print(f"Wrote {results_path}")


if __name__ == "__main__":
    raise SystemExit(main())
