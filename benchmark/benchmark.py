#!/usr/bin/env python3
"""Benchmark the pipeline on a large synthetic ticket dataset.

Generates N messy ticket records (seeded, reproducible — includes
duplicates, junk rows and inconsistent casing so cleaning is exercised),
then runs the real pipeline and reports wall-clock time and throughput.

Usage:
    python3 benchmark/benchmark.py --rows 200000
    python3 benchmark/benchmark.py --rows 1000000 --workers 8 --batch-size 50000

Standard library only.
"""

import argparse
import csv
import os
import random
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
    """Write n messy ticket rows. ~2% duplicates, ~1% junk rows."""
    rnd = random.Random(seed)
    base_ids = [f"TKT-{100000 + i}" for i in range(n)]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ticket_id", "subject", "description", "region", "channel"])
        for i in range(n):
            r = rnd.random()
            if r < 0.01:
                ticket_id = ""  # junk row: no ticket id
            elif r < 0.03 and i > 0:
                ticket_id = rnd.choice(base_ids[:i])  # duplicate of earlier row
            else:
                ticket_id = base_ids[i]
            writer.writerow([
                ticket_id,
                rnd.choice(SUBJECTS),
                rnd.choice(DESCRIPTIONS),
                rnd.choice(REGIONS),
                rnd.choice(CHANNELS),
            ])
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark the pipeline.")
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tmp_dir = "/tmp/pipeline_benchmark"
    os.makedirs(tmp_dir, exist_ok=True)
    input_path = os.path.join(tmp_dir, f"tickets_{args.rows}.csv")
    output_dir = os.path.join(tmp_dir, "out")

    print(f"Generating {args.rows} synthetic ticket rows (seed={args.seed})...")
    t0 = time.perf_counter()
    generate(input_path, args.rows, args.seed)
    print(f"Generated in {time.perf_counter() - t0:.1f}s -> {input_path}")

    cmd = [sys.executable, os.path.join(repo_root, "pipeline", "process_data.py"),
           "--input", input_path, "--output", output_dir,
           "--batch-size", str(args.batch_size)]
    if args.workers:
        cmd += ["--workers", str(args.workers)]
    print("Running:", " ".join(cmd))

    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - t0
    sys.stdout.write(proc.stdout)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"pipeline failed (exit {proc.returncode})")

    with open(os.path.join(output_dir, "structured_report.csv"),
              encoding="utf-8") as fh:
        out_rows = sum(1 for _ in fh) - 1
    rate = out_rows / elapsed if elapsed > 0 else 0
    print(f"\nBENCHMARK: rows_in={args.rows} rows_out={out_rows} "
          f"elapsed={elapsed:.1f}s throughput={rate:.0f} rows/s "
          f"workers={args.workers or 'default'} batch_size={args.batch_size}")


if __name__ == "__main__":
    raise SystemExit(main())
