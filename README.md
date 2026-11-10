# Autonomous AI Task Execution Architecture

An autonomous workflow that turns messy operational records (support tickets,
lead lists, form dumps) into **structured, AI-enriched spreadsheets** —
without manual handling.

The architecture has two cooperating halves:

1. **Orchestration (n8n, on AWS EC2)** — a scheduled workflow pulls records
   from a data source, runs each record through a **custom AI prompt** for
   analytical classification (category, sentiment, priority, action
   required), executes the Python pipeline, and logs the results directly
   into a structured spreadsheet.
2. **Pipeline (Python, standard library only)** — extract → clean →
   AI-enrich → load. Runs standalone for local development and is invoked
   by n8n's Execute Command node in production.

## Architecture & data flow

```mermaid
flowchart LR
    A[Schedule Trigger<br/>hourly] --> B[HTTP Request<br/>pull open tickets]
    B --> C[Set<br/>normalise fields]
    C --> D[OpenAI node<br/>custom classification prompt]
    D --> E[Execute Command<br/>python3 pipeline/process_data.py]
    E --> F[Google Sheets<br/>append structured rows]
    F --> G[Run Summary]
```

```
┌──────────────────────── AWS EC2 (Docker) ────────────────────────┐
│  ┌──────────┐    ┌──────────────────────────────┐   ┌──────────┐ │
│  │   n8n    │───▶│ pipeline/process_data.py     │──▶│ Google   │ │
│  │ workflow │    │  batched stream → clean →    │   │ Sheets   │ │
│  └──────────┘    │  AI-enrich (parallel) → load │   └──────────┘ │
│                  │  prompts.yaml (4 templates)  │                │
└──────────────────────────────────────────────────────────────────┘
```

What the automation actually does (factually):

- Pulls records on a schedule — no manual export step.
- Cleans data programmatically: drops junk rows, deduplicates, normalises
  casing/whitespace, coerces types.
- Classifies every record with a **custom system prompt** (category /
  sentiment / priority / action-required) instead of human triage.
- Validates each classification with a QA self-check prompt before writing.
- Appends the enriched records to a spreadsheet and writes an audit
  `run_log.csv` (timestamp, records in/out, errors) for every run.

## Repository structure

```
autonomous-ai-task-execution/
├── n8n/
│   └── workflow.json          # importable n8n workflow export (7 nodes)
├── pipeline/
│   ├── process_data.py        # the ETL: extract → clean → enrich → load
│   ├── ai_prompts.py          # prompt rendering + deterministic local classifier
│   ├── db_io.py               # SQLite helpers: load CSVs, stream tables
│   └── prompts.yaml           # 4 custom prompt templates (extraction,
│                              #   classification, summarisation, validation)
├── data/
│   └── sample_input.csv       # 63 messy sample ticket records
├── benchmark/
│   ├── benchmark.py           # generate N synthetic rows & time a full run
│   ├── results.json           # measured numbers (see Performance & scaling)
│   └── tmp/                   # benchmark scratch (gitignored)
├── output/                    # sample run outputs (committed for reference;
│   ├── structured_report.csv  #   regenerated locally on each run)
│   └── run_log.csv            # one enriched row per clean record / audit log
├── deploy/
│   ├── Dockerfile             # n8n image with the repo baked in
│   ├── docker-compose.yml     # n8n + data volumes stack
│   └── ec2-setup.md           # step-by-step EC2 deployment guide
├── requirements.txt           # standard library only — nothing to install
└── .gitignore
```

## Quick start (run the pipeline locally)

Requirements: Python 3.8+ and nothing else — the pipeline uses only the
standard library (`csv`, `argparse`, `logging`, `datetime`, `re`, `json`).

```bash
git clone https://github.com/Pirate9678/autonomous-ai-task-execution.git
cd autonomous-ai-task-execution

# run the full pipeline on the sample data
python3 pipeline/process_data.py --input data/sample_input.csv --output output/

# large input: stream in batches, enrich in parallel
python3 pipeline/process_data.py --input big_tickets.csv --output output/ \
    --workers 8 --batch-size 20000

# benchmark on 200k synthetic rows (generates data, times a real run)
python3 benchmark/benchmark.py --rows 200000

# database mode: 1M rows from a SQLite table instead of a CSV
python3 pipeline/db_io.py load --csv big_tickets.csv --db tickets.db
python3 pipeline/process_data.py --db tickets.db --workers 1
```

## Running against a database

CSV in / CSV out is the default, but for large workloads the pipeline
can read from and write to a SQLite database instead — no server needed,
standard library only:

```bash
# 1. Load raw records into the database (once per dataset)
python3 pipeline/db_io.py load --csv big_tickets.csv --db tickets.db

# 2. Run the pipeline against the database
python3 pipeline/process_data.py --db tickets.db --workers 1

# 3. (Optional) dump the report table back to CSV
python3 pipeline/db_io.py dump --db tickets.db --csv report_from_db.csv
```

Default tables are `raw_tickets` (input), `structured_report` (output)
and `run_log` (audit); override with `--db-input-table`,
`--db-report-table`, `--db-log-table`. The report table is dropped and
recreated on each run (like the CSV being overwritten); the run log
appends. Reads stream in `rowid` order and writes commit once per batch,
so memory stays flat as the database grows. At 10M rows the SQLite file
is ~2.4 GB with the report table included, and loading 10M rows takes
~77 s (one-off).

PostgreSQL: the SQL is standard except placeholders (SQLite `?` vs
PostgreSQL `%s`) and the `INTEGER PRIMARY KEY AUTOINCREMENT` column
(PostgreSQL: `SERIAL` / `GENERATED ALWAYS AS IDENTITY`). PostgreSQL
support was **not** tested on this host — SQLite is the only tested
backend.

The n8n workflow needs no change for database mode: its **Run Python
Pipeline** node invokes the pipeline in CSV mode, which still works
exactly as before. To run the workflow against a database instead,
change that one node's command to
`python3 pipeline/process_data.py --db /data/tickets.db` (after loading
the tickets into the DB with `db_io.py load`).

Expected result: `output/structured_report.csv` (61 enriched rows — 63
records in, 1 junk row and 1 duplicate dropped) and an appended entry in
`output/run_log.csv`.

> **Note:** locally, the "AI analysis" step uses the deterministic
> rule-based classifier in `pipeline/ai_prompts.py`, which mirrors the
> decisions of the `classification` prompt in `prompts.yaml`. In
> production, the n8n **AI Classification** node sends the same rendered
> prompt to an LLM; `analyse_record_with_llm()` shows the equivalent
> production code path.

## Sample run output

A completed run on the bundled sample data is committed under
[`output/`](output/) so you can see the end-to-end result without running
anything:

- [`output/structured_report.csv`](output/structured_report.csv) — 61
  enriched rows (63 records in, 1 junk row + 1 duplicate dropped).
  Each row carries the AI enrichment columns:
  `ai_category`, `ai_sentiment`, `ai_priority`, `ai_action_required`,
  `ai_confidence`, `ai_valid`.
- [`output/run_log.csv`](output/run_log.csv) — the audit entry for the
  run (`2026-10-29T08:52:40Z, records_in=63, records_out=61, errors=0,
  status=success`).

Example enriched rows:

| ticket_id | subject              | ai_category | ai_sentiment | ai_priority | ai_action_required |
|-----------|----------------------|-------------|--------------|-------------|--------------------|
| TKT-1001  | Invoice charged twice| billing     | negative     | urgent      | True               |
| TKT-1002  | Login not working    | technical   | neutral      | medium      | False              |
| TKT-1003  | Package arrived late | logistics   | neutral      | medium      | False              |

## Performance & scaling

The pipeline is built for large inputs, not just the 63-row sample:

- **Streaming, constant memory** — records flow through in batches
  (`--batch-size`, default 10,000); the full dataset is never held in RAM.
- **Parallel enrichment** — the AI-classification stage runs across worker
  processes (`--workers`, default = CPU count; `--workers 1` forces
  single-process mode). Record order is preserved, so parallel and serial
  runs produce byte-identical output.
- **Compact dedup index** — duplicates are detected on a 128-bit hash of
  the normalised ticket id (~90 bytes per unique key in the CPython set)
  instead of full strings.

Measured with `benchmark/benchmark.py` (synthetic messy tickets: ~1% junk
rows, ~2% duplicates; `--workers 1 --batch-size 20000`; full numbers in
`benchmark/results.json`). "Before" is the pre-optimisation code run on
the same host, same input, back-to-back. This host is a shared 1.5-vCPU
cell, so absolute rows/s vary with load — compare before/after here, not
against numbers measured on other hosts.

| Input rows | Mode | Before (Nov 2026) | After (Nov 2026) | Speedup | Peak RAM |
|------------|------|-------------------|------------------|---------|----------|
| 1,000,000  | CSV    | 32.6 s (~29,800 rows/s) | **19.8 s (~49,100 rows/s)** | 1.65x | 155 MB |
| 1,000,000  | SQLite | — (new mode) | **20.9 s (~46,300 rows/s)** | — | 224 MB |
| 10,000,000 | CSV    | 534.2 s (~18,200 rows/s) | **295.8 s (~32,800 rows/s)** | 1.81x | 915 MB |
| 10,000,000 | SQLite | — (new mode) | **335.1 s (~29,000 rows/s)** | — | 978 MB |

Database-mode runs exclude the one-off CSV→SQLite load (7.6 s at 1M
rows, 77.0 s at 10M). Output is **byte-identical** before/after at 1M
and 10M rows (same md5), and CSV mode vs database mode produce
byte-identical reports at both scales.

What changed in the Nov 2026 optimisation (this round):
- **Classifier** — keyword matching moved from Python-level `any()`
  loops to one compiled regex per category/sentiment/priority group
  (same priority order, same decisions — verified identical on all 160
  synthetic subject/description combinations; `benchmark.py --verify`).
- **Prompt rendering skipped in the hot path** — the pipeline discarded
  the rendered classification prompt, but `analyse_record()` still built
  it (a regex substitution over the ~750-char template per record).
  `analyse_record(..., render=False)` skips it; the enriched record is
  identical.
- **Normalisation** — `re.sub(r"\s+", " ", ...)` replaced with
  `" ".join(value.split())`, and the per-value strip in the extract
  stage removed (the cleaner normalises every field anyway).
- **Benchmark generator** — no longer builds a list of all N ticket ids
  (O(N) memory, O(N) per duplicate draw); ids are formatted on the fly.
  Generation is byte-identical for the same seed and 2.6x faster
  (200k rows: 2.1 s → 0.8 s); 10M rows generate in ~52 s.
- **Database mode** — new `--db` flag (SQLite): read from a table, write
  the report + run log to tables. See "Running against a database".

Honest notes:
- Memory at 10M rows is dominated by the dedup hash set (~90 bytes per
  unique key): 907 MB before, 915 MB after. Streaming keeps everything
  else flat, but the set itself grows with unique keys — at ~100M rows
  it would need ~9 GB, beyond this host. That is the known scaling
  ceiling; a disk-backed dedup (e.g. SQLite) would be the next step if
  that scale is ever needed, at the cost of speed. It was deliberately
  not implemented here: the in-memory set fits the 10M scale asked for.
- Database mode is ~5% slower than CSV at 1M rows and ~13% slower at
  10M, and uses ~70 MB more RAM (page cache). The trade-off buys
  queryable tables instead of flat files.
- With the lightweight local classifier the pipeline is CPU-bound on
  this host, so single-process mode (`--workers 1`) is fastest here.
  The parallel stage earns its keep in production, where enrichment is
  an LLM API call — the genuinely expensive step.
- The 1 Nov 2026 numbers in the previous version of this section
  (~28k rows/s, 1M rows in ~35s, ~210 MB) were measured on a different,
  less loaded host; the table above is the current same-host comparison.
  The 1M "before" figure here (~29,800 rows/s) happens to match it,
  which is a coincidence of host load, not a reproduced measurement.

## Importing the workflow into n8n

1. Self-host n8n (see `deploy/`) or use n8n Cloud.
2. **Workflows → Import from File** → select `n8n/workflow.json`.
3. Add credentials:
   - `OpenAI account` — API key for the AI Classification node.
   - `Google Sheets account` — OAuth2, and set your sheet ID in the
     **Log to Spreadsheet** node (replace `YOUR_GOOGLE_SHEET_ID`).
   - Tickets API — bearer token for the **Pull Ticket Data** HTTP node;
     point it at your real source and adjust field mappings in
     **Normalise Fields**.
4. Review/activate — the Schedule Trigger runs the pipeline hourly.

The workflow is exported inactive (`"active": false`) so importing it
never starts executions before credentials are configured.

## EC2 deployment (summary)

Full step-by-step guide: [`deploy/ec2-setup.md`](deploy/ec2-setup.md).

1. Launch an Ubuntu 22.04 **t3.micro** EC2 instance; open ports 22 and 5678.
2. Install Docker + the compose plugin.
3. Clone this repo, create a `.env` with `N8N_BASIC_AUTH_PASSWORD` and
   `OPENAI_API_KEY`.
4. `docker compose -f deploy/docker-compose.yml up -d --build`.
5. Open `http://<EC2_PUBLIC_IP>:5678`, import `n8n/workflow.json`,
   add credentials, activate.

## Customising the prompts

The prompts are the "brains" of the system. Edit `pipeline/prompts.yaml`
to change behaviour — no code changes needed:

| Template | Purpose |
|---|---|
| `extraction` | Pull structured fields out of free-form text |
| `classification` | Tag category / sentiment / priority / action_required |
| `summarisation` | One-line operational summary per record |
| `validation` | QA self-check before a record is committed |

Rules for editing:

- Keep the `{{field_name}}` placeholders exactly as written — the pipeline
  (and the n8n node) substitutes them per record.
- The local classifier in `ai_prompts.py` implements the same decision
  rules as the `classification` prompt; if you change the allowed values
  or priority rules, update `_classify_text()` to match.
- In production, the n8n **AI Classification** node holds a copy of the
  classification prompt — keep it in sync with `prompts.yaml`.

## Tech stack

Python · n8n · Custom AI Prompts · AWS EC2 · Docker · Google Sheets
