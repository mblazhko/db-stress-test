# PostgreSQL Adaptive Stress Test (JSON -> BSON/JSONB/NDJSON+ZSTD)

This project stress-tests PostgreSQL inserts where each row stores one large payload:
- `id` (BIGSERIAL)
- `payload` (`BYTEA` for BSON/NDJSON+ZSTD or `JSONB` for JSONB mode)

The payload is generated from the existing `JSONGenerator` and can contain millions of epochs.

## Adaptive Logic

The test automatically changes `epochs per record` after each attempt:
- Success: `epochs = epochs * success_multiplier` (default `x10`)
- Failure: `epochs = epochs / failure_divider` (default `/2`)

Goal: find the maximum practical epoch count for one DB column value.

The same logic is available for:
- single-record mode (`single`)
- parallel batch mode (`parallel`)
- both modes in one run (`both`)

Optionally, the tool refines the final maximum using binary search between:
- highest successful epoch count
- smallest failed epoch count above that success

## Requirements

- Python 3.10+
- PostgreSQL
- Python packages:
  - `psycopg[binary]`
  - `pymongo` (required for BSON mode)
  - `zstandard` (required for NDJSON+ZSTD mode)
  - `python-dotenv` (optional, for loading `.env`)

Install:

```bash
pip install -r requirements.txt
```

## PostgreSQL DSN

Use `PG_DSN` or pass `--dsn`.

Format:

```text
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

Example:

```bash
export PG_DSN='postgresql://postgres:postgres@localhost:5432/stress_db'
```

## Quick Start

Run both modes from 1,000,000 epochs:

```bash
./.venv/bin/python db_stress_test.py \
  --mode both \
  --payload-format bson \
  --start-epochs 1000000 \
  --parallel-records 100 \
  --workers 8 \
  --drop-table \
  --confirm-heavy-run
```

## Test Modes

### 1) Single Mode (`--mode single`)

Find max epochs for one record in one insert attempt.

```bash
./.venv/bin/python db_stress_test.py \
  --mode single \
  --payload-format bson \
  --start-epochs 1000000 \
  --max-attempts 20 \
  --max-refine-attempts 25 \
  --truncate \
  --confirm-heavy-run
```

### 2) Parallel Mode (`--mode parallel`)

Find max epochs when inserting multiple records in parallel for each attempt.

```bash
./.venv/bin/python db_stress_test.py \
  --mode parallel \
  --payload-format bson \
  --start-epochs 1000000 \
  --parallel-records 100 \
  --workers 8 \
  --max-attempts 20 \
  --max-refine-attempts 25 \
  --truncate \
  --confirm-heavy-run
```

### 3) Both Modes (`--mode both`)

Runs `single` first, then `parallel` in the same execution.

```bash
./.venv/bin/python db_stress_test.py \
  --mode both \
  --payload-format bson \
  --start-epochs 1000000 \
  --parallel-records 100 \
  --workers 8 \
  --truncate \
  --confirm-heavy-run
```

## JSONB Mode

Switch from BSON to JSONB storage:

```bash
./.venv/bin/python db_stress_test.py \
  --mode both \
  --payload-format jsonb \
  --start-epochs 1000000 \
  --parallel-records 100 \
  --workers 8 \
  --confirm-heavy-run
```

## NDJSON + ZSTD Mode

Store payload as newline-delimited JSON compressed with Zstandard (`BYTEA`).

```bash
./.venv/bin/python db_stress_test.py \
  --mode both \
  --payload-format ndjson_zstd \
  --start-epochs 1000000 \
  --parallel-records 100 \
  --workers 8 \
  --confirm-heavy-run
```

## Key Options

- `--success-multiplier` default: `10`
- `--failure-divider` default: `2`
- `--max-attempts` default: `20`
- `--max-refine-attempts` default: `25`
- `--max-epochs-cap` default: `10000000000`
- `--statement-timeout-ms` default: `0` (disabled)
- `--connection-retries` default: `2` (only for connection-lost OperationalError)
- `--attempt-delay-seconds` default: `0.0` (pause between attempts)
- `--no-refine-max` disable binary refinement
- `--keep-data-between-attempts` keep rows between attempts
- `--allow-submillion` allow start epochs below 1,000,000 (dry runs only)

## Output

For each attempt, the tool prints:
- status (`OK`/`FAIL`)
- epoch count
- records inserted/attempted
- timing metrics (`wall`, `gen`, `enc`, `ins`)
- payload size
- throughput (`rps`)
- latency percentile (`p95`)
- memory watermark (`rss`)

At the end of each mode:
- highest successful epoch count
- nearest failure above that success
- first few errors (if any)

At the end of the run:
- table row count
- table size
- process RSS

## Reading Attempt Log Lines

Example lines:

```text
[single:adaptive] #1 OK epochs=1000000 records=1/1 wall=14.17s gen=1.44s enc=0.77s ins=11.80s payload=388.99 MB rps=0.0706 p95=14.01s rss=2.37 GB
[single:adaptive] #2 FAIL epochs=10000000 records=0/1 wall=63.80s gen=53.61s enc=0.00s ins=0.00s payload=0.00 B rps=0.0000 p95=0.00s rss=6.68 GB
  error: ValueError: Document would overflow BSON size limit
[single:adaptive] #3 FAIL epochs=5000000 records=0/1 wall=18.17s gen=8.38s enc=7.01s ins=0.00s payload=1.90 GB rps=0.0000 p95=0.00s rss=7.25 GB
  error: OperationalError: the connection is lost
```

Field meanings:
- `[single:adaptive]`: `single` is the mode, `adaptive` is the search phase (`adaptive` or `refine`).
- `#1`: attempt number inside that mode.
- `OK`/`FAIL`: whole-attempt status.
- `epochs=...`: epochs generated per record for this attempt.
- `records=success/total`: successful inserts out of attempted records.
- `wall=...`: full wall-clock time for the attempt.
- `gen=...`: time spent generating JSON structure.
- `enc=...`: time spent encoding payload (BSON, JSONB prep, or NDJSON+ZSTD compression).
- `ins=...`: time spent executing DB insert/commit.
- `payload=...`: encoded payload size for successful encoding path.
- `rps=...`: successful records per second (`success_records / wall`).
- `p95=...`: p95 latency over successful record latencies in this attempt.
- `rss=...`: process memory high-water mark (resident set size).
- `error: ...`: first captured error for the attempt (printed only on failures).

How to read your sample:
- Attempt `#1` succeeded at `1,000,000` epochs; insert time (`ins=11.80s`) dominated runtime.
- Attempt `#2` failed at `10,000,000` epochs with BSON document size overflow; no successful encode/insert timings were recorded.
- Attempt `#3` failed at `5,000,000` epochs after generation/encoding (`payload=1.90 GB`) due to lost DB connection before successful insert.

Retry behavior for `OperationalError: the connection is lost`:
- The tool reconnects and retries insert automatically (default up to `2` retries).
- Retries are only applied to connection-loss errors, not generic encoding/size errors.
- Change retry count with `--connection-retries N`.

Cooldown between attempts:
- To reduce pressure on PostgreSQL, add a pause between adaptive/refine attempts.
- Use `--attempt-delay-seconds N` (for example, `--attempt-delay-seconds 5`).

NDJSON+ZSTD payload notes:
- Each epoch is serialized as one NDJSON line and then compressed with Zstandard.
- Stored in PostgreSQL as `BYTEA`.
- This format is useful when JSONB size limits are hit and better compression is needed.

## Notes

- Very large payloads can exhaust memory on both client and PostgreSQL sides.
- Start with smaller worker counts (`2 -> 4 -> 8`) before high parallelism.
- Use `--statement-timeout-ms` to prevent very long blocking statements.
