# PostgreSQL Stress Test (Separated Sync/Async Modes)

This benchmark runs **linear iterations** from a start epoch count to an end epoch count
with a fixed step (default: `10,000 -> 1,000,000`, step `10,000`).

For each iteration it measures:
- write time (generate + encode + insert)
- read time (select + decode)
- payload size in client memory
- payload/row/table size inside PostgreSQL
- operational memory (Python allocation peak + RSS high-water delta)

Then it builds:
- a full comparison table for **all** iterations
- CSV with all metrics
- formal markdown report with breaking/stability conclusions

Async scenario:
- run asynchronous fanout with a single configured concurrency value (for example `100`)
- run the same linear epoch ramp as sync mode (`start -> end` with `step`)
- write many records concurrently (same payload size per record)
- read them back concurrently
- capture async throughput and memory metrics

Modes are separated:
- `--mode sync`: only linear sync benchmark
- `--mode async`: only async fanout benchmark

## Requirements

- Python 3.10+
- PostgreSQL
- dependencies from `requirements.txt`

Install:

```bash
pip install -r requirements.txt
```

## PostgreSQL DSN

You can pass `--dsn` or set `PG_DSN`.

Example:

```bash
export PG_DSN='postgresql://postgres:postgres@localhost:5432/stress_db'
```

## Default Run (10k -> 1M)

```bash
./.venv/bin/python main.py \
  --mode sync \
  --payload-format ndjson_zstd \
  --start-epochs 10000 \
  --end-epochs 1000000 \
  --step-epochs 10000 \
  --drop-table \
  --truncate-before-start
```

## Async Fanout Run (single concurrency value)

```bash
./.venv/bin/python main.py \
  --mode async \
  --payload-format ndjson_zstd \
  --start-epochs 10000 \
  --end-epochs 1000000 \
  --step-epochs 10000 \
  --drop-table \
  --truncate-before-start \
  --async-concurrency 100
```

## Payload Formats

- `bson` -> stored as `BYTEA`
- `jsonb` -> stored as `JSONB`
- `ndjson_zstd` -> stored as compressed `BYTEA` (best memory behavior in this project)

## Key Options

- `--start-epochs` first iteration size (`> 0`)
- `--end-epochs` last iteration size
- `--step-epochs` increment between iterations
- `--mode {sync,async}` choose benchmark mode
- `--payload-format {bson,jsonb,ndjson_zstd}`
- `--drop-table` recreate table before run
- `--truncate-before-start` clean table before first iteration
- `--keep-data-between-iterations` do not truncate before each iteration
- `--statement-timeout-ms` PostgreSQL statement timeout per session
- `--results-csv` output CSV path (default `iteration_results.csv`)
- `--async-concurrency` async simultaneous request count (single integer, for async mode)
- `--async-results-csv` async CSV output path (default `async_fanout_results.csv`)
- `--report-md` final markdown report path (default `stress_test_report.md`)

## Output Metrics Meaning

For every iteration, the table includes:
- `write_s`: full write stage time
- `read_s`: full read stage time
- `total_s`: `write_s + read_s`
- `eps`: epochs per second (`epochs / total_s`)
- `client_payload_mb`: payload size before insert
- `db_payload_mb`: `pg_column_size(payload)`
- `db_row_mb`: `pg_column_size(row)`
- `write_peak_mb` / `read_peak_mb`: Python allocation peaks (`tracemalloc`)
- `write_rss_delta_mb` / `read_rss_delta_mb`: RSS high-water increases (`ru_maxrss`)

## Breaking Point / Stability Point

Report logic:
- **Breaking Point**: first iteration where status is not `OK`
- **Stability Point**: largest successful iteration before breaking point
- **Optimal efficiency point**: successful iteration with max `epochs/sec`

## Generated Files

After run:
- CSV table: `iteration_results.csv` (or your `--results-csv`)
- Async CSV table: `async_fanout_results.csv` (or your `--async-results-csv`, in `--mode async`)
- Formal report: `stress_test_report.md` (or your `--report-md`)

The report contains:
- full configuration
- breaking/stability points
- final conclusion
- complete comparison table for all iterations
- async fanout comparison table (in `--mode async`)

## Async Metrics Meaning

Async fanout table columns:
- `iter`: async iteration index in the linear epoch ramp
- `concurrency`: number of simultaneous async tasks/connections for this level
- `epochs`: epochs inside each inserted record
- `write_s` / `read_s`: total async phase time for the whole concurrent batch (all tasks together)
- `write_rps`: successful writes per second (`success_writes / write_s`)
- `read_rps`: successful reads per second (`success_reads / read_s`)
- `writes_ok/total`: successful async writes vs total write attempts
- `reads_ok/total`: successful async reads vs total read attempts
- `payload_total_mb`: total client payload size for the whole concurrent batch
- `payload_per_record_mb`: payload size of one record
- `db_table_mb`: total PostgreSQL table size after async level run
- `write_peak_mb` / `read_peak_mb`: process memory allocation peak for the whole async phase
- `write_rss_delta_mb` / `read_rss_delta_mb`: process RSS high-water growth for the whole async phase

## Memory Note

`ndjson_zstd` is implemented with streaming generation/compression and usually uses much less RAM than huge single-object `bson/jsonb` payload building.
