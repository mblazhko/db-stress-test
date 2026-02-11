# PostgreSQL Linear Stress Test (10k -> 1M Epochs)

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
./.venv/bin/python db_stress_test.py \
  --payload-format ndjson_zstd \
  --start-epochs 10000 \
  --end-epochs 1000000 \
  --step-epochs 10000 \
  --drop-table \
  --truncate-before-start
```

## Payload Formats

- `bson` -> stored as `BYTEA`
- `jsonb` -> stored as `JSONB`
- `ndjson_zstd` -> stored as compressed `BYTEA` (best memory behavior in this project)

## Key Options

- `--start-epochs` first iteration size (`> 0`)
- `--end-epochs` last iteration size
- `--step-epochs` increment between iterations
- `--payload-format {bson,jsonb,ndjson_zstd}`
- `--drop-table` recreate table before run
- `--truncate-before-start` clean table before first iteration
- `--keep-data-between-iterations` do not truncate before each iteration
- `--statement-timeout-ms` PostgreSQL statement timeout per session
- `--results-csv` output CSV path (default `iteration_results.csv`)
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
- Formal report: `stress_test_report.md` (or your `--report-md`)

The report contains:
- full configuration
- breaking/stability points
- final conclusion
- complete comparison table for all iterations

## Memory Note

`ndjson_zstd` is implemented with streaming generation/compression and usually uses much less RAM than huge single-object `bson/jsonb` payload building.
