import argparse
import io
import json
import os
import platform
import re
import resource
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable

from json_generator import JSONGenerator

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False


load_dotenv()

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class StressConfig:
    dsn: str
    table: str
    payload_format: str
    mode: str
    start_epochs: int
    success_multiplier: int
    failure_divider: int
    max_attempts: int
    max_refine_attempts: int
    max_epochs_cap: int
    workers: int
    parallel_records: int
    truncate: bool
    drop_table: bool
    truncate_between_attempts: bool
    statement_timeout_ms: int
    refine_max: bool
    connection_retries: int
    attempt_delay_seconds: float


@dataclass
class Dependencies:
    psycopg: Any
    bson_encoder: Any | None
    jsonb_wrapper: Any | None
    zstd_compressor: Any | None


@dataclass
class WorkerResult:
    success_count: int = 0
    failure_count: int = 0
    generation_s: float = 0.0
    encode_s: float = 0.0
    insert_s: float = 0.0
    total_s: float = 0.0
    payload_bytes: int = 0
    latencies_s: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class AttemptResult:
    mode: str
    attempt_no: int
    epochs: int
    success: bool
    success_records: int
    failure_records: int
    generation_s: float
    encode_s: float
    insert_s: float
    wall_s: float
    payload_bytes: int
    latencies_s: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    max_rss_bytes: int = 0


@dataclass
class SearchOutcome:
    mode: str
    attempts: list[AttemptResult]
    best_success: AttemptResult | None
    smallest_failed_above_best: int | None


class CLI:
    @staticmethod
    def parse_config() -> StressConfig:
        parser = argparse.ArgumentParser(
            description=(
                "Adaptive PostgreSQL stress test: "
                "on success multiply epochs, on failure divide epochs."
            )
        )
        parser.add_argument(
            "--dsn",
            default=os.getenv("PG_DSN"),
            help="PostgreSQL DSN (or set PG_DSN env variable).",
        )
        parser.add_argument(
            "--table",
            default="stress_records",
            help="Target table name.",
        )
        parser.add_argument(
            "--payload-format",
            choices=("bson", "jsonb", "ndjson_zstd"),
            default="bson",
            help=(
                "Payload type stored in one DB column: "
                "bson(BYTEA), jsonb(JSONB), or ndjson_zstd(BYTEA)."
            ),
        )
        parser.add_argument(
            "--mode",
            choices=("single", "parallel", "both"),
            default="both",
            help="Test mode: single record, parallel batch, or both.",
        )
        parser.add_argument(
            "--start-epochs",
            type=int,
            default=1_000_000,
            help="Initial number of epochs per record.",
        )
        parser.add_argument(
            "--success-multiplier",
            type=int,
            default=10,
            help="Epoch multiplier after a successful attempt.",
        )
        parser.add_argument(
            "--failure-divider",
            type=int,
            default=2,
            help="Epoch divider after a failed attempt.",
        )
        parser.add_argument(
            "--max-attempts",
            type=int,
            default=20,
            help="Maximum adaptive attempts per mode.",
        )
        parser.add_argument(
            "--max-refine-attempts",
            type=int,
            default=25,
            help="Maximum refinement attempts (binary search) per mode.",
        )
        parser.add_argument(
            "--max-epochs-cap",
            type=int,
            default=10_000_000_000,
            help="Safety cap for epochs growth.",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=min(8, os.cpu_count() or 4),
            help="Worker count for parallel mode.",
        )
        parser.add_argument(
            "--parallel-records",
            type=int,
            default=100,
            help="Records per one parallel attempt.",
        )
        parser.add_argument(
            "--truncate",
            action="store_true",
            help="TRUNCATE table before test start.",
        )
        parser.add_argument(
            "--drop-table",
            action="store_true",
            help="DROP + CREATE target table before test start.",
        )
        parser.add_argument(
            "--keep-data-between-attempts",
            action="store_true",
            help="Do not truncate data between adaptive attempts.",
        )
        parser.add_argument(
            "--statement-timeout-ms",
            type=int,
            default=0,
            help="Per-session statement_timeout in ms (0 disables timeout).",
        )
        parser.add_argument(
            "--no-refine-max",
            action="store_true",
            help="Disable binary refinement between best success and nearest failure.",
        )
        parser.add_argument(
            "--confirm-heavy-run",
            action="store_true",
            help="Explicit confirmation for heavy workload runs.",
        )
        parser.add_argument(
            "--connection-retries",
            type=int,
            default=5,
            help=(
                "Retry count for reconnect + insert when "
                "'OperationalError: connection is lost' occurs."
            ),
        )
        parser.add_argument(
            "--attempt-delay-seconds",
            type=float,
            default=0.0,
            help="Cooldown sleep in seconds between adaptive/refine attempts.",
        )
        args = parser.parse_args()
        CLI._validate_args(parser, args)
        return StressConfig(
            dsn=args.dsn,
            table=args.table,
            payload_format=args.payload_format,
            mode=args.mode,
            start_epochs=args.start_epochs,
            success_multiplier=args.success_multiplier,
            failure_divider=args.failure_divider,
            max_attempts=args.max_attempts,
            max_refine_attempts=args.max_refine_attempts,
            max_epochs_cap=args.max_epochs_cap,
            workers=args.workers,
            parallel_records=args.parallel_records,
            truncate=args.truncate,
            drop_table=args.drop_table,
            truncate_between_attempts=not args.keep_data_between_attempts,
            statement_timeout_ms=args.statement_timeout_ms,
            refine_max=not args.no_refine_max,
            connection_retries=args.connection_retries,
            attempt_delay_seconds=args.attempt_delay_seconds,
        )

    @staticmethod
    def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
        if not args.dsn:
            parser.error("You must pass --dsn or set PG_DSN.")
        if not IDENTIFIER_RE.match(args.table):
            parser.error("Invalid table name. Allowed pattern: [A-Za-z_][A-Za-z0-9_]*")
        if args.start_epochs < 1:
            parser.error("--start-epochs must be >= 1.")
        if args.success_multiplier < 2:
            parser.error("--success-multiplier must be >= 2.")
        if args.failure_divider < 2:
            parser.error("--failure-divider must be >= 2.")
        if args.max_attempts < 1:
            parser.error("--max-attempts must be >= 1.")
        if args.max_refine_attempts < 0:
            parser.error("--max-refine-attempts must be >= 0.")
        if args.max_epochs_cap < args.start_epochs:
            parser.error("--max-epochs-cap must be >= --start-epochs.")
        if args.parallel_records < 1:
            parser.error("--parallel-records must be >= 1.")
        if args.workers < 1:
            parser.error("--workers must be >= 1.")
        if args.connection_retries < 0:
            parser.error("--connection-retries must be >= 0.")
        if args.attempt_delay_seconds < 0:
            parser.error("--attempt-delay-seconds must be >= 0.")

        projected_parallel_epochs = args.start_epochs * args.parallel_records
        single_heavy = args.mode in ("single", "both") and args.start_epochs >= 100_000_000
        parallel_heavy = (
            args.mode in ("parallel", "both")
            and projected_parallel_epochs >= 100_000_000
        )
        if (single_heavy or parallel_heavy) and not args.confirm_heavy_run:
            parser.error(
                "Heavy run detected. Use --confirm-heavy-run to proceed explicitly."
            )


class DependencyProvider:
    def __init__(self, payload_format: str) -> None:
        self.payload_format = payload_format

    def load(self) -> Dependencies:
        try:
            import psycopg
        except ImportError as exc:
            raise SystemExit(
                "psycopg is not installed. Install with: pip install 'psycopg[binary]'"
            ) from exc

        bson_encoder = None
        jsonb_wrapper = None
        zstd_compressor = None
        if self.payload_format == "bson":
            try:
                from bson import BSON
            except ImportError as exc:
                raise SystemExit(
                    "bson is not installed. Install with: pip install pymongo"
                ) from exc
            bson_encoder = BSON
        elif self.payload_format == "jsonb":
            from psycopg.types.json import Jsonb

            jsonb_wrapper = Jsonb
        elif self.payload_format == "ndjson_zstd":
            try:
                import zstandard as zstd
            except ImportError as exc:
                raise SystemExit(
                    "zstandard is not installed. Install with: pip install zstandard"
                ) from exc
            zstd_compressor = zstd.ZstdCompressor(level=3)

        return Dependencies(
            psycopg=psycopg,
            bson_encoder=bson_encoder,
            jsonb_wrapper=jsonb_wrapper,
            zstd_compressor=zstd_compressor,
        )


class MetricsUtils:
    @staticmethod
    def percentile(values: list[float], p: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        index = (len(ordered) - 1) * p
        lower = int(index)
        upper = min(lower + 1, len(ordered) - 1)
        if lower == upper:
            return ordered[lower]
        share = index - lower
        return ordered[lower] + (ordered[upper] - ordered[lower]) * share

    @staticmethod
    def format_bytes(raw_bytes: int) -> str:
        value = float(raw_bytes)
        units = ["B", "KB", "MB", "GB", "TB"]
        for unit in units:
            if value < 1024.0:
                return f"{value:.2f} {unit}"
            value /= 1024.0
        return f"{value:.2f} PB"

    @staticmethod
    def current_max_rss_bytes() -> int:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == "Darwin":
            return int(usage)
        return int(usage * 1024)


class DatabaseManager:
    def __init__(self, config: StressConfig, psycopg: Any) -> None:
        self.config = config
        self.psycopg = psycopg

    def prepare_schema(self) -> None:
        payload_type = (
            "JSONB"
            if self.config.payload_format == "jsonb"
            else "BYTEA"
        )
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {self.config.table} "
            f"(id BIGSERIAL PRIMARY KEY, payload {payload_type} NOT NULL);"
        )
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                if self.config.drop_table:
                    cur.execute(f"DROP TABLE IF EXISTS {self.config.table};")
                cur.execute(create_sql)
                if self.config.truncate:
                    cur.execute(f"TRUNCATE TABLE {self.config.table};")
            conn.commit()

    def truncate_between_attempts(self) -> None:
        if not self.config.truncate_between_attempts:
            return
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {self.config.table};")
            conn.commit()

    def apply_statement_timeout(self, cursor: Any, conn: Any) -> None:
        if self.config.statement_timeout_ms > 0:
            cursor.execute(
                "SET statement_timeout = %s;",
                (self.config.statement_timeout_ms,),
            )
            conn.commit()

    def fetch_table_stats(self) -> tuple[int, int]:
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {self.config.table};")
                row_count = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT pg_total_relation_size(%s::regclass);",
                    (self.config.table,),
                )
                table_size = int(cur.fetchone()[0])
        return row_count, table_size


class AttemptRunner:
    def __init__(
        self,
        config: StressConfig,
        dependencies: Dependencies,
        db: DatabaseManager,
    ) -> None:
        self.config = config
        self.dependencies = dependencies
        self.db = db

    def run_single_attempt(self, attempt_no: int, epochs: int) -> AttemptResult:
        wall_start = time.perf_counter()
        generation_s = 0.0
        encode_s = 0.0
        insert_s = 0.0
        payload_bytes = 0
        errors: list[str] = []
        latencies_s: list[float] = []
        success_records = 0
        failure_records = 0
        conn = None
        cur = None
        record_obj = None
        db_value = None

        try:
            record_start = time.perf_counter()
            generator = JSONGenerator()
            generation_start = time.perf_counter()
            record_obj = generator.batch_generate_epochs(epochs)
            generation_s = time.perf_counter() - generation_start

            encode_start = time.perf_counter()
            db_value, payload_bytes = self._build_db_value(record_obj)
            encode_s = time.perf_counter() - encode_start

            conn, cur = self._open_session()
            insert_ok, insert_s, conn, cur, error_message = self._insert_with_retry(
                conn=conn,
                cur=cur,
                db_value=db_value,
                context=f"single attempt #{attempt_no}",
            )
            if insert_ok:
                success_records = 1
                latencies_s.append(time.perf_counter() - record_start)
            else:
                failure_records = 1
                if error_message:
                    errors.append(error_message)
        except Exception as exc:
            failure_records = 1
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            self._close_session(conn, cur)
            record_obj = None
            db_value = None

        return AttemptResult(
            mode="single",
            attempt_no=attempt_no,
            epochs=epochs,
            success=(success_records == 1 and failure_records == 0),
            success_records=success_records,
            failure_records=failure_records,
            generation_s=generation_s,
            encode_s=encode_s,
            insert_s=insert_s,
            wall_s=time.perf_counter() - wall_start,
            payload_bytes=payload_bytes,
            latencies_s=latencies_s,
            errors=errors,
            max_rss_bytes=MetricsUtils.current_max_rss_bytes(),
        )

    def run_parallel_attempt(self, attempt_no: int, epochs: int) -> AttemptResult:
        wall_start = time.perf_counter()
        groups = self._split_indices(
            self.config.parallel_records,
            min(self.config.workers, self.config.parallel_records),
        )

        worker_results: list[WorkerResult] = []
        with ThreadPoolExecutor(max_workers=len(groups)) as pool:
            futures = [
                pool.submit(
                    self._run_parallel_worker,
                    worker_id=worker_id,
                    record_indices=group,
                    epochs=epochs,
                )
                for worker_id, group in enumerate(groups)
            ]
            for future in futures:
                worker_results.append(future.result())

        return AttemptResult(
            mode="parallel",
            attempt_no=attempt_no,
            epochs=epochs,
            success=(
                sum(item.failure_count for item in worker_results) == 0
                and sum(item.success_count for item in worker_results)
                == self.config.parallel_records
            ),
            success_records=sum(item.success_count for item in worker_results),
            failure_records=sum(item.failure_count for item in worker_results),
            generation_s=sum(item.generation_s for item in worker_results),
            encode_s=sum(item.encode_s for item in worker_results),
            insert_s=sum(item.insert_s for item in worker_results),
            wall_s=time.perf_counter() - wall_start,
            payload_bytes=sum(item.payload_bytes for item in worker_results),
            latencies_s=[lat for item in worker_results for lat in item.latencies_s],
            errors=[error for item in worker_results for error in item.errors],
            max_rss_bytes=MetricsUtils.current_max_rss_bytes(),
        )

    def _run_parallel_worker(
        self,
        *,
        worker_id: int,
        record_indices: list[int],
        epochs: int,
    ) -> WorkerResult:
        result = WorkerResult()
        conn = None
        cur = None

        try:
            conn, cur = self._open_session()
        except Exception as exc:
            result.failure_count = len(record_indices)
            result.errors.append(
                f"worker={worker_id}: DB connection failed: {type(exc).__name__}: {exc}"
            )
            return result

        try:
            generator = JSONGenerator()
            for record_index in record_indices:
                record_start = time.perf_counter()
                try:
                    generation_start = time.perf_counter()
                    record_obj = generator.batch_generate_epochs(epochs)
                    generation_elapsed = time.perf_counter() - generation_start

                    encode_start = time.perf_counter()
                    db_value, payload_bytes = self._build_db_value(record_obj)
                    encode_elapsed = time.perf_counter() - encode_start

                    insert_ok, insert_elapsed, conn, cur, error_message = self._insert_with_retry(
                        conn=conn,
                        cur=cur,
                        db_value=db_value,
                        context=f"worker={worker_id} record={record_index}",
                    )
                    if not insert_ok:
                        result.failure_count += 1
                        if error_message:
                            result.errors.append(error_message)
                        continue

                    total_elapsed = time.perf_counter() - record_start
                    result.success_count += 1
                    result.generation_s += generation_elapsed
                    result.encode_s += encode_elapsed
                    result.insert_s += insert_elapsed
                    result.total_s += total_elapsed
                    result.payload_bytes += payload_bytes
                    result.latencies_s.append(total_elapsed)
                except Exception as exc:
                    result.failure_count += 1
                    result.errors.append(
                        f"worker={worker_id} record={record_index}: "
                        f"{type(exc).__name__}: {exc}"
                    )
        finally:
            self._close_session(conn, cur)

        return result

    def _build_db_value(self, record: dict[str, Any]) -> tuple[Any, int]:
        if self.config.payload_format == "bson":
            payload = self.dependencies.bson_encoder.encode(record)
            return payload, len(payload)

        if self.config.payload_format == "jsonb":
            payload_json = json.dumps(record, separators=(",", ":"))
            return self.dependencies.jsonb_wrapper(record), len(payload_json.encode("utf-8"))

        buffer = io.BytesIO()
        # Keep BytesIO open after the zstd stream closes so getvalue() is safe.
        with self.dependencies.zstd_compressor.stream_writer(buffer, closefd=False) as writer:
            for epoch_key, epoch_value in record.items():
                line = json.dumps(
                    {epoch_key: epoch_value},
                    separators=(",", ":"),
                ).encode("utf-8")
                writer.write(line)
                writer.write(b"\n")

        compressed_payload = buffer.getvalue()
        return compressed_payload, len(compressed_payload)

    @staticmethod
    def _split_indices(total: int, buckets_count: int) -> list[list[int]]:
        buckets = [[] for _ in range(buckets_count)]
        for index in range(total):
            buckets[index % buckets_count].append(index)
        return [bucket for bucket in buckets if bucket]

    def _open_session(self) -> tuple[Any, Any]:
        conn = self.dependencies.psycopg.connect(self.config.dsn)
        cur = conn.cursor()
        self.db.apply_statement_timeout(cur, conn)
        return conn, cur

    @staticmethod
    def _close_session(conn: Any, cur: Any) -> tuple[None, None]:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        return None, None

    @staticmethod
    def _is_connection_lost_error(exc: Exception) -> bool:
        message = str(exc).lower()
        type_name = type(exc).__name__.lower()
        if "operationalerror" not in type_name and "connection is lost" not in message:
            return False
        markers = (
            "connection is lost",
            "connection has been lost",
            "server closed the connection unexpectedly",
            "connection not open",
            "closed the connection unexpectedly",
        )
        return any(marker in message for marker in markers)

    def _insert_with_retry(
        self,
        *,
        conn: Any,
        cur: Any,
        db_value: Any,
        context: str,
    ) -> tuple[bool, float, Any, Any, str | None]:
        insert_started = time.perf_counter()
        retries_left = self.config.connection_retries

        while True:
            try:
                if conn is None or cur is None:
                    conn, cur = self._open_session()
                cur.execute(
                    f"INSERT INTO {self.config.table} (payload) VALUES (%s);",
                    (db_value,),
                )
                conn.commit()
                return True, time.perf_counter() - insert_started, conn, cur, None
            except Exception as exc:
                if conn is not None:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

                if self._is_connection_lost_error(exc) and retries_left > 0:
                    retry_no = self.config.connection_retries - retries_left + 1
                    conn, cur = self._close_session(conn, cur)
                    retries_left -= 1
                    try:
                        conn, cur = self._open_session()
                        continue
                    except Exception as reconnect_exc:
                        return (
                            False,
                            time.perf_counter() - insert_started,
                            None,
                            None,
                            f"{context}: reconnect failed on retry "
                            f"{retry_no}/{self.config.connection_retries}: "
                            f"{type(reconnect_exc).__name__}: {reconnect_exc}",
                        )

                if self._is_connection_lost_error(exc) and retries_left == 0:
                    return (
                        False,
                        time.perf_counter() - insert_started,
                        conn,
                        cur,
                        f"{context}: {type(exc).__name__}: {exc} "
                        f"(retries exhausted: {self.config.connection_retries})",
                    )

                return (
                    False,
                    time.perf_counter() - insert_started,
                    conn,
                    cur,
                    f"{context}: {type(exc).__name__}: {exc}",
                )


class ConsoleReporter:
    def __init__(self, config: StressConfig) -> None:
        self.config = config

    def print_config(self) -> None:
        print("=== Configuration ===")
        print(f"Table: {self.config.table}")
        print(f"Payload format: {self.config.payload_format}")
        print(f"Mode: {self.config.mode}")
        print(f"Start epochs: {self.config.start_epochs}")
        print(
            "Adaptive rule: "
            f"success x{self.config.success_multiplier}, "
            f"failure /{self.config.failure_divider}"
        )
        print(f"Max adaptive attempts: {self.config.max_attempts}")
        print(f"Max refine attempts: {self.config.max_refine_attempts}")
        print(f"Epoch cap: {self.config.max_epochs_cap}")
        print(f"Parallel records: {self.config.parallel_records}")
        print(f"Workers: {self.config.workers}")
        print(f"Truncate between attempts: {self.config.truncate_between_attempts}")
        print(f"Statement timeout: {self.config.statement_timeout_ms} ms")
        print(f"Connection-lost retries: {self.config.connection_retries}")
        print(f"Attempt delay: {self.config.attempt_delay_seconds:.2f}s")
        print()

    def print_attempt(self, attempt: AttemptResult, phase: str) -> None:
        total_records = attempt.success_records + attempt.failure_records
        rps = attempt.success_records / attempt.wall_s if attempt.wall_s > 0 else 0.0
        p95 = MetricsUtils.percentile(attempt.latencies_s, 0.95)
        status = "OK" if attempt.success else "FAIL"
        print(
            f"[{attempt.mode}:{phase}] "
            f"#{attempt.attempt_no} {status} "
            f"epochs={attempt.epochs} "
            f"records={attempt.success_records}/{total_records} "
            f"wall={attempt.wall_s:.2f}s "
            f"gen={attempt.generation_s:.2f}s "
            f"enc={attempt.encode_s:.2f}s "
            f"ins={attempt.insert_s:.2f}s "
            f"payload={MetricsUtils.format_bytes(attempt.payload_bytes)} "
            f"rps={rps:.4f} "
            f"p95={p95:.2f}s "
            f"rss={MetricsUtils.format_bytes(attempt.max_rss_bytes)}"
        )
        if attempt.errors:
            print(f"  error: {attempt.errors[0]}")

    def print_outcome_summary(self, outcome: SearchOutcome) -> None:
        print(f"=== {outcome.mode.upper()} Summary ===")
        print(f"Attempts: {len(outcome.attempts)}")
        if outcome.best_success is None:
            print("No successful insert attempts.")
        else:
            print(
                "Max successful epochs: "
                f"{outcome.best_success.epochs} "
                f"(payload={MetricsUtils.format_bytes(outcome.best_success.payload_bytes)}, "
                f"wall={outcome.best_success.wall_s:.2f}s)"
            )
        if outcome.smallest_failed_above_best is not None:
            print(
                "Smallest failure above max success: "
                f"{outcome.smallest_failed_above_best} epochs"
            )
        all_errors = [error for attempt in outcome.attempts for error in attempt.errors]
        if all_errors:
            print("First errors:")
            for err in all_errors[:5]:
                print(f"- {err}")
        print()

    def print_final_table_metrics(self, rows: int, table_size_bytes: int) -> None:
        print("=== Final Table Metrics ===")
        print(f"Rows in table: {rows}")
        print(f"Table size: {MetricsUtils.format_bytes(table_size_bytes)}")
        print(f"Max process RSS: {MetricsUtils.format_bytes(MetricsUtils.current_max_rss_bytes())}")


class AdaptiveSearchEngine:
    def __init__(
        self,
        config: StressConfig,
        db: DatabaseManager,
        runner: AttemptRunner,
        reporter: ConsoleReporter,
    ) -> None:
        self.config = config
        self.db = db
        self.runner = runner
        self.reporter = reporter

    def run_mode(self, mode: str) -> SearchOutcome:
        print(f"=== Running mode: {mode.upper()} ===")
        attempts: list[AttemptResult] = []
        current_epochs = self.config.start_epochs
        seen_epochs: dict[int, int] = {}
        run_attempt = self._resolve_runner(mode)

        for _ in range(self.config.max_attempts):
            attempt_no = len(attempts) + 1
            try:
                self.db.truncate_between_attempts()
            except Exception as exc:
                synthetic_fail = self._build_synthetic_failure(
                    mode=mode,
                    attempt_no=attempt_no,
                    epochs=current_epochs,
                    error=f"TRUNCATE failed: {type(exc).__name__}: {exc}",
                )
                attempts.append(synthetic_fail)
                self.reporter.print_attempt(synthetic_fail, phase="adaptive")
                break

            attempt = run_attempt(attempt_no=attempt_no, epochs=current_epochs)
            attempts.append(attempt)
            self.reporter.print_attempt(attempt, phase="adaptive")

            seen_epochs[current_epochs] = seen_epochs.get(current_epochs, 0) + 1
            next_epochs = self._next_epochs(current_epochs, attempt.success)
            if next_epochs == current_epochs:
                break
            if seen_epochs.get(next_epochs, 0) >= 2:
                break
            self._pause_between_attempts(
                phase="adaptive",
                next_attempt_no=attempt_no + 1,
            )
            current_epochs = next_epochs

        best_success = self._find_best_success(attempts)
        smallest_failed_above_best = None
        if best_success is not None:
            smallest_failed_above_best = self._find_smallest_failed_above(
                attempts,
                best_success.epochs,
            )

        if (
            self.config.refine_max
            and self.config.max_refine_attempts > 0
            and best_success is not None
            and smallest_failed_above_best is not None
            and smallest_failed_above_best - best_success.epochs > 1
        ):
            low = best_success.epochs
            high = smallest_failed_above_best
            for _ in range(self.config.max_refine_attempts):
                mid = (low + high) // 2
                if mid <= low or mid >= high:
                    break

                attempt_no = len(attempts) + 1
                try:
                    self.db.truncate_between_attempts()
                except Exception as exc:
                    synthetic_fail = self._build_synthetic_failure(
                        mode=mode,
                        attempt_no=attempt_no,
                        epochs=mid,
                        error=f"TRUNCATE failed: {type(exc).__name__}: {exc}",
                    )
                    attempts.append(synthetic_fail)
                    self.reporter.print_attempt(synthetic_fail, phase="refine")
                    break

                attempt = run_attempt(attempt_no=attempt_no, epochs=mid)
                attempts.append(attempt)
                self.reporter.print_attempt(attempt, phase="refine")
                if attempt.success:
                    low = mid
                    if mid > best_success.epochs:
                        best_success = attempt
                else:
                    high = mid

                if high - low <= 1:
                    break
                self._pause_between_attempts(
                    phase="refine",
                    next_attempt_no=attempt_no + 1,
                )

            smallest_failed_above_best = high

        print()
        return SearchOutcome(
            mode=mode,
            attempts=attempts,
            best_success=best_success,
            smallest_failed_above_best=smallest_failed_above_best,
        )

    def _resolve_runner(self, mode: str) -> Callable[..., AttemptResult]:
        if mode == "single":
            return self.runner.run_single_attempt
        return self.runner.run_parallel_attempt

    def _build_synthetic_failure(
        self,
        *,
        mode: str,
        attempt_no: int,
        epochs: int,
        error: str,
    ) -> AttemptResult:
        return AttemptResult(
            mode=mode,
            attempt_no=attempt_no,
            epochs=epochs,
            success=False,
            success_records=0,
            failure_records=1 if mode == "single" else self.config.parallel_records,
            generation_s=0.0,
            encode_s=0.0,
            insert_s=0.0,
            wall_s=0.0,
            payload_bytes=0,
            errors=[error],
            max_rss_bytes=MetricsUtils.current_max_rss_bytes(),
        )

    def _next_epochs(self, current_epochs: int, is_success: bool) -> int:
        if is_success:
            return self._safe_multiply(
                current_epochs,
                self.config.success_multiplier,
                self.config.max_epochs_cap,
            )
        return max(1, current_epochs // self.config.failure_divider)

    @staticmethod
    def _safe_multiply(value: int, multiplier: int, cap: int) -> int:
        if value > cap // multiplier:
            return cap
        return value * multiplier

    @staticmethod
    def _find_best_success(attempts: list[AttemptResult]) -> AttemptResult | None:
        successes = [attempt for attempt in attempts if attempt.success]
        if not successes:
            return None
        return max(successes, key=lambda attempt: attempt.epochs)

    @staticmethod
    def _find_smallest_failed_above(
        attempts: list[AttemptResult],
        threshold_epochs: int,
    ) -> int | None:
        failed_epochs = [
            attempt.epochs
            for attempt in attempts
            if not attempt.success and attempt.epochs > threshold_epochs
        ]
        if not failed_epochs:
            return None
        return min(failed_epochs)

    def _pause_between_attempts(self, *, phase: str, next_attempt_no: int) -> None:
        if self.config.attempt_delay_seconds <= 0:
            return
        print(
            f"Sleeping {self.config.attempt_delay_seconds:.2f}s before "
            f"{phase} attempt #{next_attempt_no}..."
        )
        time.sleep(self.config.attempt_delay_seconds)


class StressTestApp:
    def __init__(self, config: StressConfig) -> None:
        self.config = config
        self.dependencies = DependencyProvider(config.payload_format).load()
        self.db = DatabaseManager(config, self.dependencies.psycopg)
        self.reporter = ConsoleReporter(config)
        self.runner = AttemptRunner(config, self.dependencies, self.db)
        self.engine = AdaptiveSearchEngine(config, self.db, self.runner, self.reporter)

    def run(self) -> None:
        self.reporter.print_config()
        self.db.prepare_schema()

        outcomes: list[SearchOutcome] = []
        if self.config.mode in ("single", "both"):
            outcomes.append(self.engine.run_mode("single"))
        if self.config.mode in ("parallel", "both"):
            outcomes.append(self.engine.run_mode("parallel"))

        for outcome in outcomes:
            self.reporter.print_outcome_summary(outcome)

        rows, table_size = self.db.fetch_table_stats()
        self.reporter.print_final_table_metrics(rows, table_size)


def main() -> None:
    config = CLI.parse_config()
    app = StressTestApp(config)
    app.run()


if __name__ == "__main__":
    main()
