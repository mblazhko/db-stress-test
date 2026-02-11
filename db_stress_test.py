import argparse
import csv
import io
import json
import os
import platform
import re
import resource
import time
import tracemalloc
from dataclasses import dataclass
from datetime import datetime, timezone
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
class TestConfig:
    dsn: str
    table: str
    payload_format: str
    start_epochs: int
    end_epochs: int
    step_epochs: int
    drop_table: bool
    truncate_before_start: bool
    truncate_between_iterations: bool
    statement_timeout_ms: int
    results_csv: str
    report_md: str


@dataclass
class Dependencies:
    psycopg: Any
    bson_cls: Any | None
    jsonb_wrapper: Any | None
    zstd_compressor: Any | None
    zstd_decompressor: Any | None


@dataclass
class Measurement:
    elapsed_s: float
    peak_alloc_bytes: int
    rss_highwater_delta_bytes: int
    data: Any = None
    error: str | None = None


@dataclass
class IterationMetrics:
    iteration: int
    epochs: int
    status: str
    write_time_s: float
    read_time_s: float
    client_payload_bytes: int
    db_payload_bytes: int
    db_row_bytes: int
    db_table_bytes: int
    write_peak_alloc_bytes: int
    read_peak_alloc_bytes: int
    write_rss_delta_bytes: int
    read_rss_delta_bytes: int
    error: str = ""

    @property
    def total_time_s(self) -> float:
        return self.write_time_s + self.read_time_s

    @property
    def epochs_per_second(self) -> float:
        if self.total_time_s <= 0:
            return 0.0
        return self.epochs / self.total_time_s


class CLI:
    @staticmethod
    def parse_config() -> TestConfig:
        parser = argparse.ArgumentParser(
            description=(
                "Linear PostgreSQL stress benchmark: "
                "run iterations from start epochs to end epochs with fixed step."
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
            default="ndjson_zstd",
            help="Payload format: bson(BYTEA), jsonb(JSONB), ndjson_zstd(BYTEA).",
        )
        parser.add_argument(
            "--start-epochs",
            type=int,
            default=10_000,
            help="Start epochs per iteration.",
        )
        parser.add_argument(
            "--end-epochs",
            type=int,
            default=1_000_000,
            help="End epochs per iteration.",
        )
        parser.add_argument(
            "--step-epochs",
            type=int,
            default=10_000,
            help="Epoch step between iterations.",
        )
        parser.add_argument(
            "--drop-table",
            action="store_true",
            help="DROP + CREATE target table before benchmark.",
        )
        parser.add_argument(
            "--truncate-before-start",
            action="store_true",
            help="TRUNCATE table before first iteration.",
        )
        parser.add_argument(
            "--keep-data-between-iterations",
            action="store_true",
            help="Keep rows between iterations (default is truncate each iteration).",
        )
        parser.add_argument(
            "--statement-timeout-ms",
            type=int,
            default=0,
            help="Per-session statement_timeout in ms (0 disables timeout).",
        )
        parser.add_argument(
            "--results-csv",
            default="iteration_results.csv",
            help="CSV file for full iteration metrics table.",
        )
        parser.add_argument(
            "--report-md",
            default="stress_test_report.md",
            help="Markdown report file path.",
        )
        args = parser.parse_args()
        CLI._validate(parser, args)
        return TestConfig(
            dsn=args.dsn,
            table=args.table,
            payload_format=args.payload_format,
            start_epochs=args.start_epochs,
            end_epochs=args.end_epochs,
            step_epochs=args.step_epochs,
            drop_table=args.drop_table,
            truncate_before_start=args.truncate_before_start,
            truncate_between_iterations=not args.keep_data_between_iterations,
            statement_timeout_ms=args.statement_timeout_ms,
            results_csv=args.results_csv,
            report_md=args.report_md,
        )

    @staticmethod
    def _validate(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
        if not args.dsn:
            parser.error("You must pass --dsn or set PG_DSN.")
        if not IDENTIFIER_RE.match(args.table):
            parser.error("Invalid table name. Use [A-Za-z_][A-Za-z0-9_]*.")
        if args.start_epochs < 1:
            parser.error("--start-epochs must be > 0.")
        if args.end_epochs < args.start_epochs:
            parser.error("--end-epochs must be >= --start-epochs.")
        if args.step_epochs < 1:
            parser.error("--step-epochs must be > 0.")
        if args.statement_timeout_ms < 0:
            parser.error("--statement-timeout-ms must be >= 0.")
        if not args.results_csv:
            parser.error("--results-csv cannot be empty.")
        if not args.report_md:
            parser.error("--report-md cannot be empty.")


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

        bson_cls = None
        jsonb_wrapper = None
        zstd_compressor = None
        zstd_decompressor = None

        if self.payload_format == "bson":
            try:
                from bson import BSON
            except ImportError as exc:
                raise SystemExit("bson is not installed. Install with: pip install pymongo") from exc
            bson_cls = BSON
        elif self.payload_format == "jsonb":
            from psycopg.types.json import Jsonb
            jsonb_wrapper = Jsonb
        elif self.payload_format == "ndjson_zstd":
            try:
                import zstandard as zstd
            except ImportError as exc:
                raise SystemExit("zstandard is not installed. Install with: pip install zstandard") from exc
            zstd_compressor = zstd.ZstdCompressor(level=3)
            zstd_decompressor = zstd.ZstdDecompressor()

        return Dependencies(
            psycopg=psycopg,
            bson_cls=bson_cls,
            jsonb_wrapper=jsonb_wrapper,
            zstd_compressor=zstd_compressor,
            zstd_decompressor=zstd_decompressor,
        )


class MetricsUtils:
    @staticmethod
    def current_rss_highwater_bytes() -> int:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == "Darwin":
            return int(usage)
        return int(usage * 1024)

    @staticmethod
    def format_bytes(raw_bytes: int) -> str:
        value = float(raw_bytes)
        units = ["B", "KB", "MB", "GB", "TB"]
        for unit in units:
            if value < 1024.0:
                return f"{value:.2f} {unit}"
            value /= 1024.0
        return f"{value:.2f} PB"


class DatabaseManager:
    def __init__(self, config: TestConfig, psycopg: Any) -> None:
        self.config = config
        self.psycopg = psycopg

    def prepare_schema(self) -> None:
        payload_type = "JSONB" if self.config.payload_format == "jsonb" else "BYTEA"
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {self.config.table} "
            f"(id BIGSERIAL PRIMARY KEY, payload {payload_type} NOT NULL);"
        )
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                if self.config.drop_table:
                    cur.execute(f"DROP TABLE IF EXISTS {self.config.table};")
                cur.execute(create_sql)
                if self.config.truncate_before_start:
                    cur.execute(f"TRUNCATE TABLE {self.config.table};")
            conn.commit()

    def truncate_table(self) -> None:
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {self.config.table};")
            conn.commit()

    def open_session(self) -> tuple[Any, Any]:
        conn = self.psycopg.connect(self.config.dsn)
        cur = conn.cursor()
        if self.config.statement_timeout_ms > 0:
            cur.execute("SET statement_timeout = %s;", (self.config.statement_timeout_ms,))
            conn.commit()
        return conn, cur

    @staticmethod
    def close_session(conn: Any, cur: Any) -> None:
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

    def insert_payload(self, conn: Any, cur: Any, db_value: Any) -> int:
        cur.execute(
            f"INSERT INTO {self.config.table} (payload) VALUES (%s) RETURNING id;",
            (db_value,),
        )
        row_id = int(cur.fetchone()[0])
        conn.commit()
        return row_id

    def fetch_payload(self, conn: Any, cur: Any, row_id: int) -> Any:
        cur.execute(f"SELECT payload FROM {self.config.table} WHERE id = %s;", (row_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Row id={row_id} not found for read benchmark.")
        return row[0]

    def fetch_storage_metrics(self, row_id: int) -> tuple[int, int, int]:
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT pg_column_size(payload), pg_column_size(t) "
                    f"FROM {self.config.table} t WHERE id = %s;",
                    (row_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"Row id={row_id} not found for size metrics.")
                payload_bytes = int(row[0])
                row_bytes = int(row[1])
                cur.execute("SELECT pg_total_relation_size(%s::regclass);", (self.config.table,))
                table_bytes = int(cur.fetchone()[0])
        return payload_bytes, row_bytes, table_bytes


class PayloadCodec:
    def __init__(self, config: TestConfig, deps: Dependencies) -> None:
        self.config = config
        self.deps = deps

    def encode_for_insert(self, generator: JSONGenerator, epochs: int) -> tuple[Any, int]:
        if self.config.payload_format == "bson":
            record = generator.batch_generate_epochs(epochs)
            payload = self.deps.bson_cls.encode(record)
            record = None
            return payload, len(payload)

        if self.config.payload_format == "jsonb":
            record = generator.batch_generate_epochs(epochs)
            payload_json = json.dumps(record, separators=(",", ":"))
            size_bytes = len(payload_json.encode("utf-8"))
            db_value = self.deps.jsonb_wrapper(record)
            record = None
            return db_value, size_bytes

        return self._encode_ndjson_zstd_stream(generator, epochs)

    def decode_after_read(self, payload: Any) -> None:
        if self.config.payload_format == "bson":
            _decoded = self.deps.bson_cls(payload).decode()
            _decoded = None
            return

        if self.config.payload_format == "jsonb":
            if isinstance(payload, str):
                _decoded = json.loads(payload)
            else:
                _decoded = payload
            _decoded = None
            return

        compressed = io.BytesIO(payload)
        with self.deps.zstd_decompressor.stream_reader(compressed) as reader:
            text_reader = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text_reader:
                if line.strip():
                    json.loads(line)

    def _encode_ndjson_zstd_stream(
        self,
        generator: JSONGenerator,
        epochs: int,
    ) -> tuple[bytes, int]:
        buffer = io.BytesIO()
        with self.deps.zstd_compressor.stream_writer(buffer, closefd=False) as writer:
            for epoch_index in range(epochs):
                epoch_value = generator.generate_epoch()
                line = json.dumps(
                    {f"epoch_{epoch_index}": epoch_value},
                    separators=(",", ":"),
                ).encode("utf-8")
                writer.write(line)
                writer.write(b"\n")
        payload = buffer.getvalue()
        return payload, len(payload)


class LinearBenchmarkRunner:
    def __init__(
        self,
        config: TestConfig,
        db: DatabaseManager,
        codec: PayloadCodec,
    ) -> None:
        self.config = config
        self.db = db
        self.codec = codec

    def run(self) -> list[IterationMetrics]:
        epoch_values = self._build_epoch_values()
        results: list[IterationMetrics] = []

        for index, epochs in enumerate(epoch_values, start=1):
            print(f"[{index}/{len(epoch_values)}] epochs={epochs}")
            result = self._run_iteration(index, epochs)
            self._print_iteration_summary(result)
            results.append(result)

        return results

    def _run_iteration(self, iteration: int, epochs: int) -> IterationMetrics:
        if self.config.truncate_between_iterations:
            self.db.truncate_table()

        generator = JSONGenerator()

        def write_action() -> dict[str, Any]:
            conn, cur = self.db.open_session()
            try:
                db_value, payload_size = self.codec.encode_for_insert(generator, epochs)
                row_id = self.db.insert_payload(conn, cur, db_value)
                return {"row_id": row_id, "payload_size": payload_size}
            finally:
                self.db.close_session(conn, cur)

        write_measure = self._measure(write_action)
        if write_measure.error is not None:
            return IterationMetrics(
                iteration=iteration,
                epochs=epochs,
                status="FAIL_WRITE",
                write_time_s=write_measure.elapsed_s,
                read_time_s=0.0,
                client_payload_bytes=0,
                db_payload_bytes=0,
                db_row_bytes=0,
                db_table_bytes=0,
                write_peak_alloc_bytes=write_measure.peak_alloc_bytes,
                read_peak_alloc_bytes=0,
                write_rss_delta_bytes=write_measure.rss_highwater_delta_bytes,
                read_rss_delta_bytes=0,
                error=write_measure.error,
            )

        row_id = int(write_measure.data["row_id"])
        client_payload_bytes = int(write_measure.data["payload_size"])

        def read_action() -> bool:
            conn, cur = self.db.open_session()
            try:
                payload = self.db.fetch_payload(conn, cur, row_id)
                self.codec.decode_after_read(payload)
                return True
            finally:
                self.db.close_session(conn, cur)

        read_measure = self._measure(read_action)

        status = "OK" if read_measure.error is None else "FAIL_READ"
        error_text = read_measure.error or ""

        db_payload_bytes = 0
        db_row_bytes = 0
        db_table_bytes = 0
        try:
            db_payload_bytes, db_row_bytes, db_table_bytes = self.db.fetch_storage_metrics(row_id)
        except Exception as size_exc:
            size_error = f"{type(size_exc).__name__}: {size_exc}"
            error_text = f"{error_text} | size_metrics: {size_error}".strip(" |")
            status = "FAIL_READ" if status == "OK" else status

        return IterationMetrics(
            iteration=iteration,
            epochs=epochs,
            status=status,
            write_time_s=write_measure.elapsed_s,
            read_time_s=read_measure.elapsed_s,
            client_payload_bytes=client_payload_bytes,
            db_payload_bytes=db_payload_bytes,
            db_row_bytes=db_row_bytes,
            db_table_bytes=db_table_bytes,
            write_peak_alloc_bytes=write_measure.peak_alloc_bytes,
            read_peak_alloc_bytes=read_measure.peak_alloc_bytes,
            write_rss_delta_bytes=write_measure.rss_highwater_delta_bytes,
            read_rss_delta_bytes=read_measure.rss_highwater_delta_bytes,
            error=error_text,
        )

    @staticmethod
    def _measure(action: Callable[[], Any]) -> Measurement:
        before_rss = MetricsUtils.current_rss_highwater_bytes()
        tracemalloc.start()
        started = time.perf_counter()
        try:
            data = action()
            error = None
        except Exception as exc:
            data = None
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.perf_counter() - started
        _, peak_alloc = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        after_rss = MetricsUtils.current_rss_highwater_bytes()
        return Measurement(
            elapsed_s=elapsed,
            peak_alloc_bytes=int(peak_alloc),
            rss_highwater_delta_bytes=max(0, int(after_rss - before_rss)),
            data=data,
            error=error,
        )

    def _build_epoch_values(self) -> list[int]:
        values = list(range(self.config.start_epochs, self.config.end_epochs + 1, self.config.step_epochs))
        if not values:
            return [self.config.start_epochs]
        if values[-1] != self.config.end_epochs:
            values.append(self.config.end_epochs)
        return values

    @staticmethod
    def _print_iteration_summary(result: IterationMetrics) -> None:
        print(
            f"  status={result.status} "
            f"write={result.write_time_s:.2f}s "
            f"read={result.read_time_s:.2f}s "
            f"payload_client={MetricsUtils.format_bytes(result.client_payload_bytes)} "
            f"payload_db={MetricsUtils.format_bytes(result.db_payload_bytes)} "
            f"write_ram_peak={MetricsUtils.format_bytes(result.write_peak_alloc_bytes)} "
            f"read_ram_peak={MetricsUtils.format_bytes(result.read_peak_alloc_bytes)}"
        )
        if result.error:
            print(f"  error: {result.error}")


class ResultsReporter:
    def __init__(self, config: TestConfig) -> None:
        self.config = config

    def print_config(self) -> None:
        print("=== Configuration ===")
        print(f"Table: {self.config.table}")
        print(f"Payload format: {self.config.payload_format}")
        print(f"Epoch range: {self.config.start_epochs}..{self.config.end_epochs}")
        print(f"Step: {self.config.step_epochs}")
        print(f"Truncate between iterations: {self.config.truncate_between_iterations}")
        print(f"Statement timeout: {self.config.statement_timeout_ms} ms")
        print(f"CSV output: {self.config.results_csv}")
        print(f"Report output: {self.config.report_md}")
        print()

    def print_results_table(self, results: list[IterationMetrics]) -> None:
        if not results:
            print("No results to print.")
            return
        print("=== Results Table ===")
        print(
            "| iter | epochs | status | write_s | read_s | total_s | eps | "
            "client_payload_mb | db_payload_mb | db_row_mb | write_peak_mb | "
            "read_peak_mb | write_rss_delta_mb | read_rss_delta_mb |"
        )
        print(
            "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
        )
        for item in results:
            print(
                f"| {item.iteration} | {item.epochs} | {item.status} | "
                f"{item.write_time_s:.2f} | {item.read_time_s:.2f} | {item.total_time_s:.2f} | "
                f"{item.epochs_per_second:.2f} | "
                f"{item.client_payload_bytes / (1024 * 1024):.2f} | "
                f"{item.db_payload_bytes / (1024 * 1024):.2f} | "
                f"{item.db_row_bytes / (1024 * 1024):.2f} | "
                f"{item.write_peak_alloc_bytes / (1024 * 1024):.2f} | "
                f"{item.read_peak_alloc_bytes / (1024 * 1024):.2f} | "
                f"{item.write_rss_delta_bytes / (1024 * 1024):.2f} | "
                f"{item.read_rss_delta_bytes / (1024 * 1024):.2f} |"
            )
        print()

    def save_csv(self, results: list[IterationMetrics]) -> str:
        with open(self.config.results_csv, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "iteration",
                    "epochs",
                    "status",
                    "write_time_s",
                    "read_time_s",
                    "total_time_s",
                    "epochs_per_second",
                    "client_payload_bytes",
                    "db_payload_bytes",
                    "db_row_bytes",
                    "db_table_bytes",
                    "write_peak_alloc_bytes",
                    "read_peak_alloc_bytes",
                    "write_rss_delta_bytes",
                    "read_rss_delta_bytes",
                    "error",
                ]
            )
            for item in results:
                writer.writerow(
                    [
                        item.iteration,
                        item.epochs,
                        item.status,
                        f"{item.write_time_s:.6f}",
                        f"{item.read_time_s:.6f}",
                        f"{item.total_time_s:.6f}",
                        f"{item.epochs_per_second:.6f}",
                        item.client_payload_bytes,
                        item.db_payload_bytes,
                        item.db_row_bytes,
                        item.db_table_bytes,
                        item.write_peak_alloc_bytes,
                        item.read_peak_alloc_bytes,
                        item.write_rss_delta_bytes,
                        item.read_rss_delta_bytes,
                        item.error,
                    ]
                )
        return self.config.results_csv

    def save_report(self, results: list[IterationMetrics]) -> str:
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
        first_fail = next((item for item in results if item.status != "OK"), None)
        if first_fail is None:
            stability_point = max((item.epochs for item in results if item.status == "OK"), default=None)
            breaking_point = None
        else:
            stability_point = max(
                (item.epochs for item in results if item.status == "OK" and item.epochs < first_fail.epochs),
                default=None,
            )
            breaking_point = first_fail.epochs

        best_efficiency = max(
            (item for item in results if item.status == "OK"),
            key=lambda row: row.epochs_per_second,
            default=None,
        )

        lines: list[str] = []
        lines.append("# Stress Test Report")
        lines.append("")
        lines.append(f"Generated at (UTC): {generated_at}")
        lines.append("")
        lines.append("## Configuration")
        lines.append("")
        lines.append(f"- Payload format: `{self.config.payload_format}`")
        lines.append(f"- Epoch range: `{self.config.start_epochs}..{self.config.end_epochs}`")
        lines.append(f"- Step: `{self.config.step_epochs}`")
        lines.append(f"- Table: `{self.config.table}`")
        lines.append("")
        lines.append("## Breaking Point And Stability Point")
        lines.append("")
        lines.append(
            f"- Breaking Point: `{breaking_point}`" if breaking_point is not None
            else "- Breaking Point: not reached in tested range."
        )
        lines.append(
            f"- Stability Point: `{stability_point}`" if stability_point is not None
            else "- Stability Point: not confirmed."
        )
        if stability_point is not None:
            lines.append(
                f"- Conclusion: stable write/read behavior is confirmed up to `{stability_point}` epochs."
            )
        else:
            lines.append("- Conclusion: increase retries/resources and re-run to confirm stability.")
        if best_efficiency is not None:
            lines.append(
                "- Optimal efficiency point: "
                f"`{best_efficiency.epochs}` epochs "
                f"({best_efficiency.epochs_per_second:.2f} epochs/sec)."
            )
        lines.append("")
        lines.append("## Comparison Table (All Iterations)")
        lines.append("")
        lines.append(
            "| iter | epochs | status | write_s | read_s | total_s | eps | "
            "client_payload_mb | db_payload_mb | db_row_mb | db_table_mb | "
            "write_peak_mb | read_peak_mb | write_rss_delta_mb | read_rss_delta_mb | error |"
        )
        lines.append(
            "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
        )
        for item in results:
            lines.append(
                f"| {item.iteration} | {item.epochs} | {item.status} | "
                f"{item.write_time_s:.2f} | {item.read_time_s:.2f} | {item.total_time_s:.2f} | "
                f"{item.epochs_per_second:.2f} | "
                f"{item.client_payload_bytes / (1024 * 1024):.2f} | "
                f"{item.db_payload_bytes / (1024 * 1024):.2f} | "
                f"{item.db_row_bytes / (1024 * 1024):.2f} | "
                f"{item.db_table_bytes / (1024 * 1024):.2f} | "
                f"{item.write_peak_alloc_bytes / (1024 * 1024):.2f} | "
                f"{item.read_peak_alloc_bytes / (1024 * 1024):.2f} | "
                f"{item.write_rss_delta_bytes / (1024 * 1024):.2f} | "
                f"{item.read_rss_delta_bytes / (1024 * 1024):.2f} | "
                f"{item.error.replace('|', '/').replace(chr(10), ' ')} |"
            )
        lines.append("")

        with open(self.config.report_md, "w", encoding="utf-8") as file:
            file.write("\n".join(lines))
        return self.config.report_md


class App:
    def __init__(self, config: TestConfig) -> None:
        self.config = config
        self.deps = DependencyProvider(config.payload_format).load()
        self.db = DatabaseManager(config, self.deps.psycopg)
        self.codec = PayloadCodec(config, self.deps)
        self.runner = LinearBenchmarkRunner(config, self.db, self.codec)
        self.reporter = ResultsReporter(config)

    def run(self) -> None:
        self.reporter.print_config()
        self.db.prepare_schema()
        results = self.runner.run()
        self.reporter.print_results_table(results)
        csv_path = self.reporter.save_csv(results)
        report_path = self.reporter.save_report(results)
        print(f"CSV saved: {csv_path}")
        print(f"Report saved: {report_path}")


def main() -> None:
    config = CLI.parse_config()
    app = App(config)
    app.run()


if __name__ == "__main__":
    main()
