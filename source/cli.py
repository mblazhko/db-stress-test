import argparse
import os
import re

from source.entities import TestConfig

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False


load_dotenv()

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class CLI:
    @staticmethod
    def parse_config() -> TestConfig:
        parser = argparse.ArgumentParser(
            description=(
                "PostgreSQL stress benchmark with separated modes: "
                "sync linear iterations or async fanout."
            )
        )
        parser.add_argument(
            "--mode",
            choices=("sync", "async"),
            default="sync",
            help="Execution mode: sync (linear iterations) or async (single fanout level).",
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
            help="Start epochs per iteration (used in both modes).",
        )
        parser.add_argument(
            "--end-epochs",
            type=int,
            default=1_000_000,
            help="End epochs per iteration (used in both modes).",
        )
        parser.add_argument(
            "--step-epochs",
            type=int,
            default=10_000,
            help="Epoch step between iterations (used in both modes).",
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
            "--async-results-csv",
            default="async_fanout_results.csv",
            help="CSV file for async fanout metrics table.",
        )
        parser.add_argument(
            "--report-md",
            default="stress_test_report.md",
            help="Markdown report file path.",
        )
        parser.add_argument(
            "--async-concurrency",
            type=int,
            default=10,
            help="Number of simultaneous async requests/connections.",
        )
        args = parser.parse_args()
        CLI._validate(parser, args)
        return TestConfig(
            mode=args.mode,
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
            async_results_csv=args.async_results_csv,
            report_md=args.report_md,
            async_concurrency=args.async_concurrency,
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
        if not args.async_results_csv:
            parser.error("--async-results-csv cannot be empty.")
        if not args.report_md:
            parser.error("--report-md cannot be empty.")
        if args.async_concurrency < 1:
            parser.error("--async-concurrency must be > 0.")
