import csv
from datetime import datetime, UTC

from source.entities import TestConfig, IterationMetrics, AsyncFanoutMetrics


class ResultsReporter:
    def __init__(self, config: TestConfig) -> None:
        self.config = config

    def print_config(self) -> None:
        print("=== Configuration ===")
        print(f"Mode: {self.config.mode}")
        print(f"Table: {self.config.table}")
        print(f"Payload format: {self.config.payload_format}")
        print(f"Epoch range: {self.config.start_epochs}..{self.config.end_epochs}")
        print(f"Step: {self.config.step_epochs}")
        if self.config.mode == "async":
            print(f"Async concurrency: {self.config.async_concurrency}")
        print(f"Truncate between iterations: {self.config.truncate_between_iterations}")
        print(f"Statement timeout: {self.config.statement_timeout_ms} ms")
        print(f"CSV output: {self.config.results_csv}")
        print(f"Async CSV output: {self.config.async_results_csv}")
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
        print("|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
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

    def print_async_results_table(self, results: list[AsyncFanoutMetrics]) -> None:
        if not results:
            return
        print("=== Async Fanout Results ===")
        print(
            "| iter | concurrency | epochs | status | write_s | read_s | total_s | "
            "write_rps | read_rps | writes_ok/total | reads_ok/total | payload_total_mb | payload_per_record_mb | "
            "db_table_mb | write_peak_mb | read_peak_mb | write_rss_delta_mb | read_rss_delta_mb |"
        )
        print(
            "|---:|---:|---:|---|---:|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|"
        )
        for item in results:
            print(
                f"| {item.iteration} | {item.concurrency} | {item.epochs} | {item.status} | "
                f"{item.write_time_s:.2f} | {item.read_time_s:.2f} | {item.total_time_s:.2f} | "
                f"{item.write_records_per_second:.2f} | {item.read_records_per_second:.2f} | "
                f"{item.success_writes}/{item.success_writes + item.failed_writes} | "
                f"{item.success_reads}/{item.success_reads + item.failed_reads} | "
                f"{item.payload_bytes_total / (1024 * 1024):.2f} | "
                f"{item.payload_bytes_per_record / (1024 * 1024):.2f} | "
                f"{item.db_table_bytes / (1024 * 1024):.2f} | "
                f"{item.write_peak_alloc_bytes / (1024 * 1024):.2f} | "
                f"{item.read_peak_alloc_bytes / (1024 * 1024):.2f} | "
                f"{item.write_rss_delta_bytes / (1024 * 1024):.2f} | "
                f"{item.read_rss_delta_bytes / (1024 * 1024):.2f} |"
            )
        print()

    def save_async_csv(self, results: list[AsyncFanoutMetrics]) -> str:
        with open(
            self.config.async_results_csv, "w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "iteration",
                    "concurrency",
                    "epochs",
                    "status",
                    "write_time_s",
                    "read_time_s",
                    "total_time_s",
                    "write_records_per_second",
                    "read_records_per_second",
                    "success_writes",
                    "failed_writes",
                    "success_reads",
                    "failed_reads",
                    "payload_bytes_total",
                    "payload_bytes_per_record",
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
                        item.concurrency,
                        item.epochs,
                        item.status,
                        f"{item.write_time_s:.6f}",
                        f"{item.read_time_s:.6f}",
                        f"{item.total_time_s:.6f}",
                        f"{item.write_records_per_second:.6f}",
                        f"{item.read_records_per_second:.6f}",
                        item.success_writes,
                        item.failed_writes,
                        item.success_reads,
                        item.failed_reads,
                        item.payload_bytes_total,
                        item.payload_bytes_per_record,
                        item.db_table_bytes,
                        item.write_peak_alloc_bytes,
                        item.read_peak_alloc_bytes,
                        item.write_rss_delta_bytes,
                        item.read_rss_delta_bytes,
                        item.error,
                    ]
                )
        return self.config.async_results_csv

    def save_report(
        self,
        results: list[IterationMetrics],
        async_results: list[AsyncFanoutMetrics],
    ) -> str:
        generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
        first_fail = None
        stability_point = None
        breaking_point = None
        best_efficiency = None
        if results:
            first_fail = next((item for item in results if item.status != "OK"), None)
            if first_fail is None:
                stability_point = max(
                    (item.epochs for item in results if item.status == "OK"),
                    default=None,
                )
                breaking_point = None
            else:
                stability_point = max(
                    (
                        item.epochs
                        for item in results
                        if item.status == "OK" and item.epochs < first_fail.epochs
                    ),
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
        lines.append(f"- Mode: `{self.config.mode}`")
        lines.append(f"- Payload format: `{self.config.payload_format}`")
        lines.append(
            f"- Epoch range: `{self.config.start_epochs}..{self.config.end_epochs}`"
        )
        lines.append(f"- Step: `{self.config.step_epochs}`")
        if self.config.mode == "async":
            lines.append(f"- Async concurrency: `{self.config.async_concurrency}`")
        lines.append(f"- Table: `{self.config.table}`")
        lines.append("")

        if results:
            lines.append("## Breaking Point And Stability Point")
            lines.append("")
            lines.append(
                f"- Breaking Point: `{breaking_point}`"
                if breaking_point is not None
                else "- Breaking Point: not reached in tested range."
            )
            lines.append(
                f"- Stability Point: `{stability_point}`"
                if stability_point is not None
                else "- Stability Point: not confirmed."
            )
            if stability_point is not None:
                lines.append(
                    f"- Conclusion: stable write/read behavior is confirmed up to `{stability_point}` epochs."
                )
            else:
                lines.append(
                    "- Conclusion: increase retries/resources and re-run to confirm stability."
                )
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
        else:
            lines.append("## Linear Benchmark")
            lines.append("")
            lines.append(
                "Linear benchmark was not executed in this run (`--mode async`)."
            )
            lines.append("")

        if not async_results:
            lines.append("## Async Fanout Results")
            lines.append("")
            lines.append(
                "Async benchmark was not executed in this run (`--mode sync`)."
            )
            lines.append("")

        if async_results:
            lines.append("## Async Fanout Results")
            lines.append("")
            lines.append(
                "| iter | concurrency | epochs | status | write_s | read_s | total_s | "
                "write_rps | read_rps | writes_ok/total | reads_ok/total | payload_total_mb | payload_per_record_mb | "
                "db_table_mb | write_peak_mb | read_peak_mb | write_rss_delta_mb | read_rss_delta_mb | error |"
            )
            lines.append(
                "|---:|---:|---:|---|---:|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---|"
            )
            for item in async_results:
                lines.append(
                    f"| {item.iteration} | {item.concurrency} | {item.epochs} | {item.status} | "
                    f"{item.write_time_s:.2f} | {item.read_time_s:.2f} | {item.total_time_s:.2f} | "
                    f"{item.write_records_per_second:.2f} | {item.read_records_per_second:.2f} | "
                    f"{item.success_writes}/{item.success_writes + item.failed_writes} | "
                    f"{item.success_reads}/{item.success_reads + item.failed_reads} | "
                    f"{item.payload_bytes_total / (1024 * 1024):.2f} | "
                    f"{item.payload_bytes_per_record / (1024 * 1024):.2f} | "
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
