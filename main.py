from source.cli import CLI
from source.db import DatabaseManager
from source.entities import TestConfig, IterationMetrics, AsyncFanoutMetrics
from source.report import ResultsReporter
from source.runners.async_fanout import AsyncFanoutBenchmarkRunner
from source.runners.linear import LinearBenchmarkRunner
from source.utils import DependencyProvider, PayloadCodec


class App:
    def __init__(self, config: TestConfig) -> None:
        self.config = config
        self.deps = DependencyProvider(config.payload_format).load()
        self.db = DatabaseManager(config, self.deps.psycopg)
        self.codec = PayloadCodec(config, self.deps)
        self.runner = LinearBenchmarkRunner(config, self.db, self.codec)
        self.async_runner = AsyncFanoutBenchmarkRunner(config, self.db, self.codec)
        self.reporter = ResultsReporter(config)

    def run(self) -> None:
        self.reporter.print_config()
        self.db.prepare_schema()
        results: list[IterationMetrics] = []
        async_results: list[AsyncFanoutMetrics] = []
        csv_path = ""
        async_csv_path = ""

        if self.config.mode == "sync":
            results = self.runner.run()
            self.reporter.print_results_table(results)
            csv_path = self.reporter.save_csv(results)
            print(f"CSV saved: {csv_path}")
        else:
            async_results = self.async_runner.run()
            self.reporter.print_async_results_table(async_results)
            async_csv_path = self.reporter.save_async_csv(async_results)
            print(f"Async CSV saved: {async_csv_path}")

        report_path = self.reporter.save_report(results, async_results)
        print(f"Report saved: {report_path}")


def main() -> None:
    config = CLI.parse_config()
    app = App(config)
    app.run()


if __name__ == "__main__":
    main()
