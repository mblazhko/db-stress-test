from dataclasses import dataclass
from typing import Any


@dataclass
class TestConfig:
    mode: str
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
    async_results_csv: str
    report_md: str
    async_concurrency: int


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


@dataclass
class AsyncFanoutMetrics:
    iteration: int
    concurrency: int
    epochs: int
    status: str
    write_time_s: float
    read_time_s: float
    success_writes: int
    failed_writes: int
    success_reads: int
    failed_reads: int
    payload_bytes_per_record: int
    payload_bytes_total: int
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
        return (self.success_writes * self.epochs) / self.total_time_s

    @property
    def write_records_per_second(self) -> float:
        if self.write_time_s <= 0:
            return 0.0
        return self.success_writes / self.write_time_s

    @property
    def read_records_per_second(self) -> float:
        if self.read_time_s <= 0:
            return 0.0
        return self.success_reads / self.read_time_s
