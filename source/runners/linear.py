import time
import tracemalloc
from typing import Any, Callable

from source.db import DatabaseManager
from source.entities import TestConfig, IterationMetrics, Measurement
from source.json_generator import JSONGenerator
from source.utils import PayloadCodec, MetricsUtils


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
            db_payload_bytes, db_row_bytes, db_table_bytes = (
                self.db.fetch_storage_metrics(row_id)
            )
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
        values = list(
            range(
                self.config.start_epochs,
                self.config.end_epochs + 1,
                self.config.step_epochs,
            )
        )
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
