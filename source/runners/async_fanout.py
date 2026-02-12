import asyncio
import time
import tracemalloc
from typing import Callable, Any

from source.db import DatabaseManager
from source.entities import TestConfig, AsyncFanoutMetrics, Measurement
from source.json_generator import JSONGenerator
from source.utils import PayloadCodec, MetricsUtils


class AsyncFanoutBenchmarkRunner:
    def __init__(
        self,
        config: TestConfig,
        db: DatabaseManager,
        codec: PayloadCodec,
    ) -> None:
        self.config = config
        self.db = db
        self.codec = codec

    def run(self) -> list[AsyncFanoutMetrics]:
        epoch_values = self._build_epoch_values()
        concurrency = self.config.async_concurrency
        results: list[AsyncFanoutMetrics] = []
        for index, epochs in enumerate(epoch_values, start=1):
            print(
                f"[async {index}/{len(epoch_values)}] "
                f"concurrency={concurrency} epochs={epochs}"
            )
            try:
                result = asyncio.run(self._run_level(index, concurrency, epochs))
            except Exception as exc:
                result = self._fatal_iteration_result(index, concurrency, epochs, exc)
                self._print_async_summary(result)
                results.append(result)
                break
            self._print_async_summary(result)
            results.append(result)
        return results

    async def _run_level(
        self,
        iteration: int,
        concurrency: int,
        epochs: int,
    ) -> AsyncFanoutMetrics:
        if self.config.truncate_between_iterations:
            await self.db.async_truncate_table()

        # Build once and fan out the same payload to focus on async DB IO behavior.
        generator = JSONGenerator()
        try:
            shared_db_value, payload_size = self.codec.encode_for_insert(
                generator=generator,
                epochs=epochs,
            )
        except Exception as exc:
            prep_error = f"{type(exc).__name__}: {exc}"
            return AsyncFanoutMetrics(
                iteration=iteration,
                concurrency=concurrency,
                epochs=epochs,
                status="FAIL",
                write_time_s=0.0,
                read_time_s=0.0,
                success_writes=0,
                failed_writes=0,
                success_reads=0,
                failed_reads=0,
                payload_bytes_per_record=0,
                payload_bytes_total=0,
                db_table_bytes=0,
                write_peak_alloc_bytes=0,
                read_peak_alloc_bytes=0,
                write_rss_delta_bytes=0,
                read_rss_delta_bytes=0,
                error=f"prepare_payload: {prep_error}",
            )

        write_measure = await self._measure_async(
            lambda: self._run_async_writes(concurrency, shared_db_value)
        )
        row_ids: list[int] = []
        write_errors: list[str] = []
        if write_measure.data is not None:
            row_ids = write_measure.data["row_ids"]
            write_errors = write_measure.data["errors"]
        failed_writes = len(write_errors)
        success_writes = len(row_ids)
        payload_total_bytes = payload_size * concurrency

        read_measure = await self._measure_async(lambda: self._run_async_reads(row_ids))
        read_errors: list[str] = []
        success_reads = 0
        failed_reads = 0
        if read_measure.data is not None:
            success_reads = int(read_measure.data["success"])
            read_errors = read_measure.data["errors"]
            failed_reads = int(read_measure.data["failed"])
        elif read_measure.error is not None:
            failed_reads = len(row_ids)
            read_errors = [read_measure.error]

        table_bytes = 0
        try:
            table_bytes = self.db.fetch_table_size()
        except Exception as size_exc:
            read_errors.append(f"table_size: {type(size_exc).__name__}: {size_exc}")

        status = "OK" if failed_writes == 0 and failed_reads == 0 else "FAIL"
        all_errors = write_errors + read_errors
        error_message = all_errors[0] if all_errors else ""

        return AsyncFanoutMetrics(
            iteration=iteration,
            concurrency=concurrency,
            epochs=epochs,
            status=status,
            write_time_s=write_measure.elapsed_s,
            read_time_s=read_measure.elapsed_s,
            success_writes=success_writes,
            failed_writes=failed_writes,
            success_reads=success_reads,
            failed_reads=failed_reads,
            payload_bytes_per_record=payload_size,
            payload_bytes_total=payload_total_bytes,
            db_table_bytes=table_bytes,
            write_peak_alloc_bytes=write_measure.peak_alloc_bytes,
            read_peak_alloc_bytes=read_measure.peak_alloc_bytes,
            write_rss_delta_bytes=write_measure.rss_highwater_delta_bytes,
            read_rss_delta_bytes=read_measure.rss_highwater_delta_bytes,
            error=error_message,
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
    def _fatal_iteration_result(
        iteration: int, concurrency: int, epochs: int, exc: Exception
    ) -> AsyncFanoutMetrics:
        return AsyncFanoutMetrics(
            iteration=iteration,
            concurrency=concurrency,
            epochs=epochs,
            status="FAIL",
            write_time_s=0.0,
            read_time_s=0.0,
            success_writes=0,
            failed_writes=0,
            success_reads=0,
            failed_reads=0,
            payload_bytes_per_record=0,
            payload_bytes_total=0,
            db_table_bytes=0,
            write_peak_alloc_bytes=0,
            read_peak_alloc_bytes=0,
            write_rss_delta_bytes=0,
            read_rss_delta_bytes=0,
            error=f"fatal_iteration_error: {type(exc).__name__}: {exc}",
        )

    async def _run_async_writes(
        self, concurrency: int, shared_db_value: Any
    ) -> dict[str, Any]:
        tasks = [
            asyncio.create_task(self._insert_one_async(shared_db_value, producer_id=i))
            for i in range(concurrency)
        ]
        results = await asyncio.gather(*tasks)
        row_ids: list[int] = []
        errors: list[str] = []
        for success, row_id, error in results:
            if success and row_id is not None:
                row_ids.append(row_id)
            elif error:
                errors.append(error)
        return {"row_ids": row_ids, "errors": errors}

    async def _run_async_reads(self, row_ids: list[int]) -> dict[str, Any]:
        if not row_ids:
            return {"success": 0, "failed": 0, "errors": []}
        tasks = [
            asyncio.create_task(self._read_one_async(row_id)) for row_id in row_ids
        ]
        results = await asyncio.gather(*tasks)
        success = 0
        failed = 0
        errors: list[str] = []
        for ok, error in results:
            if ok:
                success += 1
            else:
                failed += 1
                if error:
                    errors.append(error)
        return {"success": success, "failed": failed, "errors": errors}

    async def _insert_one_async(
        self, shared_db_value: Any, producer_id: int
    ) -> tuple[bool, int | None, str]:
        conn = None
        try:
            conn = await self.codec.deps.psycopg.AsyncConnection.connect(
                self.config.dsn
            )
            async with conn.cursor() as cur:
                if self.config.statement_timeout_ms > 0:
                    await cur.execute(
                        "SET statement_timeout = %s;",
                        (self.config.statement_timeout_ms,),
                    )
                    await conn.commit()
                await cur.execute(
                    f"INSERT INTO {self.config.table} (payload) VALUES (%s) RETURNING id;",
                    (shared_db_value,),
                )
                row = await cur.fetchone()
            await conn.commit()
            return True, int(row[0]), ""
        except Exception as exc:
            if conn is not None:
                try:
                    await conn.rollback()
                except Exception:
                    pass
            return False, None, f"producer={producer_id}: {type(exc).__name__}: {exc}"
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass

    async def _read_one_async(self, row_id: int) -> tuple[bool, str]:
        conn = None
        try:
            conn = await self.codec.deps.psycopg.AsyncConnection.connect(
                self.config.dsn
            )
            async with conn.cursor() as cur:
                if self.config.statement_timeout_ms > 0:
                    await cur.execute(
                        "SET statement_timeout = %s;",
                        (self.config.statement_timeout_ms,),
                    )
                    await conn.commit()
                await cur.execute(
                    f"SELECT payload FROM {self.config.table} WHERE id = %s;", (row_id,)
                )
                row = await cur.fetchone()
            if row is None:
                return False, f"row {row_id} not found on async read"
            return True, ""
        except Exception as exc:
            return False, f"read row={row_id}: {type(exc).__name__}: {exc}"
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass

    @staticmethod
    async def _measure_async(action: Callable[[], Any]) -> Measurement:
        before_rss = MetricsUtils.current_rss_highwater_bytes()
        tracemalloc.start()
        started = time.perf_counter()
        try:
            data = await action()
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

    @staticmethod
    def _print_async_summary(result: AsyncFanoutMetrics) -> None:
        print(
            f"  status={result.status} "
            f"write={result.write_time_s:.2f}s "
            f"read={result.read_time_s:.2f}s "
            f"writes={result.success_writes}/{result.success_writes + result.failed_writes} "
            f"reads={result.success_reads}/{result.success_reads + result.failed_reads} "
            f"payload_total={MetricsUtils.format_bytes(result.payload_bytes_total)} "
            f"payload_per_record={MetricsUtils.format_bytes(result.payload_bytes_per_record)}"
        )
        if result.error:
            print(f"  error: {result.error}")
