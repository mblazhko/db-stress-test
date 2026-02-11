from typing import Any

from source.entities import TestConfig


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

    async def async_truncate_table(self) -> None:
        conn = await self.psycopg.AsyncConnection.connect(self.config.dsn)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"TRUNCATE TABLE {self.config.table};")
            await conn.commit()
        finally:
            await conn.close()

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

    def fetch_table_size(self) -> int:
        with self.psycopg.connect(self.config.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_total_relation_size(%s::regclass);", (self.config.table,))
                return int(cur.fetchone()[0])
