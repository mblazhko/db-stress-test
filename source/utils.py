import io
import json
import platform
import resource
from typing import Any

from source.entities import Dependencies, TestConfig
from source.json_generator import JSONGenerator


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
                raise SystemExit(
                    "bson is not installed. Install with: pip install pymongo"
                ) from exc
            bson_cls = BSON
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


class PayloadCodec:
    def __init__(self, config: TestConfig, deps: Dependencies) -> None:
        self.config = config
        self.deps = deps

    def encode_for_insert(
        self, generator: JSONGenerator, epochs: int
    ) -> tuple[Any, int]:
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
