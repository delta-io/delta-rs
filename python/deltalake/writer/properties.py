from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal


class Compression(Enum):
    UNCOMPRESSED = "UNCOMPRESSED"
    SNAPPY = "SNAPPY"
    GZIP = "GZIP"
    BROTLI = "BROTLI"
    LZ4 = "LZ4"
    ZSTD = "ZSTD"
    LZ4_RAW = "LZ4_RAW"

    @classmethod
    def from_str(cls, value: str) -> Compression:
        try:
            return cls(value.upper())
        except ValueError:
            raise ValueError(
                f"{value} is not a valid Compression. Valid values are: {[item.value for item in Compression]}"
            )

    def get_level_range(self) -> tuple[int, int]:
        if self == Compression.GZIP:
            MIN_LEVEL = 0
            MAX_LEVEL = 10
        elif self == Compression.BROTLI:
            MIN_LEVEL = 0
            MAX_LEVEL = 11
        elif self == Compression.ZSTD:
            MIN_LEVEL = 1
            MAX_LEVEL = 22
        else:
            raise KeyError(f"{self.value} does not have a compression level.")
        return MIN_LEVEL, MAX_LEVEL

    def get_default_level(self) -> int:
        if self == Compression.GZIP:
            DEFAULT = 6
        elif self == Compression.BROTLI:
            DEFAULT = 1
        elif self == Compression.ZSTD:
            DEFAULT = 1
        else:
            raise KeyError(f"{self.value} does not have a compression level.")
        return DEFAULT

    def check_valid_level(self, level: int) -> bool:
        MIN_LEVEL, MAX_LEVEL = self.get_level_range()
        if level < MIN_LEVEL or level > MAX_LEVEL:
            raise ValueError(
                f"Compression level for {self.value} should fall between {MIN_LEVEL}-{MAX_LEVEL}"
            )
        else:
            return True


class Encoding(Enum):
    """
    Encoding types for Parquet columns.
    https://parquet.apache.org/docs/file-format/data-pages/encodings/
    Dictionary encodings (PLAIN_DICTIONARY and RLE_DICTIONARY) are enabled via the `dictionary_enabled` flag
    in `ColumnProperties` and should not be specified here.
    BIT_PACKED is deprecated and not supported in delta-rs
    """

    PLAIN = "PLAIN"
    RLE = "RLE"
    DELTA_BINARY_PACKED = "DELTA_BINARY_PACKED"
    DELTA_LENGTH_BYTE_ARRAY = "DELTA_LENGTH_BYTE_ARRAY"
    DELTA_BYTE_ARRAY = "DELTA_BYTE_ARRAY"
    BYTE_STREAM_SPLIT = "BYTE_STREAM_SPLIT"

    @classmethod
    def from_str(cls, value: str) -> "Encoding":
        try:
            return cls(value.upper())
        except ValueError:
            raise ValueError(
                f"{value} is not a valid Encoding. Valid values are: {[item.value for item in Encoding]}"
            )


@dataclass(init=True)
class BloomFilterProperties:
    """The Bloom Filter Properties instance for the Rust parquet writer."""

    def __init__(
        self,
        set_bloom_filter_enabled: bool | None,
        fpp: float | None = None,
        ndv: int | None = None,
    ) -> None:
        """Create a Bloom Filter Properties instance for the Rust parquet writer:

        Args:
            set_bloom_filter_enabled: If True and no fpp or ndv are provided, the default values will be used.
            fpp: The false positive probability for the bloom filter. Must be between 0 and 1 exclusive.
            ndv: The number of distinct values for the bloom filter.
        """
        if fpp is not None and (fpp <= 0 or fpp >= 1):
            raise ValueError("fpp must be between 0 and 1 exclusive")
        self.set_bloom_filter_enabled = set_bloom_filter_enabled
        self.fpp = fpp
        self.ndv = ndv

    def __str__(self) -> str:
        return f"set_bloom_filter_enabled: {self.set_bloom_filter_enabled}, fpp: {self.fpp}, ndv: {self.ndv}"


@dataclass(init=True)
class ColumnProperties:
    """The Column Properties instance for the Rust parquet writer."""

    def __init__(
        self,
        dictionary_enabled: bool | None = None,
        statistics_enabled: Literal["NONE", "CHUNK", "PAGE"] | None = None,
        bloom_filter_properties: BloomFilterProperties | None = None,
        encoding: Literal[
            "PLAIN",
            "RLE",
            "DELTA_BINARY_PACKED",
            "DELTA_LENGTH_BYTE_ARRAY",
            "DELTA_BYTE_ARRAY",
            "BYTE_STREAM_SPLIT",
        ]
        | None = None,
    ) -> None:
        """Create a Column Properties instance for the Rust parquet writer:

        Args:
            dictionary_enabled: Enable dictionary encoding for the column.
            statistics_enabled: Statistics level for the column.
            bloom_filter_properties: Bloom Filter Properties for the column.
            encoding: Encoding for the column if *NOT* using dictionary encoding.
        """
        self.dictionary_enabled = dictionary_enabled
        self.statistics_enabled = statistics_enabled
        self.bloom_filter_properties = bloom_filter_properties
        self.encoding = encoding
        if isinstance(encoding, str):
            if self.dictionary_enabled:
                raise ValueError("Cannot specify dictionary_enabled=True and encoding")

    def __str__(self) -> str:
        return (
            f"dictionary_enabled: {self.dictionary_enabled}, statistics_enabled: {self.statistics_enabled}, "
            f"bloom_filter_properties: {self.bloom_filter_properties}, encoding: {self.encoding}"
        )


@dataclass(init=True)
class WriterProperties:
    """A Writer Properties instance for the Rust parquet writer."""

    def __init__(
        self,
        data_page_size_limit: int | None = None,
        dictionary_page_size_limit: int | None = None,
        data_page_row_count_limit: int | None = None,
        write_batch_size: int | None = None,
        max_row_group_size: int | None = None,
        compression: Literal[
            "UNCOMPRESSED", "SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"
        ]
        | None = None,
        compression_level: int | None = None,
        statistics_truncate_length: int | None = None,
        default_column_properties: ColumnProperties | None = None,
        column_properties: dict[str, ColumnProperties] | None = None,
    ) -> None:
        """Create a Writer Properties instance for the Rust parquet writer:

        Args:
            data_page_size_limit: Limit DataPage size to this in bytes.
            dictionary_page_size_limit: Limit the size of each DataPage to store dicts to this amount in bytes.
            data_page_row_count_limit: Limit the number of rows in each DataPage.
            write_batch_size: Splits internally to smaller batch size.
            max_row_group_size: Max number of rows in row group.
            compression: compression type.
            compression_level: If none and compression has a level, the default level will be used, only relevant for
                GZIP: levels (1-9),
                BROTLI: levels (1-11),
                ZSTD: levels (1-22),
            statistics_truncate_length: maximum length of truncated min/max values in statistics.
            default_column_properties: Default Column Properties for the Rust parquet writer.
            column_properties: Column Properties for the Rust parquet writer.
        """
        self.data_page_size_limit = data_page_size_limit
        self.dictionary_page_size_limit = dictionary_page_size_limit
        self.data_page_row_count_limit = data_page_row_count_limit
        self.write_batch_size = write_batch_size
        self.max_row_group_size = max_row_group_size
        self.compression = None
        self.statistics_truncate_length = statistics_truncate_length
        self.default_column_properties = default_column_properties
        self.column_properties = column_properties

        if compression_level is not None and compression is None:
            raise ValueError(
                """Providing a compression level without the compression type is not possible,
                             please provide the compression as well."""
            )
        if isinstance(compression, str):
            compression_enum = Compression.from_str(compression)
            if compression_enum in [
                Compression.GZIP,
                Compression.BROTLI,
                Compression.ZSTD,
            ]:
                if compression_level is not None:
                    if compression_enum.check_valid_level(compression_level):
                        parquet_compression = (
                            f"{compression_enum.value}({compression_level})"
                        )
                else:
                    parquet_compression = f"{compression_enum.value}({compression_enum.get_default_level()})"
            else:
                parquet_compression = compression_enum.value
            self.compression = parquet_compression

    def __str__(self) -> str:
        column_properties_str = (
            ", ".join([f"column '{k}': {v}" for k, v in self.column_properties.items()])
            if self.column_properties
            else None
        )
        return (
            f"WriterProperties(data_page_size_limit: {self.data_page_size_limit}, dictionary_page_size_limit: {self.dictionary_page_size_limit}, "
            f"data_page_row_count_limit: {self.data_page_row_count_limit}, write_batch_size: {self.write_batch_size}, "
            f"max_row_group_size: {self.max_row_group_size}, compression: {self.compression}, statistics_truncate_length: {self.statistics_truncate_length},"
            f"default_column_properties: {self.default_column_properties}, column_properties: {column_properties_str})"
        )
