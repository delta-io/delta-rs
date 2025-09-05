import os
import pandas as pd
import numpy as np

from deltalake import write_deltalake, WriterProperties, ColumnProperties,DeltaTable
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa

# Make some fake time series data
TOTAL_ROWS = 100_000_00
timestamps = pd.date_range(start=pd.Timestamp.now(), periods=TOTAL_ROWS, freq="5ms")
timeline = np.linspace(0, len(timestamps), len(timestamps))
print("Generating data...")
pat = pa.Table.from_pandas(
    pd.DataFrame(
        {
            # timestamp (auto-generated)
            "timestamp": timestamps,
            # Timeseries data as float32
            "timeseries_data": (10 * np.sin(2 * np.pi * 50 * timeline)).astype(
                np.float32
            ),
            "timeseries_int_data": (1000 * np.sin(2 * np.pi * 50 * timeline)).astype(
                np.int32
            ),
            # 1 minute partitions
            "partition_label": timestamps.strftime("%H%M"),
        }
    )
)
print("Data generated.")
output_path_normal = "example_deltalake"
write_deltalake(
    output_path_normal,
    data=pat,
    partition_by=["partition_label"],
    # Enabled compression for equivalent comparison, dictionary enabled leads to a larger file size
    writer_properties=WriterProperties(
        compression="ZSTD",
        compression_level=1,
        default_column_properties=ColumnProperties(dictionary_enabled=True),
    ),
    mode="overwrite",
    # Can't specify per-column encoding
)
print("Wrote normal delta table.")



output_path_encoded = "encoded_example_deltalake"
write_deltalake(
    output_path_encoded,
    data=pat,
    partition_by=["partition_label"],
    # Enabled compression for equivalent comparison, dictionary enabled leads to a larger file size
    writer_properties=WriterProperties(
        compression="ZSTD",
        compression_level=1,
        column_properties={
            "timestamp": ColumnProperties(dictionary_enabled=False,encoding="DELTA_BINARY_PACKED"),
            "timeseries_data": ColumnProperties(dictionary_enabled=False,encoding="BYTE_STREAM_SPLIT"),
            "timeseries_int_data": ColumnProperties(dictionary_enabled=False,encoding="DELTA_BINARY_PACKED"),
            "partition_label": ColumnProperties(dictionary_enabled=False,encoding="DELTA_BINARY_PACKED"),
        },
    ),
    mode="overwrite",
    # Can't specify per-column encoding
)
print("Wrote encoded delta table.")

output_path_default_encoded = "example_pyarrow_delta_default_encoding"
pq.write_to_dataset(
    pat,
    output_path_default_encoded,
    partition_cols=["partition_label"],
    use_dictionary=False,
    use_byte_stream_split=True,
    compression="ZSTD",
    compression_level=1,
)


output_path_delta_specifc_encoded = "example_pyarrow_delta_specifc_col_encoding"
pq.write_to_dataset(
    pat,
    output_path_delta_specifc_encoded,
    partition_cols=["partition_label"],
    # Ability to specify column encodings here
    use_dictionary=False,
    use_byte_stream_split=False,
    column_encoding={
        "timestamp": "DELTA_BINARY_PACKED",
        "timeseries_data": "BYTE_STREAM_SPLIT",
        "timeseries_int_data": "DELTA_BINARY_PACKED",
        "partition_label": "DELTA_BINARY_PACKED",
    },
    compression="ZSTD",
    compression_level=1,
)
print("Wrote delta table with pyarrow column encodings.")


def get_folder_size(folder):
    return ByteSize(
        sum(file.stat().st_size for file in Path(folder).rglob("*"))
    ).megabytes


class ByteSize(int):
    _KB = 1024
    _suffixes = "B", "KB", "MB", "GB", "PB"

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        self.bytes = self.B = int(self)
        self.kilobytes = self.KB = self / self._KB**1
        self.megabytes = self.MB = self / self._KB**2
        self.gigabytes = self.GB = self / self._KB**3
        self.petabytes = self.PB = self / self._KB**4
        *suffixes, last = self._suffixes
        suffix = next(
            (suffix for suffix in suffixes if 1 < getattr(self, suffix) < self._KB),
            last,
        )
        self.readable = suffix, getattr(self, suffix)

        super().__init__()

    def __str__(self):
        return self.__format__(".2f")

print(DeltaTable(output_path_encoded).to_pandas())

print(f"The File size of delta table is {get_folder_size(output_path_normal)} MB")
print(f"The File size of delta table with parquet encoding is {get_folder_size(output_path_default_encoded)} MB")
print(
    f"The File size of delta table with pyarrow default column encodings is {get_folder_size(output_path_default_encoded)} MB"
)
print(
    f"The File size of delta table with pyarrow specific column encodings is {get_folder_size(output_path_delta_specifc_encoded)} MB"
)


print("Deleting the folders now...")
import shutil

shutil.rmtree(output_path_normal)
shutil.rmtree(output_path_encoded)
shutil.rmtree(output_path_default_encoded)
shutil.rmtree(output_path_delta_specifc_encoded)
print("Deleted the folders.")
