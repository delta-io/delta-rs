#!/usr/bin/env python3
"""
Test 2: delta-rs writes a Delta table with column mapping enabled, PySpark reads it.
"""

import os
import sys
import tempfile
import shutil
import uuid

# ============================================================
# Part 1: Create Delta table with delta-rs (column mapping mode)
# ============================================================
print("="*60)
print("Creating Delta table with delta-rs (column mapping mode)")
print("="*60)

test_dir = "/tmp/delta_column_mapping_test_deltalake"
table_path = os.path.join(test_dir, "deltalake_table")

# Clean up any previous test
if os.path.exists(test_dir):
    shutil.rmtree(test_dir)
os.makedirs(test_dir)

print(f"\nTable path: {table_path}")

import pyarrow as pa
import deltalake
from deltalake import DeltaTable, write_deltalake

print(f"deltalake version: {deltalake.__version__}")


def generate_physical_name():
    """Generate a UUID-based physical column name like PySpark does."""
    return f"col-{uuid.uuid4()}"


def add_column_mapping_metadata(schema: pa.Schema) -> pa.Schema:
    """Add column mapping metadata to a PyArrow schema."""
    new_fields = []
    for idx, field in enumerate(schema):
        # Generate physical name and column ID
        physical_name = generate_physical_name()
        column_id = str(idx + 1)

        # Add metadata
        metadata = dict(field.metadata) if field.metadata else {}
        metadata[b"delta.columnMapping.id"] = column_id.encode()
        metadata[b"delta.columnMapping.physicalName"] = physical_name.encode()

        new_field = pa.field(field.name, field.type, field.nullable, metadata)
        new_fields.append(new_field)

    return pa.schema(new_fields)


# Create sample data with PyArrow
original_schema = pa.schema([
    ("id", pa.int64()),
    ("user name", pa.string()),
    ("age", pa.int64()),
    ("salary amount", pa.float64()),
])

# NOTE: Do NOT add column mapping metadata manually!
# When creating a new table with column mapping, delta-rs should handle this automatically.
# Adding metadata manually causes validation errors because the kernel checks schema
# metadata against the column mapping mode BEFORE the configuration is applied.

print("\nUsing schema WITHOUT column mapping metadata:")
for field in original_schema:
    print(f"  {field.name}: {field.type}")

data = pa.table({
    "id": [1, 2, 3, 4, 5],
    "user name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "age": [30, 25, 35, 28, 32],
    "salary amount": [50000.0, 60000.0, 70000.0, 55000.0, 65000.0],
}, schema=original_schema)

print("\nOriginal data:")
print(data.to_pandas())

# Write to Delta with column mapping mode enabled
print(f"\nWriting to Delta table with column mapping mode = 'name'...")
write_deltalake(
    table_path,
    data,
    mode="overwrite",
    configuration={
        "delta.columnMapping.mode": "name",
    },
)

print("Delta table created successfully!")

# Read back with delta-rs to verify
print("\n" + "-"*60)
print("Reading back with delta-rs:")
print("-"*60)
dt = DeltaTable(table_path)
print(f"Table version: {dt.version()}")
print(f"Column mapping mode: {dt._table.get_column_mapping_mode()}")
print("\nSchema:")
for field in dt.schema().fields:
    print(f"  - {field.name}: {field.type}")
    if field.metadata:
        for key, value in field.metadata.items():
            print(f"      {key}: {value}")

table_read = dt.to_pyarrow_table()
print(f"\nData read by delta-rs:")
print(table_read.to_pandas())

# Show what's in the delta log
print("\n" + "-"*60)
print("Delta log contents:")
print("-"*60)
delta_log_path = os.path.join(table_path, "_delta_log")
for f in sorted(os.listdir(delta_log_path)):
    if f.endswith('.json'):
        print(f"\n  {f}:")
        with open(os.path.join(delta_log_path, f)) as fp:
            for line in fp:
                print(f"    {line.strip()[:200]}...")

# ============================================================
# Part 2: Read Delta table with PySpark
# ============================================================
print("\n\n" + "="*60)
print("Reading Delta table with PySpark")
print("="*60)

from pyspark.sql import SparkSession

# Use delta-spark from Maven with correct Scala version (2.12 for Spark 3.5.x)
# Note: delta-spark 3.2.x requires PySpark 3.5.x (Delta 4.x is for Spark 4.x)
spark = SparkSession.builder \
    .appName("DeltaColumnMappingReadTest") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.warehouse.dir", test_dir) \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully!")

try:
    print(f"\nReading Delta table from: {table_path}")
    spark_df = spark.read.format("delta").load(table_path)

    print("\nPySpark schema:")
    spark_df.printSchema()

    print("\nData read by PySpark:")
    spark_df.show()

    # Convert to pandas for comparison
    pandas_df = spark_df.toPandas()
    print("\nPySpark data as pandas DataFrame:")
    print(pandas_df)

    # Verify data matches
    original_df = data.to_pandas()

    # Sort both dataframes by 'id' for comparison
    original_sorted = original_df.sort_values('id').reset_index(drop=True)
    spark_sorted = pandas_df.sort_values('id').reset_index(drop=True)

    # Compare
    if original_sorted.equals(spark_sorted):
        print("\n" + "="*60)
        print("SUCCESS: PySpark correctly read delta-rs column mapping table!")
        print("Data matches perfectly!")
        print("="*60)
    else:
        print("\n" + "!"*60)
        print("WARNING: Data mismatch!")
        print("Original:")
        print(original_sorted)
        print("\nPySpark read:")
        print(spark_sorted)
        print("!"*60)

except Exception as e:
    print(f"\nERROR reading with PySpark: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    print("\n" + "="*60)
    print("FAILED: PySpark could not read the delta-rs column mapping table")
    print("="*60)

finally:
    spark.stop()
    print("\nPySpark session stopped.")

# ============================================================
# Part 3: Direct parquet inspection
# ============================================================
print("\n\n" + "="*60)
print("Part 3: Direct parquet inspection")
print("="*60)

import pyarrow.parquet as pq

parquet_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]
if parquet_files:
    parquet_path = os.path.join(table_path, parquet_files[0])
    print(f"\nReading parquet file directly: {parquet_files[0]}")
    pq_table = pq.read_table(parquet_path)
    print(f"\nParquet schema:")
    print(pq_table.schema)
    print(f"\nParquet column names: {pq_table.column_names}")
    print(f"\nParquet data:")
    print(pq_table.to_pandas())

print(f"\n\nTest table preserved at: {table_path}")
print("Done!")
