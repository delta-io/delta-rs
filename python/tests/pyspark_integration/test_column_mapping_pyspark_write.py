#!/usr/bin/env python3
"""
Test reading Delta tables with column mapping mode enabled.
Creates a table with PySpark and reads it with delta-rs.
"""

import os
import sys
import tempfile
import shutil

# ============================================================
# Part 1: Create Delta table with PySpark (column mapping mode)
# ============================================================
print("="*60)
print("Creating Delta table with PySpark (column mapping mode)")
print("="*60)

# Use a persistent location
test_dir = "/tmp/delta_column_mapping_test"
table_path = os.path.join(test_dir, "pyspark_table")

# Clean up any previous test
if os.path.exists(test_dir):
    shutil.rmtree(test_dir)
os.makedirs(test_dir)

print(f"\nTable path: {table_path}")

from pyspark.sql import SparkSession

# Use delta-spark from Maven with correct Scala version (2.12 for Spark 3.5.x)
# Note: delta-spark 3.2.x requires PySpark 3.5.x (Delta 4.x is for Spark 4.x)
spark = SparkSession.builder \
    .appName("DeltaColumnMappingTest") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.warehouse.dir", test_dir) \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully!")

# Create a DataFrame with sample data
data = [
    (1, "Alice", 30, 50000.0),
    (2, "Bob", 25, 60000.0),
    (3, "Charlie", 35, 70000.0),
    (4, "Diana", 28, 55000.0),
    (5, "Eve", 32, 65000.0),
]
columns = ["id", "user name", "age", "salary amount"]  # Names with spaces to test column mapping

df = spark.createDataFrame(data, columns)
print("\nOriginal DataFrame:")
df.show()
df.printSchema()

# Write to Delta with column mapping mode enabled
print(f"\nWriting to Delta table with column mapping mode = 'name'...")
df.write.format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .save(table_path)

print("Delta table created successfully!")

# Read back with PySpark to verify
print("\n" + "-"*60)
print("Reading back with PySpark:")
print("-"*60)
spark_df = spark.read.format("delta").load(table_path)
spark_df.show()

spark.stop()
print("\nPySpark session stopped.")

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
                print(f"    {line.strip()}")

# ============================================================
# Part 2: Read Delta table with delta-rs
# ============================================================
print("\n\n" + "="*60)
print("Reading Delta table with delta-rs (deltalake)")
print("="*60)

import deltalake
print(f"\ndeltalake version: {deltalake.__version__}")

try:
    dt = deltalake.DeltaTable(table_path)

    print(f"\nTable version: {dt.version()}")

    # Check schema
    schema = dt.schema()
    print(f"\nTable schema (logical names):")
    for field in schema.fields:
        print(f"  - {field.name}: {field.type}")
        if field.metadata:
            for key, value in field.metadata.items():
                print(f"      {key}: {value}")

    # Check metadata
    metadata = dt.metadata()
    print(f"\nTable metadata configuration:")
    for key, value in metadata.configuration.items():
        print(f"  - {key}: {value}")

    # Read the data
    print("\n" + "-"*60)
    print("Reading data with to_pyarrow_table():")
    print("-"*60)
    arrow_table = dt.to_pyarrow_table()
    print(f"\nPyArrow schema:")
    print(arrow_table.schema)
    print(f"\nColumn names: {arrow_table.column_names}")
    print(f"\nData ({arrow_table.num_rows} rows):")
    pdf = arrow_table.to_pandas()
    print(pdf.to_string())

    # Check if data was read correctly
    has_null_values = pdf.isna().all().all()

    if has_null_values:
        print("\n" + "!"*60)
        print("WARNING: All values are NULL/NaN!")
        print("This indicates column mapping translation is NOT working")
        print("during parquet reads - it's reading physical column names")
        print("from parquet but schema expects logical names.")
        print("!"*60)
    else:
        print("\n" + "="*60)
        print("SUCCESS: delta-rs can read the PySpark column mapping table!")
        print("="*60)

except Exception as e:
    print(f"\nERROR reading with delta-rs: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    print("\n" + "="*60)
    print("FAILED: delta-rs could not read the column mapping table")
    print("="*60)

# ============================================================
# Part 3: Directly read parquet to verify column names
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
    print(f"\nParquet schema (physical names):")
    print(pq_table.schema)
    print(f"\nParquet column names: {pq_table.column_names}")
    print(f"\nParquet data:")
    print(pq_table.to_pandas().to_string())

    print("\n" + "-"*60)
    print("CONCLUSION:")
    print("-"*60)
    print("Parquet files contain PHYSICAL column names (col-xxx-xxx)")
    print("Delta schema contains LOGICAL names (id, user name, etc.)")
    print("delta-rs needs to translate physical -> logical during reads")

print(f"\n\nTest table preserved at: {table_path}")
print("Done!")
