//! Write-path benchmarks: large (optionally partitioned) writes through both the
//! high-level `DeltaTable::write` path and the low-level `RecordBatchWriter`.
//!
//! Table creation and data generation are done as untimed setup; only the
//! write + commit is timed.

use std::sync::Arc;

use deltalake_core::arrow::array::{Int32Array, Int64Array, StringArray};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema};
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake_core::writer::{DeltaWriter as _, RecordBatchWriter};
use deltalake_core::{DeltaResult, DeltaTable};
use url::Url;

/// Distinct values of the `category` (partition) column.
const PARTITION_CARDINALITY: usize = 32;
/// Distinct values of the `value` string column (kept small so data generation
/// is cheap; the encoded size is still realistic).
const STRING_POOL: usize = 10_000;

/// Which write API to exercise.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WritePath {
    /// High-level `DeltaTable::write` (the DataFusion write path).
    HighLevel,
    /// Low-level `RecordBatchWriter` (the direct parquet write path).
    RecordBatch,
}

/// Parameters for one write benchmark scenario.
#[derive(Clone)]
pub struct WriteParams {
    pub rows: usize,
    /// `0` = unpartitioned; otherwise partition by `category`.
    pub partitions: usize,
    pub batch_size: usize,
    pub path: WritePath,
}

impl std::fmt::Debug for WriteParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = match self.path {
            WritePath::HighLevel => "write",
            WritePath::RecordBatch => "rbw",
        };
        let part = if self.partitions == 0 {
            "unpart".to_string()
        } else {
            format!("part{}", self.partitions)
        };
        let shape = if self.batch_size >= self.rows.div_ceil(8) {
            "few-large"
        } else {
            "many-small"
        };
        write!(f, "{path}/{}/{part}/{shape}", human(self.rows))
    }
}

fn human(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

/// Arrow schema of the generated data: `id: Int64, category: Int32, value: Utf8`.
pub fn write_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn delta_fields() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "category",
            DeltaDataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "value",
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
    ]
}

/// Generate `rows` rows split into batches of `batch_size`.
pub fn generate_batches(params: &WriteParams) -> Vec<RecordBatch> {
    let schema = write_arrow_schema();
    let pool: Vec<String> = (0..STRING_POOL).map(|i| format!("value_{i}")).collect();
    let card = if params.partitions == 0 {
        PARTITION_CARDINALITY
    } else {
        params.partitions
    };
    let batch_size = params.batch_size.max(1);

    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < params.rows {
        let end = (start + batch_size).min(params.rows);
        let ids = Int64Array::from_iter_values((start..end).map(|i| i as i64));
        let cats = Int32Array::from_iter_values((start..end).map(|i| (i % card) as i32));
        let vals =
            StringArray::from_iter_values((start..end).map(|i| pool[i % STRING_POOL].as_str()));
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(ids), Arc::new(cats), Arc::new(vals)],
            )
            .expect("build record batch"),
        );
        start = end;
    }
    batches
}

/// Create an empty Delta table (with the right schema/partitioning) to write into.
pub async fn create_table(url: &Url, params: &WriteParams) -> DeltaResult<DeltaTable> {
    let builder = DeltaTable::try_from_url(url.clone())
        .await?
        .create()
        .with_columns(delta_fields());
    let builder = if params.partitions > 0 {
        builder.with_partition_columns(["category"])
    } else {
        builder
    };
    builder.await
}

/// Write `batches` into `table` using the path selected by `params`. This is the
/// timed portion of the benchmark.
pub async fn run_write(
    table: DeltaTable,
    batches: Vec<RecordBatch>,
    params: &WriteParams,
) -> DeltaResult<()> {
    match params.path {
        WritePath::HighLevel => {
            table.write(batches).await?;
        }
        WritePath::RecordBatch => {
            let mut table = table;
            let mut writer = RecordBatchWriter::for_table(&table)?;
            for batch in batches {
                writer.write(batch).await?;
            }
            writer.flush_and_commit(&mut table).await?;
        }
    }
    Ok(())
}

/// The scenario matrix: {1M, 10M} rows × {unpartitioned, 32 partitions}, the
/// high-level path with few large batches, and the `RecordBatchWriter` path with
/// both few-large and many-small batches (the latter stresses per-batch cost).
pub fn write_cases() -> Vec<WriteParams> {
    let mut cases = Vec::new();
    for rows in [1_000_000usize, 10_000_000] {
        for partitions in [0usize, 32] {
            cases.push(WriteParams {
                rows,
                partitions,
                batch_size: rows.div_ceil(8),
                path: WritePath::HighLevel,
            });
            cases.push(WriteParams {
                rows,
                partitions,
                batch_size: rows.div_ceil(8),
                path: WritePath::RecordBatch,
            });
            cases.push(WriteParams {
                rows,
                partitions,
                batch_size: 8_192,
                path: WritePath::RecordBatch,
            });
        }
    }
    cases
}
