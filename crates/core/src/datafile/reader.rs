//! DataFusion-free data-file readers (file and dataset tiers).
//!
//! This module hosts two kinds of reader:
//!
//! * [`ParquetFileReader`] / [`ParquetTableReader`]
//!   The first concrete implementations of the read traits
//!   They read raw parquet directly from object storage with no DataFusion, and
//!   are intentionally minimal: they reject tables that need deletion-vector
//!   application, column mapping, or partition-value reconstruction.
//! * [`KernelDataFileReader`] / [`KernelDataReader`] — placeholders for the
//!   later, full-fidelity reader backed by `delta-kernel`'s scan engine (which
//!   applies deletion vectors, partition values, and column-mapping transforms).
//!
//! In a `datafusion` build, full reads go through
//! [`crate::datafile::datafusion_ext::DeltaDataReaderExt`].

use std::sync::Arc;

use delta_kernel::table_features::ColumnMappingMode;
use futures::stream::{StreamExt as _, TryStreamExt as _};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};

use super::{
    BatchFuture, DataFileReader, DeltaDataReader, ReadOptions, RecordBatchFutureStream,
    results_to_future_stream,
};

fn not_yet_implemented(what: &str) -> DeltaTableError {
    DeltaTableError::Generic(format!(
        "The kernel-backed read path ({what}) is not yet implemented. For a plain \
         table, use ParquetTableReader (a DataFusion-free reader); for full Delta \
         read semantics (deletion vectors, column mapping, partition values) \
         enable the `datafusion` feature."
    ))
}

fn not_supported(feature: &str) -> DeltaTableError {
    DeltaTableError::Generic(format!(
        "ParquetTableReader cannot read a table that uses {feature}; \
         use the DataFusion read path (DeltaDataReaderExt) for such tables."
    ))
}

// ---------------------------------------------------------------------------
// New: a concrete, DataFusion-free parquet reader that proves the read traits.
// ---------------------------------------------------------------------------

/// File-tier reader that reads a single parquet data file directly from object
/// storage, with no DataFusion.
///
/// Concrete [`DataFileReader`], added to validate the
/// per-file read seam (the same seam where parquet decryption will later
/// attach, mirroring the write side).
#[derive(Debug, Clone)]
pub struct ParquetFileReader {
    store: Arc<dyn ObjectStore>,
}

impl ParquetFileReader {
    /// Create a reader over the given object store.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Read a data file, optionally passing its known size so the parquet reader
    /// can skip the extra `HEAD` request it would otherwise make to discover it.
    async fn read_file_sized(
        &self,
        path: Path,
        size: Option<u64>,
    ) -> DeltaResult<RecordBatchFutureStream> {
        let mut reader = ParquetObjectReader::new(self.store.clone(), path);
        if let Some(size) = size {
            reader = reader.with_file_size(size);
        }
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .build()?;
        Ok(results_to_future_stream(stream))
    }
}

#[async_trait::async_trait]
impl DataFileReader for ParquetFileReader {
    async fn read_file(&self, path: Path) -> DeltaResult<RecordBatchFutureStream> {
        self.read_file_sized(path, None).await
    }
}

/// Dataset-tier reader that reads all of a table's parquet data files directly
/// (no DataFusion, no predicate), composing [`ParquetFileReader`] across the
/// table.
///
/// Concrete [`DeltaDataReader`], added to prove that
/// the file tier composes into a whole-table read through the
/// [`RecordBatchFutureStream`] waist.
///
/// Minimal by design — it reads raw parquet, so [`ParquetTableReader::try_new`]
/// rejects tables that use deletion vectors, column mapping, or partition
/// columns. Honoring those is the job of the kernel-backed [`KernelDataReader`].
///
/// It also does not unify schemas across files: the returned batches reflect
/// each data file's physical schema as written. On a table whose schema evolved
/// (a widened column type or an added column), older and newer files yield
/// batches with differing schemas, and the caller is responsible for
/// reconciling them (e.g. casting to a common schema before `concat`).
#[derive(Debug, Clone)]
pub struct ParquetTableReader {
    file_reader: ParquetFileReader,
    /// Data files as (object-store path, size in bytes).
    files: Vec<(Path, u64)>,
}

impl ParquetTableReader {
    /// Build a reader over the table's current data files.
    ///
    /// Errors if the table uses a feature this raw reader cannot honor
    /// (deletion vectors, column mapping, or partition columns).
    pub async fn try_new(table: &DeltaTable) -> DeltaResult<Self> {
        let snapshot = table.snapshot()?;

        // Guard: column mapping would mean physical (not logical) column names.
        // Use the resolved mode (the property is only honored when the protocol
        // actually enables the feature) to avoid rejecting a table that merely
        // carries a stale, ignored `delta.columnMapping.mode` property.
        if snapshot
            .snapshot()
            .table_configuration()
            .column_mapping_mode()
            != ColumnMappingMode::None
        {
            return Err(not_supported("column mapping"));
        }
        // Guard: partition column values live in the path, not the parquet file.
        if !snapshot.metadata().partition_columns().is_empty() {
            return Err(not_supported("partition columns"));
        }

        let log_store = table.log_store();
        let mut files = Vec::new();
        let mut views = snapshot.snapshot().file_views(log_store.as_ref(), None);
        while let Some(view) = views.try_next().await? {
            // Guard: a raw read would return rows that a deletion vector removes.
            if view.deletion_vector_descriptor().is_some() {
                return Err(not_supported("deletion vectors"));
            }
            // Use the canonical object-store path helper (preserves percent
            // encoding), matching every other read path; carry the size so the
            // parquet reader can skip a `HEAD` per file.
            files.push((view.object_store_path(), view.size() as u64));
        }

        let store = log_store.object_store(None);
        Ok(Self {
            file_reader: ParquetFileReader::new(store),
            files,
        })
    }
}

#[async_trait::async_trait]
impl DeltaDataReader for ParquetTableReader {
    async fn read(&self, options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        // Projection / limit pushdown is a follow-up; reject rather than silently
        // ignore so callers don't get more data than they asked for.
        if options.projection.is_some() || options.limit.is_some() {
            return Err(DeltaTableError::Generic(
                "projection and limit are not yet supported by the basic parquet reader".into(),
            ));
        }

        let file_reader = self.file_reader.clone();
        // Read up to `num_cpus` files concurrently and interleave their batches
        // (`flat_map_unordered`), so the `RecordBatchFutureStream` waist actually
        // overlaps per-file open/read latency instead of serializing it. Row
        // order across files is not preserved, which is fine for a no-predicate
        // scan.
        let concurrency = num_cpus::get().max(1);
        let stream = futures::stream::iter(self.files.clone())
            .flat_map_unordered(concurrency, move |(path, size)| {
                futures::stream::once(open_file_stream(file_reader.clone(), path, size))
                    .flatten()
                    .boxed()
            })
            .boxed();
        Ok(stream)
    }
}

/// Open one data file, turning an open error into a single failing batch future
/// so it surfaces while draining rather than being silently dropped.
async fn open_file_stream(
    reader: ParquetFileReader,
    path: Path,
    size: u64,
) -> RecordBatchFutureStream {
    match reader.read_file_sized(path, Some(size)).await {
        Ok(file_stream) => file_stream,
        Err(err) => {
            let failing: BatchFuture = Box::pin(async move { Err(err) });
            futures::stream::once(std::future::ready(failing)).boxed()
        }
    }
}

// ---------------------------------------------------------------------------
// Placeholders for the future kernel-backed (full-fidelity) reader.
// ---------------------------------------------------------------------------

/// File-tier reader backed by `delta-kernel`'s parquet handler (placeholder).
#[derive(Debug, Clone, Default)]
pub struct KernelDataFileReader;

#[async_trait::async_trait]
impl DataFileReader for KernelDataFileReader {
    async fn read_file(&self, _path: Path) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataFileReader"))
    }
}

/// Dataset-tier reader backed by `delta-kernel`'s scan engine (placeholder).
///
/// Unlike [`ParquetTableReader`], this will apply deletion vectors, partition
/// values, and column-mapping transforms — the full Delta read semantics.
#[derive(Debug, Clone, Default)]
pub struct KernelDataReader;

#[async_trait::async_trait]
impl DeltaDataReader for KernelDataReader {
    async fn read(&self, _options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataReader"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::create::CreateBuilder;
    use crate::writer::DeltaWriter as _;
    use crate::writer::RecordBatchWriter;
    use crate::writer::test_utils::{get_delta_schema, get_record_batch};

    #[tokio::test]
    async fn test_parquet_table_reader_reads_all_files() {
        // Prove the read traits end-to-end: write a plain (unpartitioned, no-DV,
        // no-column-mapping) table, then read every parquet file back with no
        // predicate through the DataFusion-free reader.
        let schema = get_delta_schema();
        let tmp = tempfile::tempdir().unwrap();
        let mut table = CreateBuilder::new()
            .with_location(tmp.path().to_str().unwrap())
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();

        let batch = get_record_batch(None, false);
        let expected_rows = batch.num_rows();
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer.write(batch).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();

        let reader = ParquetTableReader::try_new(&table).await.unwrap();
        let stream = reader.read(ReadOptions::default()).await.unwrap();
        let batches: Vec<_> = stream.buffered(4).try_collect().await.unwrap();

        assert!(!batches.is_empty(), "expected at least one batch");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, expected_rows);
    }

    #[tokio::test]
    async fn test_parquet_table_reader_rejects_partitioned_table() {
        // The raw reader cannot reconstruct partition columns, so it must refuse
        // a partitioned table rather than return rows missing those columns.
        let schema = get_delta_schema();
        let tmp = tempfile::tempdir().unwrap();
        let table = CreateBuilder::new()
            .with_location(tmp.path().to_str().unwrap())
            .with_columns(schema.fields().cloned())
            .with_partition_columns(vec!["modified".to_string()])
            .await
            .unwrap();

        let err = ParquetTableReader::try_new(&table).await.unwrap_err();
        assert!(matches!(err, DeltaTableError::Generic(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn test_kernel_readers_not_yet_implemented() {
        // The kernel-backed readers are placeholders; both report a clear
        // not-yet-implemented error rather than panicking or returning empty data.
        let file_err = KernelDataFileReader
            .read_file(Path::from("data/file.parquet"))
            .await
            .err()
            .expect("KernelDataFileReader is a placeholder and must error");
        assert!(
            matches!(file_err, DeltaTableError::Generic(_)),
            "got: {file_err:?}"
        );

        let table_err = KernelDataReader
            .read(ReadOptions::default())
            .await
            .err()
            .expect("KernelDataReader is a placeholder and must error");
        assert!(
            matches!(table_err, DeltaTableError::Generic(_)),
            "got: {table_err:?}"
        );
    }

    #[tokio::test]
    async fn test_parquet_file_reader_reads_single_file() {
        // Exercise the file-tier reader's `read_file` (size-unknown) path directly,
        // independent of the dataset-tier reader.
        let schema = get_delta_schema();
        let tmp = tempfile::tempdir().unwrap();
        let mut table = CreateBuilder::new()
            .with_location(tmp.path().to_str().unwrap())
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();

        let batch = get_record_batch(None, false);
        let expected_rows = batch.num_rows();
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer.write(batch).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();

        // Locate a data file in the table root.
        let store = table.object_store();
        let mut listing = store.list(None);
        let mut data_file = None;
        while let Some(meta) = listing.next().await {
            let location = meta.unwrap().location;
            if location.filename().is_some_and(|f| f.ends_with(".parquet")) {
                data_file = Some(location);
                break;
            }
        }
        let path = data_file.expect("table should have a parquet data file");

        let reader = ParquetFileReader::new(store);
        let stream = reader.read_file(path).await.unwrap();
        let batches: Vec<_> = stream.buffered(2).try_collect().await.unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, expected_rows);
    }

    #[tokio::test]
    async fn test_parquet_table_reader_rejects_projection() {
        // The basic reader has no pushdown yet, so it rejects projection/limit
        // rather than silently returning unfiltered data.
        let schema = get_delta_schema();
        let tmp = tempfile::tempdir().unwrap();
        let mut table = CreateBuilder::new()
            .with_location(tmp.path().to_str().unwrap())
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer.write(get_record_batch(None, false)).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();

        let reader = ParquetTableReader::try_new(&table).await.unwrap();
        let options = ReadOptions::default().with_projection(vec!["id".to_string()]);
        let err = reader
            .read(options)
            .await
            .err()
            .expect("the basic reader should reject projection");
        assert!(matches!(err, DeltaTableError::Generic(_)), "got: {err:?}");
    }
}
