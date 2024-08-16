//! Handle JSON messages when writing to delta tables
//!

use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use object_store::path::Path;
use parking_lot::RwLock;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde_json::Value;
use uuid::Uuid;

use crate::errors::DeltaResult;
use crate::writer::DeltaWriterError;

/// Generate the name of the file to be written
/// prefix: The location of the file to be written
/// part_count: Used the indicate that single logical partition was split into multiple physical files
///     starts at 0. Is typically used when writer splits that data due to file size constraints
pub(crate) fn next_data_path(
    prefix: &Path,
    part_count: usize,
    writer_id: &Uuid,
    writer_properties: &WriterProperties,
) -> Path {
    fn compression_to_str(compression: &Compression) -> &str {
        match compression {
            // This is to match HADOOP's convention
            // https://github.com/apache/parquet-mr/blob/c4977579ab3b149ea045a177b039f055b5408e8f/parquet-common/src/main/java/org/apache/parquet/hadoop/metadata/CompressionCodecName.java#L27-L34
            Compression::UNCOMPRESSED => "",
            Compression::SNAPPY => ".snappy",
            Compression::GZIP(_) => ".gz",
            Compression::LZO => ".lzo",
            Compression::BROTLI(_) => ".br",
            Compression::LZ4 => ".lz4",
            Compression::ZSTD(_) => ".zstd",
            Compression::LZ4_RAW => ".lz4raw",
        }
    }

    // We can not access the default column properties but the current implementation will return
    // the default compression when the column is not found
    let column_path = ColumnPath::new(Vec::new());
    let compression = writer_properties.compression(&column_path);

    let part = format!("{:0>5}", part_count);

    // TODO: what does c000 mean?
    let file_name = format!(
        "part-{}-{}-c000{}.parquet",
        part,
        writer_id,
        compression_to_str(&compression)
    );
    prefix.child(file_name)
}

/// Convert a vector of json values to a RecordBatch
pub fn record_batch_from_message(
    arrow_schema: Arc<ArrowSchema>,
    json: &[Value],
) -> DeltaResult<RecordBatch> {
    let mut decoder = ReaderBuilder::new(arrow_schema).build_decoder().unwrap();
    decoder.serialize(json)?;
    decoder
        .flush()?
        .ok_or_else(|| DeltaWriterError::EmptyRecordBatch.into())
}

/// Remove any partition related columns from the record batch
pub(crate) fn record_batch_without_partitions(
    record_batch: &RecordBatch,
    partition_columns: &[String],
) -> Result<RecordBatch, DeltaWriterError> {
    let mut non_partition_columns = Vec::new();
    for (i, field) in record_batch.schema().fields().iter().enumerate() {
        if !partition_columns.contains(field.name()) {
            non_partition_columns.push(i);
        }
    }

    Ok(record_batch.project(&non_partition_columns)?)
}

/// Arrow schema for the physical file which has partition columns removed
pub(crate) fn arrow_schema_without_partitions(
    arrow_schema: &Arc<ArrowSchema>,
    partition_columns: &[String],
) -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(
        arrow_schema
            .fields()
            .iter()
            .filter(|f| !partition_columns.contains(f.name()))
            .map(|f| f.to_owned())
            .collect::<Vec<_>>(),
    ))
}

/// An in memory buffer that allows for shared ownership and interior mutability.
/// The underlying buffer is wrapped in an `Arc` and `RwLock`, so cloning the instance
/// allows multiple owners to have access to the same underlying buffer.
#[derive(Debug, Default, Clone)]
pub struct ShareableBuffer {
    buffer: Arc<RwLock<Vec<u8>>>,
}

impl ShareableBuffer {
    /// Consumes this instance and returns the underlying buffer.
    /// Returns None if there are other references to the instance.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.buffer)
            .ok()
            .map(|lock| lock.into_inner())
    }

    /// Returns a clone of the the underlying buffer as a `Vec`.
    pub fn to_vec(&self) -> Vec<u8> {
        let inner = self.buffer.read();
        (*inner).to_vec()
    }

    /// Returns the number of bytes in the underlying buffer.
    pub fn len(&self) -> usize {
        let inner = self.buffer.read();
        (*inner).len()
    }

    /// Returns true if the underlying buffer is empty.
    pub fn is_empty(&self) -> bool {
        let inner = self.buffer.read();
        (*inner).is_empty()
    }

    /// Creates a new instance with buffer initialized from the underylying bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            buffer: Arc::new(RwLock::new(bytes.to_vec())),
        }
    }
}

impl Write for ShareableBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.buffer.write();
        (*inner).write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.buffer.write();
        (*inner).flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};

    #[test]
    fn test_data_path() {
        let prefix = Path::parse("x=0/y=0").unwrap();
        let uuid = Uuid::parse_str("02f09a3f-1624-3b1d-8409-44eff7708208").unwrap();

        // Validated against Spark
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();

        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.parquet"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.snappy.parquet"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::GZIP(GzipLevel::default()))
            .build();
        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.gz.parquet"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .build();
        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.lz4.parquet"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.zstd.parquet"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4_RAW)
            .build();
        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.lz4raw.parquet"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::BROTLI(BrotliLevel::default()))
            .build();
        assert_eq!(
            next_data_path(&prefix, 1, &uuid, &props).as_ref(),
            "x=0/y=0/part-00001-02f09a3f-1624-3b1d-8409-44eff7708208-c000.br.parquet"
        );
    }
}
