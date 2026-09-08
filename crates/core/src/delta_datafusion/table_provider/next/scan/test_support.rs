//! Parquet fixtures with known row positions for scan regression tests.
//! Expected rows are computed from the fixture definition.

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};
use std::{collections::BTreeSet, error::Error, fs, sync::Arc};
use tempfile::TempDir;
use url::Url;

type FixtureResult<T> = Result<T, Box<dyn Error>>;

pub struct FileSpec {
    pub groups: Vec<usize>,
    pub deleted: Option<BTreeSet<u64>>,
    pub log_stats: bool,
}

pub struct Fixture {
    pub directory: TempDir,
    pub coordinates: Vec<(usize, u64, i64, i64)>,
    pub deleted: Vec<Option<BTreeSet<u64>>>,
    adds: Vec<Value>,
    version: usize,
}

impl Fixture {
    pub fn new(specs: Vec<FileSpec>) -> FixtureResult<Self> {
        const PAGE_ROWS: usize = 8;
        let directory = tempfile::tempdir()?;
        fs::create_dir(directory.path().join("_delta_log"))?;
        let has_dvs = specs.iter().any(|f| f.deleted.is_some());
        let protocol = if has_dvs {
            json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}})
        } else {
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}})
        };
        let logical_schema = json!({"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"long","nullable":false,"metadata":{}}]});
        let metadata = json!({"metaData":{"id":"c75e5410-d53e-4fb3-b848-2e595cda0001","format":{"provider":"parquet","options":{}},"schemaString":logical_schema.to_string(),"partitionColumns":[],"configuration":{}}});
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let mut fixture = Self {
            directory,
            coordinates: Vec::new(),
            deleted: Vec::new(),
            adds: Vec::new(),
            version: 0,
        };
        let mut global_id = 0_i64;
        for (file_id, spec) in specs.into_iter().enumerate() {
            let path = format!("part-{file_id}.parquet");
            let file = fs::File::create(fixture.directory.path().join(&path))?;
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(None)
                .set_data_page_row_count_limit(PAGE_ROWS)
                .set_write_batch_size(4)
                .build();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))?;
            let first_id = global_id;
            let mut position = 0;
            let rows: usize = spec.groups.iter().sum();
            for count in spec.groups {
                let mut ids = Vec::with_capacity(count);
                let mut values = Vec::with_capacity(count);
                for row in position..position + count {
                    ids.push(global_id);
                    let value = -(row as i64);
                    values.push(value);
                    fixture
                        .coordinates
                        .push((file_id, row as u64, global_id, value));
                    global_id += 1;
                }
                writer.write(&RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(Int64Array::from(values)),
                    ],
                )?)?;
                writer.flush()?;
                position += count;
            }
            writer.close()?;
            let size = fs::metadata(fixture.directory.path().join(&path))?.len();
            let mut add = json!({"path":path,"partitionValues":{},"size":size,"modificationTime":0,"dataChange":true});
            if spec.log_stats {
                add["stats"] = json!(json!({"numRecords":rows,"minValues":{"id":first_id,"value":-(rows as i64 - 1)},"maxValues":{"id":global_id - 1,"value":0},"nullCount":{"id":0,"value":0}}).to_string());
            }
            if let Some(deleted) = &spec.deleted {
                add["deletionVector"] = fixture.write_dv(file_id, 0, deleted)?;
            }
            fixture.deleted.push(spec.deleted);
            fixture.adds.push(add);
        }
        let mut actions = vec![protocol, metadata];
        actions.extend(fixture.adds.iter().map(|add| json!({"add":add})));
        fixture.write_log(0, actions)?;
        Ok(fixture)
    }

    pub fn url(&self) -> Url {
        Url::from_directory_path(self.directory.path()).unwrap()
    }

    /// Rewrite only the footer, as a writer that omits the optional ordinals would.
    pub fn remove_row_group_ordinals(&mut self, file_id: usize) -> FixtureResult<()> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataWriter, RowGroupMetaData};
        use parquet::file::writer::TrackedWrite;
        use std::io::Write;

        let path = self
            .directory
            .path()
            .join(format!("part-{file_id}.parquet"));
        let bytes = fs::read(&path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.clone()))?;
        let original = reader.metadata();
        let groups = original
            .row_groups()
            .iter()
            .map(|group| {
                let mut builder = RowGroupMetaData::builder(group.schema_descr_ptr())
                    .set_column_metadata(group.columns().to_vec())
                    .set_num_rows(group.num_rows())
                    .set_total_byte_size(group.total_byte_size())
                    .set_sorting_columns(group.sorting_columns().cloned());
                if let Some(offset) = group.file_offset() {
                    builder = builder.set_file_offset(offset);
                }
                builder.build()
            })
            .collect::<parquet::errors::Result<Vec<_>>>()?;
        assert!(groups.iter().all(|group| group.ordinal().is_none()));
        let metadata = ParquetMetaData::new(original.file_metadata().clone(), groups);
        let footer_len =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into()?) as usize;
        let mut rewritten = Vec::new();
        let mut output = TrackedWrite::new(&mut rewritten);
        output.write_all(&bytes[..bytes.len() - 8 - footer_len])?;
        ParquetMetaDataWriter::new_with_tracked(output, &metadata).finish()?;
        fs::write(path, &rewritten)?;
        self.adds[file_id]["size"] = json!(rewritten.len());
        let log = self
            .directory
            .path()
            .join("_delta_log/00000000000000000000.json");
        let mut actions = fs::read_to_string(&log)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let path = format!("part-{file_id}.parquet");
        for action in &mut actions {
            if action.get("add").and_then(|add| add["path"].as_str()) == Some(&path) {
                action["add"] = self.adds[file_id].clone();
            }
        }
        self.write_log(0, actions)
    }

    pub fn live_coordinates(&self) -> Vec<(usize, u64, i64, i64)> {
        self.coordinates
            .iter()
            .copied()
            .filter(|(file, position, _, _)| {
                !self.deleted[*file]
                    .as_ref()
                    .is_some_and(|deleted| deleted.contains(position))
            })
            .collect()
    }

    pub fn replace_visibility(
        &mut self,
        file_id: usize,
        deleted: BTreeSet<u64>,
    ) -> FixtureResult<()> {
        self.version += 1;
        let old = self.adds[file_id].clone();
        let mut remove = old.clone();
        remove["deletionTimestamp"] = json!(self.version);
        remove["extendedFileMetadata"] = json!(true);
        let mut add = old;
        add["deletionVector"] = self.write_dv(file_id, self.version, &deleted)?;
        self.write_log(
            self.version,
            vec![json!({"remove":remove}), json!({"add":add})],
        )?;
        self.adds[file_id] = add;
        self.deleted[file_id] = Some(deleted);
        Ok(())
    }

    fn write_dv(
        &self,
        file: usize,
        version: usize,
        positions: &BTreeSet<u64>,
    ) -> FixtureResult<Value> {
        let path = self
            .directory
            .path()
            .join(format!("dv-{file}-{version}.bin"));
        let mut output = fs::File::create(&path)?;
        let mut writer = StreamingDeletionVectorWriter::new(&mut output);
        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes(positions.iter().copied());
        let written = writer.write_deletion_vector(dv)?;
        writer.finalize()?;
        Ok(
            json!({"storageType":"p","pathOrInlineDv":Url::from_file_path(path).unwrap().as_str(),"offset":written.offset,"sizeInBytes":written.size_in_bytes,"cardinality":positions.len()}),
        )
    }

    fn write_log(&self, version: usize, actions: Vec<Value>) -> FixtureResult<()> {
        fs::write(
            self.directory
                .path()
                .join("_delta_log")
                .join(format!("{version:020}.json")),
            actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        Ok(())
    }
}

/// Pause Parquet requests until their futures are cancelled.
#[derive(Debug)]
pub struct PausedStore {
    inner: Arc<dyn object_store::ObjectStore>,
    pub paused: std::sync::atomic::AtomicBool,
    pub active: std::sync::atomic::AtomicUsize,
    pub entered: tokio::sync::Notify,
    pub changed: tokio::sync::Notify,
}

impl PausedStore {
    pub fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            paused: false.into(),
            active: 0.into(),
            entered: Default::default(),
            changed: Default::default(),
        }
    }
}

impl std::fmt::Display for PausedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paused test store")
    }
}

struct PendingRead<'a>(&'a PausedStore);
impl Drop for PendingRead<'_> {
    fn drop(&mut self) {
        self.0
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for PausedStore {
    async fn get_opts(
        &self,
        path: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        use std::sync::atomic::Ordering;
        self.active.fetch_add(1, Ordering::SeqCst);
        let _pending = PendingRead(self);
        if path.as_ref().ends_with(".parquet")
            && self.paused.load(std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.get_opts(path, options).await
    }
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        path: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(path)
    }
    async fn list_with_delimiter(
        &self,
        path: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(path).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
