//! Deterministic fixture coordinates shared by qualification and the Criterion harness.
//! Expected visibility comes from the generator's declared positions, never reader output.

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
    pub rows: usize,
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
    pub fn new(specs: Vec<FileSpec>, page_rows: usize) -> FixtureResult<Self> {
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
                .set_data_page_row_count_limit(page_rows)
                .set_write_batch_size(page_rows.min(4))
                .build();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))?;
            let first_id = global_id;
            let mut position = 0;
            let mut group = 0;
            while position < spec.rows {
                let count = spec.groups[group % spec.groups.len()].min(spec.rows - position);
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
                group += 1;
            }
            writer.close()?;
            let size = fs::metadata(fixture.directory.path().join(&path))?.len();
            let mut add = json!({"path":path,"partitionValues":{},"size":size,"modificationTime":0,"dataChange":true});
            if spec.log_stats {
                add["stats"] = json!(json!({"numRecords":spec.rows,"minValues":{"id":first_id,"value":-(spec.rows as i64 - 1)},"maxValues":{"id":global_id - 1,"value":0},"nullCount":{"id":0,"value":0}}).to_string());
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
