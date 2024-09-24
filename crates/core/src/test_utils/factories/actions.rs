use std::collections::HashMap;

use arrow_array::*;
use chrono::Utc;
use delta_kernel::schema::{DataType, PrimitiveType};
use object_store::path::Path;
use object_store::ObjectMeta;

use super::{get_parquet_bytes, DataFactory, FileStats};
use crate::kernel::arrow::extract::{self as ex};
use crate::kernel::partitions_schema;
use crate::kernel::{Add, Metadata, Protocol, ReaderFeatures, Remove, StructType, WriterFeatures};
use crate::operations::transaction::PROTOCOL;

pub struct ActionFactory;

impl ActionFactory {
    pub fn add_raw(
        meta: ObjectMeta,
        stats: FileStats,
        partition_values: HashMap<String, Option<String>>,
        data_change: bool,
    ) -> Add {
        Add {
            path: meta.location.to_string(),
            size: meta.size as i64,
            partition_values,
            data_change,
            modification_time: meta.last_modified.timestamp_millis(),
            stats: serde_json::to_string(&stats).ok(),
            tags: Some(HashMap::new()),
            default_row_commit_version: None,
            deletion_vector: None,
            base_row_id: None,
            clustering_provider: None,
            stats_parsed: None,
        }
    }

    pub fn add(
        schema: &StructType,
        bounds: HashMap<&str, (&str, &str)>,
        partition_columns: Vec<String>,
        data_change: bool,
    ) -> Add {
        let partitions_schema = partitions_schema(&schema, &partition_columns).unwrap();
        let partition_values = if let Some(p_schema) = partitions_schema {
            let batch = DataFactory::record_batch(&p_schema, 1, &bounds).unwrap();
            p_schema
                .fields()
                .map(|f| {
                    let value = match f.data_type() {
                        DataType::Primitive(PrimitiveType::String) => {
                            let arr =
                                ex::extract_and_cast::<StringArray>(&batch, f.name()).unwrap();
                            Some(arr.value(0).to_string())
                        }
                        DataType::Primitive(PrimitiveType::Integer) => {
                            let arr = ex::extract_and_cast::<Int32Array>(&batch, f.name()).unwrap();
                            Some(arr.value(0).to_string())
                        }
                        DataType::Primitive(PrimitiveType::Long) => {
                            let arr = ex::extract_and_cast::<Int64Array>(&batch, f.name()).unwrap();
                            Some(arr.value(0).to_string())
                        }
                        _ => unimplemented!(),
                    };
                    (f.name().to_owned(), value)
                })
                .collect()
        } else {
            HashMap::new()
        };

        let data_schema = StructType::new(
            schema
                .fields()
                .filter(|f| !partition_columns.contains(f.name()))
                .cloned()
                .collect(),
        );

        let batch = DataFactory::record_batch(&data_schema, 10, &bounds).unwrap();
        let stats = DataFactory::file_stats(&batch).unwrap();
        let path = Path::from(generate_file_name());
        let data = get_parquet_bytes(&batch).unwrap();
        let meta = ObjectMeta {
            location: path.clone(),
            size: data.len(),
            last_modified: Utc::now(),
            e_tag: None,
            version: None,
        };
        ActionFactory::add_raw(meta, stats, partition_values, data_change)
    }

    pub fn remove(add: &Add, data_change: bool) -> Remove {
        add_as_remove(add, data_change)
    }

    pub fn protocol(
        max_reader: Option<i32>,
        max_writer: Option<i32>,
        reader_features: Option<impl IntoIterator<Item = ReaderFeatures>>,
        writer_features: Option<impl IntoIterator<Item = WriterFeatures>>,
    ) -> Protocol {
        Protocol {
            min_reader_version: max_reader.unwrap_or(PROTOCOL.default_reader_version()),
            min_writer_version: max_writer.unwrap_or(PROTOCOL.default_writer_version()),
            writer_features: writer_features.map(|i| i.into_iter().collect()),
            reader_features: reader_features.map(|i| i.into_iter().collect()),
        }
    }

    pub fn metadata(
        schema: &StructType,
        partition_columns: Option<impl IntoIterator<Item = impl ToString>>,
        configuration: Option<HashMap<String, Option<String>>>,
    ) -> Metadata {
        Metadata {
            id: uuid::Uuid::new_v4().hyphenated().to_string(),
            format: Default::default(),
            schema_string: serde_json::to_string(schema).unwrap(),
            partition_columns: partition_columns
                .map(|i| i.into_iter().map(|c| c.to_string()).collect())
                .unwrap_or_default(),
            configuration: configuration.unwrap_or_default(),
            name: None,
            description: None,
            created_time: Some(Utc::now().timestamp_millis()),
        }
    }
}

pub fn add_as_remove(add: &Add, data_change: bool) -> Remove {
    Remove {
        path: add.path.clone(),
        data_change,
        deletion_timestamp: Some(Utc::now().timestamp_millis()),
        size: Some(add.size),
        extended_file_metadata: Some(true),
        partition_values: Some(add.partition_values.clone()),
        tags: add.tags.clone(),
        deletion_vector: add.deletion_vector.clone(),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
    }
}

fn generate_file_name() -> String {
    let file_name = uuid::Uuid::new_v4().hyphenated().to_string();
    format!("part-0001-{}.parquet", file_name)
}
