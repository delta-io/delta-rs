#![allow(unused)]
use std::collections::HashMap;

use super::prepare_commit;
use crate::kernel::{
    Action, Add, CommitInfo, DataType, Metadata, PrimitiveType, Protocol, Remove, StructField,
    StructType,
};
use crate::operations::transaction::PROTOCOL;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::table::state::DeltaTableState;
use crate::{DeltaTable, DeltaTableBuilder};

pub fn create_add_action(
    path: impl Into<String>,
    data_change: bool,
    stats: Option<String>,
) -> Action {
    Action::Add(Add {
        path: path.into(),
        size: 100,
        data_change,
        stats,
        modification_time: -1,
        partition_values: Default::default(),
        stats_parsed: None,
        base_row_id: None,
        default_row_commit_version: None,
        tags: None,
        deletion_vector: None,
        clustering_provider: None,
    })
}

pub fn create_remove_action(path: impl Into<String>, data_change: bool) -> Action {
    Action::Remove(Remove {
        path: path.into(),
        data_change,
        size: None,
        deletion_timestamp: None,
        deletion_vector: None,
        partition_values: Default::default(),
        extended_file_metadata: None,
        base_row_id: None,
        default_row_commit_version: None,
        tags: None,
    })
}

pub fn create_protocol_action(max_reader: Option<i32>, max_writer: Option<i32>) -> Action {
    let protocol = Protocol {
        min_reader_version: max_reader.unwrap_or(PROTOCOL.default_reader_version()),
        min_writer_version: max_writer.unwrap_or(PROTOCOL.default_writer_version()),
        writer_features: None,
        reader_features: None,
    };
    Action::Protocol(protocol)
}

pub fn create_metadata_action(
    parttiton_columns: Option<Vec<String>>,
    configuration: Option<HashMap<String, Option<String>>>,
) -> Action {
    let table_schema = StructType::new(vec![
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "modified".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]);
    Action::Metadata(
        Metadata::try_new(
            table_schema,
            parttiton_columns.unwrap_or_default(),
            configuration.unwrap_or_default(),
        )
        .unwrap(),
    )
}

pub fn init_table_actions(configuration: Option<HashMap<String, Option<String>>>) -> Vec<Action> {
    let raw = r#"
        {
            "timestamp": 1670892998177,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[\"c1\",\"c2\"]"
            },
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": "3",
                "numOutputRows": "3",
                "numOutputBytes": "1356"
            },
            "engineInfo": "Apache-Spark/3.3.1 Delta-Lake/2.2.0",
            "txnId": "046a258f-45e3-4657-b0bf-abfb0f76681c"
        }"#;

    let commit_info = serde_json::from_str::<CommitInfo>(raw).unwrap();
    vec![
        Action::CommitInfo(commit_info),
        create_protocol_action(None, None),
        create_metadata_action(None, configuration),
    ]
}

pub async fn create_initialized_table(
    partition_cols: &[String],
    configuration: Option<HashMap<String, Option<String>>>,
) -> DeltaTable {
    let log_store = DeltaTableBuilder::from_uri("memory://")
        .build_storage()
        .unwrap();
    let table_schema = StructType::new(vec![
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "modified".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]);
    let state = DeltaTableState::from_actions(init_table_actions(None)).unwrap();
    let operation = DeltaOperation::Create {
        mode: SaveMode::ErrorIfExists,
        location: "location".into(),
        protocol: Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
            writer_features: None,
            reader_features: None,
        },
        metadata: Metadata {
            id: uuid::Uuid::new_v4().to_string(),
            name: None,
            description: None,
            format: Default::default(),
            schema_string: serde_json::to_string(&table_schema).unwrap(),
            partition_columns: partition_cols.to_vec(),
            configuration: configuration.unwrap_or_default(),
            created_time: Some(chrono::Utc::now().timestamp_millis()),
        },
    };
    let actions = init_table_actions(None);
    let prepared_commit = prepare_commit(
        log_store.object_store().as_ref(),
        &operation,
        &actions,
        None,
    )
    .await
    .unwrap();

    log_store
        .write_commit_entry(0, &prepared_commit)
        .await
        .unwrap();
    DeltaTable::new_with_state(log_store, state)
}
