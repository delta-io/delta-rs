#![allow(unused)]
use super::{prepare_commit, try_commit_transaction, CommitInfo};
use crate::action::{Action, Add, DeltaOperation, MetaData, Protocol, Remove, SaveMode};
use crate::table_state::DeltaTableState;
use crate::{
    DeltaTable, DeltaTableBuilder, DeltaTableMetaData, Schema, SchemaDataType, SchemaField,
};
use std::collections::HashMap;

pub fn create_add_action(
    path: impl Into<String>,
    data_change: bool,
    stats: Option<String>,
) -> Action {
    Action::add(Add {
        path: path.into(),
        size: 100,
        data_change,
        stats,
        ..Default::default()
    })
}

pub fn create_remove_action(path: impl Into<String>, data_change: bool) -> Action {
    Action::remove(Remove {
        path: path.into(),
        data_change,
        ..Default::default()
    })
}

pub fn create_protocol_action(max_reader: Option<i32>, max_writer: Option<i32>) -> Action {
    let protocol = Protocol {
        min_reader_version: max_reader.unwrap_or(crate::operations::MAX_SUPPORTED_READER_VERSION),
        min_writer_version: max_writer.unwrap_or(crate::operations::MAX_SUPPORTED_WRITER_VERSION),
    };
    Action::protocol(protocol)
}

pub fn create_metadata_action(
    parttiton_columns: Option<Vec<String>>,
    configuration: Option<HashMap<String, Option<String>>>,
) -> Action {
    let table_schema = Schema::new(vec![
        SchemaField::new(
            "id".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "value".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "modified".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
    ]);
    let metadata = DeltaTableMetaData::new(
        None,
        None,
        None,
        table_schema,
        parttiton_columns.unwrap_or_default(),
        configuration.unwrap_or_default(),
    );
    Action::metaData(MetaData::try_from(metadata).unwrap())
}

pub fn init_table_actions() -> Vec<Action> {
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
        Action::commitInfo(commit_info),
        create_protocol_action(None, None),
        create_metadata_action(None, None),
    ]
}

pub async fn create_initialized_table(
    partition_cols: &[String],
    configuration: Option<HashMap<String, Option<String>>>,
) -> DeltaTable {
    let storage = DeltaTableBuilder::from_uri("memory://")
        .build_storage()
        .unwrap();
    let table_schema = Schema::new(vec![
        SchemaField::new(
            "id".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "value".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "modified".to_string(),
            SchemaDataType::primitive("string".to_string()),
            true,
            HashMap::new(),
        ),
    ]);
    let state = DeltaTableState::from_actions(init_table_actions(), 0).unwrap();
    let operation = DeltaOperation::Create {
        mode: SaveMode::ErrorIfExists,
        location: "location".into(),
        protocol: Protocol {
            min_reader_version: 1,
            min_writer_version: 1,
        },
        metadata: DeltaTableMetaData::new(
            None,
            None,
            None,
            table_schema,
            partition_cols.to_vec(),
            configuration.unwrap_or_default(),
        ),
    };
    let actions = init_table_actions();
    let prepared_commit = prepare_commit(storage.as_ref(), &operation, &actions, None)
        .await
        .unwrap();
    try_commit_transaction(storage.as_ref(), &prepared_commit, 0)
        .await
        .unwrap();
    DeltaTable::new_with_state(storage, state)
}
