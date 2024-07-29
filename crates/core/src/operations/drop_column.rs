//! Drop a column from the table

use delta_kernel::schema::DataType;
use delta_kernel::schema::StructType;
use futures::future::BoxFuture;
use std::collections::HashMap;

use itertools::Itertools;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::transaction::{CommitBuilder, CommitProperties};

use crate::kernel::StructField;
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Add new columns and/or nested fields to a table
pub struct DropColumnBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Fields to drop from the schema
    fields: Option<Vec<String>>,
    /// Raise if constraint doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl super::Operation<()> for DropColumnBuilder {}

impl DropColumnBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            raise_if_not_exists: true,
            fields: None,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the fields to be removed
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = String> + Clone) -> Self {
        self.fields = Some(fields.into_iter().collect());
        self
    }
    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Specify if you want to raise if the specified column does not exist
    pub fn with_raise_if_not_exists(mut self, raise: bool) -> Self {
        self.raise_if_not_exists = raise;
        self
    }
}

impl std::future::IntoFuture for DropColumnBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let dialect = GenericDialect {};
            let mut metadata = this.snapshot.metadata().clone();
            let fields = match this.fields {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
            };

            let table_schema = this.snapshot.schema();
            let mut fields_not_found = HashMap::new();

            let fields_map = fields
                .iter()
                .map(|field_name| {
                    let identifiers = Parser::new(&dialect)
                        .try_with_sql(field_name.as_str())
                        .unwrap()
                        .parse_multipart_identifier()
                        .unwrap()
                        .iter()
                        .map(|v| v.value.to_owned())
                        .collect_vec();
                    // Root field, field path
                    (identifiers[0].clone(), identifiers)
                })
                .collect::<HashMap<String, Vec<String>>>();

            let new_table_schema = StructType::new(
                table_schema
                    .fields()
                    .filter_map(|field| {
                        if let Some(identifiers) = fields_map.get(field.name()) {
                            if identifiers.len() == 1 {
                                None
                            } else {
                                drop_nested_fields(field, &identifiers[1..], &mut fields_not_found)
                            }
                        } else {
                            Some(field.clone())
                        }
                    })
                    .collect::<Vec<StructField>>(),
            );

            let mut not_found: Vec<String> = fields_not_found
                .iter()
                .map(|(key, value)| format!("{}.{}", key, value.join(".")))
                .collect();

            // Catch root fields that do not exist
            not_found.append(
                &mut fields_map
                    .values()
                    .filter_map(|v| {
                        if v.len() == 1 {
                            if table_schema.field(&v[0]).is_none() {
                                Some(v[0].clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect_vec(),
            );

            if !not_found.is_empty() && this.raise_if_not_exists {
                return Err(DeltaTableError::Generic(format!(
                    "Column(s) with name: {:#?} doesn't exist",
                    &not_found
                )));
            }

            let operation = DeltaOperation::DropColumn { fields };

            metadata.schema_string = serde_json::to_string(&new_table_schema)?;

            let actions = vec![metadata.into()];

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .build(Some(&this.snapshot), this.log_store.clone(), operation)
                .await?;

            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}

fn drop_nested_fields<'a>(
    field: &StructField,
    path: &'a [String],
    unmatched_paths: &mut HashMap<String, &'a [String]>,
) -> Option<StructField> {
    match field.data_type() {
        DataType::Struct(inner_struct) => {
            let remaining_fields = inner_struct
                .fields()
                .filter_map(|nested_field| {
                    if nested_field.name() == &path[0] {
                        if path.len() > 1 {
                            drop_nested_fields(nested_field, &path[1..], unmatched_paths)
                        } else {
                            None
                        }
                    } else {
                        Some(nested_field.clone())
                    }
                })
                .collect::<Vec<StructField>>();

            // If field was the same, we push the missing paths recursively into the hashmap
            // we also remove the subpaths from the hashmap if we see that it's a subset
            // (might need to find better way )
            if remaining_fields.eq(&inner_struct.fields().cloned().collect_vec()) {
                if let Some(part_path) = unmatched_paths.get(&path[0]) {
                    if part_path == &&path[1..] {
                        unmatched_paths.remove(&path[0]);
                    }
                }
                unmatched_paths.insert(field.name().to_owned(), path);
            };

            if remaining_fields.is_empty() {
                None
            } else {
                Some(StructField::new(
                    field.name(),
                    DataType::Struct(Box::new(StructType::new(remaining_fields))),
                    field.is_nullable(),
                ))
            }
        }
        _ => Some(field.clone()),
    }
}
