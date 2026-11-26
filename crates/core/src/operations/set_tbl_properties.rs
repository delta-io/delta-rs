//! Set table properties on a table

use std::borrow::Cow;
use std::collections::HashMap;

use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::{
    ColumnMappingMode, assign_column_mapping_metadata, find_max_column_id_in_schema,
    validate_schema_column_mapping,
};
use delta_kernel::transforms::{SchemaTransform, transform_output_type};
use futures::future::BoxFuture;

use crate::DeltaResult;
use crate::DeltaTable;
use crate::errors::DeltaTableError;
use crate::kernel::transaction::CommitProperties;
use crate::kernel::{
    Action, ColumnMetadataKey, EagerSnapshot, Metadata, MetadataExt as _, MetadataValue,
    ProtocolExt as _, SnapshotMetadataRef, StructField, StructType, resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::operations::commit_actions_in_scope;
use crate::protocol::DeltaOperation;
use crate::table::config::TableProperty;

/// Remove constraints from the table
pub struct SetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Name of the property
    properties: HashMap<String, String>,
    /// Raise if property doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl SetTablePropertiesBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            properties: HashMap::new(),
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the properties to be removed
    pub fn with_properties(mut self, table_properties: HashMap<String, String>) -> Self {
        self.properties = table_properties;
        self
    }

    /// Specify if you want to raise if the property does not exist
    pub fn with_raise_if_not_exists(mut self, raise: bool) -> Self {
        self.raise_if_not_exists = raise;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

/// Sets the physical name of every field to the current name of the field.
///
/// Data files written without column mapping store each column under its logical name, so that
/// name must stay the physical name when column mapping is enabled.
struct LogicalNamesAsPhysicalNames;

impl<'a> SchemaTransform<'a> for LogicalNamesAsPhysicalNames {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Cow<'a, StructField> {
        let mut field = self.recurse_into_struct_field(field).into_owned();
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String(field.name.clone()),
        );
        Cow::Owned(field)
    }

    // The fields inside a variant are not table columns and take no column mapping metadata.
    fn transform_variant(&mut self, stype: &'a StructType) -> Cow<'a, StructType> {
        Cow::Borrowed(stype)
    }
}

/// Applies a requested `delta.columnMapping.mode` to the table metadata.
///
/// An existing table can only change from `none` to `name`. Each field keeps its current name as
/// the physical name and gets a column id, so the existing data files stay readable. The `id`
/// mode is rejected because the existing data files hold no Parquet field ids.
fn apply_column_mapping_mode(
    table_configuration: &TableConfiguration,
    metadata: Metadata,
    requested: &str,
) -> DeltaResult<Metadata> {
    let requested_mode = requested.parse::<ColumnMappingMode>().map_err(|_| {
        DeltaTableError::Generic(format!("Invalid delta.columnMapping.mode: {requested}"))
    })?;
    let current_mode = table_configuration.column_mapping_mode();

    match (current_mode, requested_mode) {
        _ if current_mode == requested_mode => Ok(metadata),
        (ColumnMappingMode::None, ColumnMappingMode::Name) => {
            let table_schema = table_configuration.logical_schema();
            let schema = LogicalNamesAsPhysicalNames.transform_struct(table_schema.as_ref());

            let mut max_id = find_max_column_id_in_schema(&schema)
                .max(
                    table_configuration
                        .table_properties()
                        .column_mapping_max_column_id,
                )
                .unwrap_or(0);
            let schema = assign_column_mapping_metadata(&schema, &mut max_id, false)?;
            validate_schema_column_mapping(&schema, requested_mode)?;

            Ok(metadata.with_schema(&schema)?.add_config_key(
                "delta.columnMapping.maxColumnId".to_string(),
                max_id.to_string(),
            )?)
        }
        _ => Err(DeltaTableError::Generic(format!(
            "Changing delta.columnMapping.mode from {current_mode:?} to {requested_mode:?} is not \
             supported. An existing table can only change from None to Name."
        ))),
    }
}

fn plan_set_table_properties_actions(
    snapshot: SnapshotMetadataRef<'_>,
    properties: HashMap<String, String>,
    raise_if_not_exists: bool,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    let mut metadata = snapshot.metadata.clone();
    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_properties_to_protocol(&properties, raise_if_not_exists)?;

    for (key, value) in &properties {
        metadata = metadata.add_config_key(key.clone(), value.to_string())?;
    }

    if let Some(mode) = properties.get(TableProperty::ColumnMappingMode.as_ref()) {
        metadata = apply_column_mapping_mode(snapshot.table_configuration, metadata, mode)?;
    }

    let final_protocol = new_protocol.move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::SetTableProperties { properties };

    let mut actions = vec![Action::Metadata(metadata)];

    if current_protocol.ne(&final_protocol) {
        actions.push(Action::Protocol(final_protocol));
    }

    Ok((actions, operation))
}

impl std::future::IntoFuture for SetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot.clone(), None).await?;

            let properties = this.properties;
            let (actions, operation) = plan_set_table_properties_actions(
                snapshot.snapshot().metadata_state(),
                properties,
                this.raise_if_not_exists,
            )?;

            commit_actions_in_scope(
                &this.log_store,
                &snapshot,
                this.commit_properties,
                actions,
                operation,
            )
            .await
        })
    }
}

#[cfg(test)]
/// Tests for the set-table-properties operation.
pub mod tests {
    use crate::DeltaTable;
    use crate::kernel::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
    };
    use crate::writer::test_utils::create_initialized_table;
    use delta_kernel::table_features::TableFeature;
    use rstest::rstest;
    use std::collections::HashMap;
    use tempfile::tempdir;

    /// Verify that setting table properties is persisted to table metadata.
    #[tokio::test]
    async fn test_set_tbl_properties() -> crate::DeltaResult<()> {
        let temp_loc = tempdir()?;
        let ops = create_initialized_table(temp_loc.path().to_str().unwrap(), &[]).await;

        // Test setting properties that enable features (should work with proper handling)
        let props = HashMap::from([("delta.enableChangeDataFeed".to_string(), "true".to_string())]);
        ops.set_tbl_properties().with_properties(props).await?;

        Ok(())
    }

    /// Test setting protocol versions with features properly handled.
    #[tokio::test]
    async fn test_set_protocol_versions_with_features() -> crate::DeltaResult<()> {
        let temp_loc = tempdir()?;
        let ops = create_initialized_table(temp_loc.path().to_str().unwrap(), &[]).await;

        // Test enabling features that automatically set appropriate protocol versions
        let props = HashMap::from([(
            "delta.enableDeletionVectors".to_string(),
            "true".to_string(),
        )]);
        ops.set_tbl_properties().with_properties(props).await?;

        Ok(())
    }

    /// If a user attempts to set a newer minWriterVersion e.g. 7, then the protocol must add
    /// writer and reader features to the table.
    #[tokio::test]
    async fn test_increase_protocol_versions() -> crate::DeltaResult<()> {
        let temp_loc = tempdir()?;
        let ops = create_initialized_table(temp_loc.path().to_str().unwrap(), &[]).await;

        // Test enabling features that automatically set appropriate protocol versions
        let props = HashMap::from([("delta.minWriterVersion".to_string(), "7".to_string())]);
        ops.set_tbl_properties().with_properties(props).await?;

        Ok(())
    }

    async fn set_column_mapping_mode(
        table: DeltaTable,
        mode: &str,
    ) -> crate::DeltaResult<DeltaTable> {
        table
            .set_tbl_properties()
            .with_properties(HashMap::from([(
                "delta.columnMapping.mode".to_string(),
                mode.to_string(),
            )]))
            .await
    }

    async fn table_with(configuration: &[(&str, &str)]) -> crate::DeltaResult<DeltaTable> {
        DeltaTable::new_in_memory()
            .create()
            .with_columns([StructField::nullable("id", DataType::INTEGER)])
            .with_configuration(
                configuration
                    .iter()
                    .map(|(key, value)| (*key, Some(*value))),
            )
            .await
    }

    /// Collects the name, column id, and physical name of every field in `data_type`.
    fn column_mapping_annotations(data_type: &DataType, found: &mut Vec<(String, i64, String)>) {
        match data_type {
            DataType::Struct(fields) => {
                for field in fields.fields() {
                    let id = match field
                        .metadata()
                        .get(ColumnMetadataKey::ColumnMappingId.as_ref())
                    {
                        Some(MetadataValue::Number(id)) => *id,
                        other => panic!("field '{}' has no column id: {other:?}", field.name()),
                    };
                    let physical_name = match field
                        .metadata()
                        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                    {
                        Some(MetadataValue::String(name)) => name.clone(),
                        other => panic!("field '{}' has no physical name: {other:?}", field.name()),
                    };
                    found.push((field.name().clone(), id, physical_name));
                    column_mapping_annotations(field.data_type(), found);
                }
            }
            DataType::Array(array) => column_mapping_annotations(array.element_type(), found),
            DataType::Map(map) => {
                column_mapping_annotations(map.key_type(), found);
                column_mapping_annotations(map.value_type(), found);
            }
            _ => {}
        }
    }

    /// Enabling column mapping gives every field, nested fields included, a column id and keeps
    /// the field name as the physical name.
    #[tokio::test]
    async fn test_enable_column_mapping_keeps_names_as_physical_names() -> crate::DeltaResult<()> {
        let address = StructType::try_new([
            StructField::nullable("city", DataType::STRING),
            StructField::nullable("zip", DataType::STRING),
        ])?;
        let event = StructType::try_new([StructField::nullable("kind", DataType::STRING)])?;
        let attribute = StructType::try_new([StructField::nullable("value", DataType::LONG)])?;
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                StructField::nullable("id", DataType::INTEGER),
                StructField::nullable("address", address),
                StructField::nullable("events", ArrayType::new(event, true)),
                StructField::nullable(
                    "attributes",
                    MapType::new(DataType::STRING, attribute, true),
                ),
            ])
            .await?;

        let table = set_column_mapping_mode(table, "name").await?;

        let snapshot = table.snapshot()?;
        let configuration = snapshot.metadata().configuration();
        assert_eq!(
            configuration
                .get("delta.columnMapping.mode")
                .map(String::as_str),
            Some("name")
        );
        assert_eq!(
            configuration
                .get("delta.columnMapping.maxColumnId")
                .map(String::as_str),
            Some("8")
        );

        let mut found = Vec::new();
        column_mapping_annotations(
            &DataType::from(snapshot.schema().as_ref().clone()),
            &mut found,
        );
        let names: Vec<_> = found.iter().map(|(name, _, _)| name.as_str()).collect();
        let physical_names: Vec<_> = found.iter().map(|(_, _, name)| name.as_str()).collect();
        assert_eq!(
            names,
            [
                "id",
                "address",
                "city",
                "zip",
                "events",
                "kind",
                "attributes",
                "value"
            ]
        );
        assert_eq!(physical_names, names);
        let mut ids: Vec<_> = found.iter().map(|(_, id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, (1..=8).collect::<Vec<i64>>());

        // The current mode is accepted and leaves the schema as it is.
        let unchanged = set_column_mapping_mode(table.clone(), "name").await?;
        assert_eq!(unchanged.snapshot()?.schema(), snapshot.schema());

        Ok(())
    }

    /// Enabling column mapping upgrades each kind of protocol to one that supports it.
    #[rstest]
    #[case::legacy(&[], (2, 5), false, false)]
    #[case::table_features(&[("delta.enableDeletionVectors", "true")], (3, 7), true, true)]
    #[case::writer_features(&[("delta.minWriterVersion", "7")], (2, 7), false, true)]
    #[tokio::test]
    async fn test_enable_column_mapping_upgrades_protocol(
        #[case] configuration: &[(&str, &str)],
        #[case] versions: (i32, i32),
        #[case] reader_feature: bool,
        #[case] writer_feature: bool,
    ) -> crate::DeltaResult<()> {
        let table = set_column_mapping_mode(table_with(configuration).await?, "name").await?;

        let snapshot = table.snapshot()?;
        let protocol = snapshot.protocol();
        let has_feature = |features: Option<&[TableFeature]>| {
            features.is_some_and(|features| features.contains(&TableFeature::ColumnMapping))
        };
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            versions
        );
        assert_eq!(has_feature(protocol.reader_features()), reader_feature);
        assert_eq!(has_feature(protocol.writer_features()), writer_feature);

        Ok(())
    }

    /// An existing table can only change the column mapping mode from `none` to `name`.
    #[rstest]
    #[case::none_to_id("none", "id", "from None to Id")]
    #[case::none_to_unknown("none", "physical", "Invalid delta.columnMapping.mode")]
    #[case::name_to_none("name", "none", "from Name to None")]
    #[case::name_to_id("name", "id", "from Name to Id")]
    #[tokio::test]
    async fn test_set_column_mapping_mode_rejects_unsupported_changes(
        #[case] current: &str,
        #[case] requested: &str,
        #[case] message: &str,
    ) -> crate::DeltaResult<()> {
        let table = table_with(&[("delta.columnMapping.mode", current)]).await?;

        let err = set_column_mapping_mode(table, requested)
            .await
            .expect_err("the change must be rejected");
        assert!(err.to_string().contains(message), "{err}");

        Ok(())
    }
}
