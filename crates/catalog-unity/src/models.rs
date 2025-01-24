//! Api models for databricks unity catalog APIs
use chrono::serde::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Once;

/// Error response from unity API
#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    /// The error code
    pub error_code: String,
    /// The error message
    pub message: String,
    #[serde(default)]
    pub details: Vec<ErrorDetails>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
pub struct ErrorDetails {
    #[serde(rename = "@type")]
    tpe: String,
    reason: String,
    domain: String,
    metadata: HashMap<String, String>,
    request_id: String,
    serving_data: String,
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}] {}", self.error_code, self.message)
    }
}
impl std::error::Error for ErrorResponse {}

/// List catalogs response
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ListCatalogsResponse {
    /// Successful response
    Success {
        /// The schemas within the parent catalog
        catalogs: Vec<Catalog>,
        next_page_token: Option<String>,
    },
    /// Error response
    Error(ErrorResponse),
}

/// List schemas response
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ListSchemasResponse {
    /// Successful response
    Success {
        /// The schemas within the parent catalog
        schemas: Vec<Schema>,
    },
    /// Error response
    Error(ErrorResponse),
}

/// Get table response
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum GetTableResponse {
    /// Successful response
    Success(Table),
    /// Error response
    Error(ErrorResponse),
}

/// List schemas response
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum GetSchemaResponse {
    /// Successful response
    Success(Box<Schema>),
    /// Error response
    Error(ErrorResponse),
}

/// List table summaries response
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ListTableSummariesResponse {
    /// Successful response
    Success {
        /// Basic table infos
        #[serde(default)]
        tables: Vec<TableSummary>,
        /// Continuation token
        next_page_token: Option<String>,
    },
    /// Error response
    Error(ErrorResponse),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum TableTempCredentialsResponse {
    Success(TemporaryTableCredentials),
    Error(ErrorResponse),
}

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// Whether the current securable is accessible from all workspaces or a specific set of workspaces.
pub enum IsolationMode {
    #[default]
    Undefined,
    Open,
    Isolated,
}

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// The type of the catalog.
pub enum CatalogType {
    #[default]
    Undefined,
    ManagedCatalog,
    DeltasharingCatalog,
    SystemCatalog,
}

/// A catalog within a metastore
#[derive(Deserialize, Default, Debug)]
#[serde(default)]
pub struct Catalog {
    pub created_by: String,
    pub name: String,
    pub updated_by: String,
    pub isolation_mode: IsolationMode,
    pub catalog_type: CatalogType,
    pub storage_root: String,
    pub provider_name: String,
    pub storage_location: String,
    pub properties: HashMap<String, String>,
    pub share_name: String,
    pub comment: String,
    pub created_at: i64,
    pub owner: String,
    pub updated_at: i64,
    pub metastore_id: String,
    pub enabled_predictive_optimization: String,
    pub effective_predictive_optimization_flag: EffectivePredictiveOptimizationFlag,
    pub connection_name: String,
    pub full_name: String,
    pub options: HashMap<String, String>,
    pub securable_type: String,
    pub provisioning_info: ProvisioningInfo,
    pub browse_only: Option<bool>,
    pub accessible_in_current_workspace: bool,
    pub id: String,
    pub securable_kind: String,
    pub delta_sharing_valid_through_timestamp: u64,
}

#[allow(unused)]
#[derive(Deserialize, Default, Debug)]
pub struct ProvisioningInfo {
    state: ProvisioningState,
}

#[derive(Deserialize, Debug, Default)]
pub enum ProvisioningState {
    #[default]
    Provisioning,
    Active,
    Failed,
    Deleting,
    Updating,
}

#[derive(Deserialize, Default, Debug)]
pub struct EffectivePredictiveOptimizationFlag {
    pub value: String,
    pub inherited_from_type: String,
    pub inherited_from_name: String,
}

/// A schema within a catalog
#[derive(Deserialize, Default, Debug)]
pub struct Schema {
    /// Username of schema creator.
    #[serde(default)]
    pub created_by: String,

    /// Name of schema, relative to parent catalog.
    pub name: String,

    /// Username of user who last modified schema.
    #[serde(default)]
    pub updated_by: String,

    /// Full name of schema, in form of catalog_name.schema_name.
    pub full_name: String,

    /// The type of the parent catalog.
    pub catalog_type: String,

    /// Name of parent catalog.
    pub catalog_name: String,

    /// Storage root URL for managed tables within schema.
    #[serde(default)]
    pub storage_root: String,

    /// Storage location for managed tables within schema.
    #[serde(default)]
    pub storage_location: String,

    /// A map of key-value properties attached to the securable.
    #[serde(default)]
    pub properties: HashMap<String, String>,

    /// User-provided free-form text description.
    #[serde(default)]
    pub comment: String,

    /// Time at which this schema was created, in epoch milliseconds.
    #[serde(default)]
    pub created_at: i64,

    /// Username of current owner of schema.
    #[serde(default)]
    pub owner: String,

    /// Time at which this schema was created, in epoch milliseconds.
    #[serde(default)]
    pub updated_at: i64,

    /// Unique identifier of parent metastore.
    pub metastore_id: String,
}

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// Possible data source formats for unity tables
#[derive(Clone, PartialEq)]
pub enum DataSourceFormat {
    #[default]
    Undefined,
    Delta,
    Csv,
    Json,
    Avro,
    Parquet,
    Orc,
    Text,
    UnityCatalog,
    Deltasharing,
    DatabricksFormat,
    MySQLFormat,
    PostgreSQLFormat,
    RedshiftFormat,
    SnowflakeFormat,
    SQLDWFormat,
    SQLServerFormat,
    SalesForceFormat,
    BigQueryFormat,
    NetSuiteFormat,
    WorkdayRAASFormat,
    HiveSerde,
    HiveCustom,
    VectorIndexFormat,
}

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// Possible data source formats for unity tables
#[derive(PartialEq, Clone)]
pub enum TableType {
    #[default]
    Undefined,
    Managed,
    External,
    View,
    MaterializedView,
    StreamingTable,
}

#[derive(Deserialize, Debug)]
/// Summary of the table
pub struct TableSummary {
    /// The full name of the table.
    pub full_name: String,
    /// Type of table
    pub table_type: TableType,
}

/// A table within a schema
#[derive(Clone, Debug, PartialEq, Default, Deserialize)]
pub struct Table {
    pub name: String,
    /// Name of parent catalog.
    pub catalog_name: String,
    /// Name of parent schema relative to its parent catalog.
    pub schema_name: String,
    pub table_type: TableType,
    pub data_source_format: DataSourceFormat,
    /// The array of __ColumnInfo__ definitions of the table's columns.
    pub columns: Vec<ColumnInfo>,
    /// Storage root URL for table (for **MANAGED**, **EXTERNAL** tables)
    pub storage_location: String,
    /// User-provided free-form text description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// A map of key-value properties attached to the securable.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    /// Time at which this table was created, in epoch milliseconds.
    #[serde(with = "ts_milliseconds")]
    pub created_at: DateTime<Utc>,
    /// Time at which this table was last modified, in epoch milliseconds.
    #[serde(with = "ts_milliseconds")]
    pub updated_at: DateTime<Utc>,
    /// Unique identifier for the table.
    pub table_id: String,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Name of Column.
    pub name: String,
    /// Full data type specification as SQL/catalogString text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_text: Option<String>,
    /// Full data type specification, JSON-serialized.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_name: Option<ColumnTypeName>,
    /// Digits of precision; required for DecimalTypes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_precision: Option<i32>,
    /// Digits to right of decimal; Required for DecimalTypes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_scale: Option<i32>,
    /// Format of IntervalType.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_interval_type: Option<String>,
    /// Ordinal position of column (starting at position 0).
    pub position: u32,
    /// User-provided free-form text description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Whether field may be Null.
    pub nullable: bool,
    /// Partition index for column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_index: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ColumnTypeName {
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    Float,
    Double,
    Date,
    Timestamp,
    TimestampNtz,
    String,
    Binary,
    Decimal,
    Interval,
    Array,
    Struct,
    Map,
    Char,
    Null,
    UserDefinedType,
    TableType,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
pub struct DeltaRuntimeProperties {
    pub delta_runtime_properties: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TemporaryTableCredentials {
    pub aws_temp_credentials: Option<AwsTempCredentials>,
    pub azure_user_delegation_sas: Option<AzureUserDelegationSas>,
    pub gcp_oauth_token: Option<GcpOauthToken>,
    pub r2_temp_credentials: Option<R2TempCredentials>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiration_time: DateTime<Utc>,
    pub url: String,
}

#[cfg(any(feature = "aws", feature = "r2"))]
static INIT_AWS: Once = Once::new();
#[cfg(feature = "azure")]
static INIT_AZURE: Once = Once::new();
#[cfg(feature = "gcp")]
static INIT_GCP: Once = Once::new();

impl TemporaryTableCredentials {
    #[cfg(feature = "aws")]
    pub fn get_aws_credentials(&self) -> Option<HashMap<String, String>> {
        INIT_AWS.call_once(|| deltalake_aws::register_handlers(None));
        self.aws_temp_credentials.clone().map(Into::into)
    }

    #[cfg(not(feature = "aws"))]
    pub fn get_aws_credentials(&self) -> Option<HashMap<String, String>> {
        tracing::warn!("AWS Credentials found, but the feature is not enabled.");
        None
    }

    #[cfg(feature = "azure")]
    pub fn get_azure_credentials(&self) -> Option<HashMap<String, String>> {
        INIT_AZURE.call_once(|| deltalake_azure::register_handlers(None));
        self.azure_user_delegation_sas.clone().map(Into::into)
    }

    #[cfg(not(feature = "azure"))]
    pub fn get_azure_credentials(&self) -> Option<HashMap<String, String>> {
        tracing::warn!("Azure credentials found, but the feature is not enabled.");
        None
    }

    #[cfg(feature = "gcp")]
    pub fn get_gcp_credentials(&self) -> Option<HashMap<String, String>> {
        INIT_GCP.call_once(|| deltalake_gcp::register_handlers(None));
        self.gcp_oauth_token.clone().map(Into::into)
    }

    #[cfg(not(feature = "gcp"))]
    pub fn get_gcp_credentials(&self) -> Option<HashMap<String, String>> {
        tracing::warn!("GCP credentials found, but the feature is not enabled.");
        None
    }

    #[cfg(feature = "r2")]
    pub fn get_r2_credentials(&self) -> Option<HashMap<String, String>> {
        INIT_AWS.call_once(|| deltalake_aws::register_handlers(None));
        self.r2_temp_credentials.clone().map(Into::into)
    }

    #[cfg(not(feature = "r2"))]
    pub fn get_r2_credentials(&self) -> Option<HashMap<String, String>> {
        tracing::warn!("r2 credentials found, but feature is not enabled.");
        None
    }

    pub fn get_credentials(self) -> Option<HashMap<String, String>> {
        self.get_aws_credentials()
            .or(self.get_azure_credentials())
            .or(self.get_gcp_credentials())
            .or(self.get_r2_credentials())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct AwsTempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub access_point: Option<String>,
}

#[cfg(feature = "aws")]
impl From<AwsTempCredentials> for HashMap<String, String> {
    fn from(value: AwsTempCredentials) -> Self {
        let mut result = HashMap::from_iter([
            (
                deltalake_aws::constants::AWS_ACCESS_KEY_ID.to_string(),
                value.access_key_id,
            ),
            (
                deltalake_aws::constants::AWS_SECRET_ACCESS_KEY.to_string(),
                value.secret_access_key,
            ),
        ]);
        if let Some(st) = value.session_token {
            result.insert(deltalake_aws::constants::AWS_SESSION_TOKEN.to_string(), st);
        }
        if let Some(ap) = value.access_point {
            result.insert(deltalake_aws::constants::AWS_ENDPOINT_URL.to_string(), ap);
        }
        result
    }
}

#[cfg(feature = "azure")]
impl From<AzureUserDelegationSas> for HashMap<String, String> {
    fn from(value: AzureUserDelegationSas) -> Self {
        HashMap::from_iter([("azure_storage_sas_key".to_string(), value.sas_token)])
    }
}

#[cfg(feature = "gcp")]
impl From<GcpOauthToken> for HashMap<String, String> {
    fn from(value: GcpOauthToken) -> Self {
        HashMap::from_iter([(
            "google_application_credentials".to_string(),
            value.oauth_token,
        )])
    }
}

#[cfg(feature = "r2")]
impl From<R2TempCredentials> for HashMap<String, String> {
    fn from(value: R2TempCredentials) -> Self {
        HashMap::from_iter([
            (
                deltalake_aws::constants::AWS_ACCESS_KEY_ID.to_string(),
                value.access_key_id,
            ),
            (
                deltalake_aws::constants::AWS_SECRET_ACCESS_KEY.to_string(),
                value.secret_access_key,
            ),
            (
                deltalake_aws::constants::AWS_SESSION_TOKEN.to_string(),
                value.session_token,
            ),
        ])
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct AzureUserDelegationSas {
    pub sas_token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GcpOauthToken {
    pub oauth_token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct R2TempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct TemporaryTableCredentialsRequest {
    pub table_id: String,
    pub operation: String,
}

impl TemporaryTableCredentialsRequest {
    pub fn new(table_id: &str, operation: &str) -> Self {
        Self {
            table_id: table_id.to_string(),
            operation: operation.to_string(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const ERROR_RESPONSE: &str = r#"
        {
            "error_code": "404",
            "message": "error message",
            "details": []
        }
    "#;

    pub(crate) const LIST_SCHEMAS_RESPONSE: &str = r#"
    {
        "schemas": [
            {
            "created_by": "string",
            "name": "string",
            "updated_by": "string",
            "full_name": "string",
            "catalog_type": "string",
            "catalog_name": "string",
            "schema_name": "string",
            "storage_root": "string",
            "storage_location": "string",
            "properties": {
                "property1": "string",
                "property2": "string"
            },
            "comment": "string",
            "created_at": 0,
            "owner": "string",
            "updated_at": 0,
            "metastore_id": "string",
            "table_id": "string"
            }
        ]
    }"#;

    pub(crate) const GET_SCHEMA_RESPONSE: &str = r#"
        {
            "created_by": "string",
            "name": "schema_name",
            "updated_by": "string",
            "full_name": "catalog_name.schema_name",
            "catalog_type": "string",
            "catalog_name": "catalog_name",
            "storage_root": "string",
            "storage_location": "string",
            "properties": {
                "property1": "string",
                "property2": "string"
            },
            "comment": "string",
            "created_at": 0,
            "owner": "string",
            "updated_at": 0,
            "metastore_id": "string"
        }"#;

    pub(crate) const GET_TABLE_RESPONSE: &str = r#"
        {
          "name": "string",
          "catalog_name": "string",
          "schema_name": "string",
          "table_type": "MANAGED",
          "data_source_format": "DELTA",
          "columns": [
            {
              "name": "string",
              "type_text": "string",
              "type_name": "BOOLEAN",
              "position": 0,
              "type_precision": 0,
              "type_scale": 0,
              "type_interval_type": "string",
              "type_json": "string",
              "comment": "string",
              "nullable": true,
              "partition_index": 0,
              "mask": {
                "function_name": "string",
                "using_column_names": [
                  "string"
                ]
              }
            }
          ],
          "storage_location": "string",
          "view_definition": "string",
          "view_dependencies": {
            "dependencies": [
              {
                "table": {
                  "table_full_name": "string"
                },
                "function": {
                  "function_full_name": "string"
                }
              }
            ]
          },
          "sql_path": "string",
          "owner": "string",
          "comment": "string",
          "properties": {
            "property1": "string",
            "property2": "string"
          },
          "storage_credential_name": "string",
          "table_constraints": [
            {
              "primary_key_constraint": {
                "name": "string",
                "child_columns": [
                  "string"
                ]
              },
              "foreign_key_constraint": {
                "name": "string",
                "child_columns": [
                  "string"
                ],
                "parent_table": "string",
                "parent_columns": [
                  "string"
                ]
              },
              "named_table_constraint": {
                "name": "string"
              }
            }
          ],
          "row_filter": {
            "function_name": "string",
            "input_column_names": [
              "string"
            ]
          },
          "enable_predictive_optimization": "DISABLE",
          "metastore_id": "string",
          "full_name": "string",
          "data_access_configuration_id": "string",
          "created_at": 0,
          "created_by": "string",
          "updated_at": 0,
          "updated_by": "string",
          "deleted_at": 0,
          "table_id": "string",
          "delta_runtime_properties_kvpairs": {
            "delta_runtime_properties": {
              "property1": "string",
              "property2": "string"
            }
          },
          "effective_predictive_optimization_flag": {
            "value": "DISABLE",
            "inherited_from_type": "CATALOG",
            "inherited_from_name": "string"
          },
          "access_point": "string",
          "pipeline_id": "string",
          "browse_only": true
        }
    "#;

    pub(crate) const LIST_TABLES: &str = r#"
        {
	        "tables": [{
                "full_name": "catalog.schema.table_name",
                "table_type": "MANAGED"
            }]
		}
		"#;
    pub(crate) const LIST_TABLES_EMPTY: &str = "{}";

    #[test]
    fn test_responses() {
        let list_schemas: Result<ListSchemasResponse, _> =
            serde_json::from_str(LIST_SCHEMAS_RESPONSE);
        assert!(list_schemas.is_ok());
        assert!(matches!(
            list_schemas.unwrap(),
            ListSchemasResponse::Success { .. }
        ));

        let get_table: Result<GetTableResponse, _> = serde_json::from_str(GET_TABLE_RESPONSE);
        assert!(get_table.is_ok());
        assert!(matches!(
            get_table.unwrap(),
            GetTableResponse::Success { .. }
        ));

        let list_tables: Result<ListTableSummariesResponse, _> = serde_json::from_str(LIST_TABLES);
        assert!(list_tables.is_ok());
        assert!(matches!(
            list_tables.unwrap(),
            ListTableSummariesResponse::Success { .. }
        ));

        let list_tables: Result<ListTableSummariesResponse, _> =
            serde_json::from_str(LIST_TABLES_EMPTY);
        assert!(list_tables.is_ok());
        assert!(matches!(
            list_tables.unwrap(),
            ListTableSummariesResponse::Success { .. }
        ));

        let get_schema: Result<GetSchemaResponse, _> = serde_json::from_str(GET_SCHEMA_RESPONSE);
        assert!(get_schema.is_ok());
        assert!(matches!(get_schema.unwrap(), GetSchemaResponse::Success(_)))
    }

    #[test]
    fn test_response_errors() {
        let list_schemas: Result<ListSchemasResponse, _> = serde_json::from_str(ERROR_RESPONSE);
        assert!(list_schemas.is_ok());
        assert!(matches!(
            list_schemas.unwrap(),
            ListSchemasResponse::Error(_)
        ));

        let get_table: Result<GetTableResponse, _> = serde_json::from_str(ERROR_RESPONSE);
        assert!(get_table.is_ok());
        dbg!(&get_table);
        assert!(matches!(get_table.unwrap(), GetTableResponse::Error(_)))
    }
}
