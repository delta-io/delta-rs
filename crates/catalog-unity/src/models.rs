//! Api models for databricks unity catalog APIs

use core::fmt;
use std::collections::HashMap;

use serde::Deserialize;

/// Error response from unity API
#[derive(Debug, Deserialize)]
pub struct ErrorResponse {
    /// The error code
    pub error_code: String,
    /// The error message
    pub message: String,
}
impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}] {}", self.error_code, self.message)
    }
}
impl std::error::Error for ErrorResponse {}

/// List catalogs response
#[derive(Deserialize)]
#[serde(untagged)]
pub enum ListCatalogsResponse {
    /// Successful response
    Success {
        /// The schemas within the parent catalog
        catalogs: Vec<Catalog>,
    },
    /// Error response
    Error(ErrorResponse),
}

/// List schemas response
#[derive(Deserialize)]
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
#[derive(Deserialize)]
#[serde(untagged)]
pub enum GetTableResponse {
    /// Successful response
    Success(Table),
    /// Error response
    Error(ErrorResponse),
}

/// List schemas response
#[derive(Deserialize)]
#[serde(untagged)]
pub enum GetSchemaResponse {
    /// Successful response
    Success(Box<Schema>),
    /// Error response
    Error(ErrorResponse),
}

/// List table summaries response
#[derive(Deserialize)]
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// Whether the current securable is accessible from all workspaces or a specific set of workspaces.
pub enum IsomationMode {
    #[default]
    Undefined,
    Open,
    Isolated,
}

#[derive(Deserialize, Default)]
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
#[derive(Deserialize, Default)]
pub struct Catalog {
    /// Username of schema creator.
    #[serde(default)]
    pub created_by: String,

    /// Name of schema, relative to parent catalog.
    pub name: String,

    /// Username of user who last modified schema.
    #[serde(default)]
    pub updated_by: String,

    #[serde(default)]
    /// Whether the current securable is accessible from all workspaces or a specific set of workspaces.
    pub isolation_mode: IsomationMode,

    #[serde(default)]
    /// The type of the catalog.
    pub catalog_type: CatalogType,

    /// Storage root URL for managed tables within catalog.
    pub storage_root: String,

    /// The name of delta sharing provider.
    ///
    /// A Delta Sharing catalog is a catalog that is based on a Delta share on a remote sharing server.
    pub provider_name: Option<String>,

    /// Storage Location URL (full path) for managed tables within catalog.
    pub storage_location: String,

    /// A map of key-value properties attached to the securable.
    #[serde(default)]
    pub properties: HashMap<String, String>,

    /// The name of the share under the share provider.
    pub share_name: Option<String>,

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

/// A schema within a catalog
#[derive(Deserialize, Default)]
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// Possible data source formats for unity tables
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
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(missing_docs)]
/// Possible data source formats for unity tables
pub enum TableType {
    #[default]
    Undefined,
    Managed,
    External,
    View,
    MaterializedView,
    StreamingTable,
}

#[derive(Deserialize)]
/// Summary of the table
pub struct TableSummary {
    /// The full name of the table.
    pub full_name: String,
    /// Type of table
    pub table_type: TableType,
}

/// A table within a schema
#[derive(Deserialize, Default)]
pub struct Table {
    /// Username of table creator.
    #[serde(default)]
    pub created_by: String,

    /// Name of table, relative to parent schema.
    pub name: String,

    /// Username of user who last modified the table.
    #[serde(default)]
    pub updated_by: String,

    /// List of schemes whose objects can be referenced without qualification.
    #[serde(default)]
    pub sql_path: String,

    /// Data source format
    pub data_source_format: DataSourceFormat,

    /// Full name of table, in form of catalog_name.schema_name.table_name
    pub full_name: String,

    /// Name of parent schema relative to its parent catalog.
    pub schema_name: String,

    /// Storage root URL for table (for MANAGED, EXTERNAL tables)
    pub storage_location: String,

    /// Unique identifier of parent metastore.
    pub metastore_id: String,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const ERROR_RESPONSE: &str = r#"
        {
            "error_code": "404",
            "message": "error message"
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
        "created_by": "string",
        "name": "table_name",
        "updated_by": "string",
        "sql_path": "string",
        "data_source_format": "DELTA",
        "full_name": "string",
        "delta_runtime_properties_kvpairs": {
          "delta_runtime_properties": {
            "property1": "string",
            "property2": "string"
          }
        },
        "catalog_name": "string",
        "table_constraints": {
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
          ]
        },
        "schema_name": "string",
        "storage_location": "string",
        "properties": {
          "property1": "string",
          "property2": "string"
        },
        "columns": [
          {
            "nullable": "true",
            "name": "string",
            "type_interval_type": "string",
            "mask": {
              "function_name": "string",
              "using_column_names": [
                "string"
              ]
            },
            "type_scale": 0,
            "type_text": "string",
            "comment": "string",
            "partition_index": 0,
            "type_json": "string",
            "position": 0,
            "type_name": "BOOLEAN",
            "type_precision": 0
          }
        ],
        "comment": "string",
        "table_id": "string",
        "table_type": "MANAGED",
        "created_at": 0,
        "row_filter": {
          "name": "string",
          "input_column_names": [
            "string"
          ]
        },
        "owner": "string",
        "storage_credential_name": "string",
        "updated_at": 0,
        "view_definition": "string",
        "view_dependencies": [
          {
            "table": {
              "table_full_name": "string"
            },
            "function": {
              "function_full_name": "string"
            }
          }
        ],
        "data_access_configuration_id": "string",
        "deleted_at": 0,
        "metastore_id": "string"
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
        assert!(matches!(get_table.unwrap(), GetTableResponse::Error(_)))
    }
}
