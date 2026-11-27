#![cfg(feature = "integration_test_unity")]
//! Tests with a Unity Catalog OSS server.
//!
//! Each test starts `uc-server`, which the Python package `unitycatalog-rs-server` installs.

use std::collections::HashMap;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use deltalake_catalog_unity::models::*;
use deltalake_catalog_unity::{UnityCatalog, UnityCatalogBuilder};
use deltalake_core::{DataCatalog, DeltaTable, DeltaTableBuilder};
use deltalake_test::utils::TestTables;
use reqwest::Url;
use serde_json::{Value, json};
use tempfile::TempDir;

const CATALOG: &str = "unity";
const SCHEMA: &str = "default";

/// A `uc-server` process with one catalog and one schema. The process stops on drop.
struct UnityCatalogServer {
    process: Child,
    url: String,
    _data_dir: TempDir,
}

impl UnityCatalogServer {
    async fn start() -> Self {
        deltalake_catalog_unity::register_handlers(None);

        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .unwrap()
            .port();
        let data_dir = tempfile::tempdir().unwrap();
        let process = Command::new("uc-server")
            .env("UC_SERVER_BIND", format!("127.0.0.1:{port}"))
            .env("UC_SERVER_DATA_DIR", data_dir.path())
            .env("UC_DB_URL", "memory")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("uc-server must be installed: pip install unitycatalog-rs-server");
        let mut server = Self {
            process,
            url: format!("http://127.0.0.1:{port}"),
            _data_dir: data_dir,
        };

        server.wait_until_ready().await;
        server.post("catalogs", json!({"name": CATALOG})).await;
        server
            .post("schemas", json!({"name": SCHEMA, "catalog_name": CATALOG}))
            .await;
        server
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api/2.1/unity-catalog/{path}", self.url)
    }

    async fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while reqwest::get(self.api_url("catalogs")).await.is_err() {
            if let Some(status) = self.process.try_wait().unwrap() {
                panic!("uc-server stopped: {status}");
            }
            assert!(Instant::now() < deadline, "uc-server did not start");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn post(&self, path: &str, body: Value) {
        let response = reqwest::Client::new()
            .post(self.api_url(path))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        assert!(status.is_success(), "POST {path} failed: {status} {text}");
    }

    /// Registers a test table as an external table. Returns its storage location.
    async fn register_table(&self, name: &str, table: TestTables) -> String {
        let location = Url::from_file_path(table.as_path()).unwrap().to_string();
        self.post(
            "tables",
            json!({
                "name": name,
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "columns": [],
                "storage_location": location,
            }),
        )
        .await;
        location
    }

    fn options(&self) -> HashMap<String, String> {
        HashMap::from([
            ("unity_workspace_url".to_string(), self.url.clone()),
            // The server does not check the token, but the client needs one.
            ("unity_access_token".to_string(), "token".to_string()),
            ("unity_allow_http_url".to_string(), "true".to_string()),
        ])
    }

    fn client(&self) -> UnityCatalog {
        UnityCatalogBuilder::from_env()
            .try_with_options(&self.options())
            .unwrap()
            .build()
            .unwrap()
    }

    async fn load_table(&self, name: &str) -> DeltaTable {
        let url = Url::parse(&format!("uc://{CATALOG}.{SCHEMA}.{name}")).unwrap();
        DeltaTableBuilder::from_url(url)
            .unwrap()
            .with_storage_options(self.options())
            .load()
            .await
            .unwrap()
    }
}

impl Drop for UnityCatalogServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unity_client() {
    let server = UnityCatalogServer::start().await;
    let location = server
        .register_table("partitioned", TestTables::Delta0_8_0Partitioned)
        .await;
    server.register_table("simple", TestTables::Simple).await;
    let client = server.client();

    let ListCatalogsResponse::Success { catalogs, .. } = client.list_catalogs().await.unwrap()
    else {
        panic!("expected the catalogs");
    };
    let catalog_names: Vec<_> = catalogs.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(catalog_names, [CATALOG]);

    let ListSchemasResponse::Success { schemas } = client.list_schemas(CATALOG).await.unwrap()
    else {
        panic!("expected the schemas");
    };
    let schema_names: Vec<_> = schemas.iter().map(|s| s.full_name.as_str()).collect();
    assert_eq!(schema_names, ["unity.default"]);

    let GetSchemaResponse::Success(schema) = client.get_schema(CATALOG, SCHEMA).await.unwrap()
    else {
        panic!("expected the schema");
    };
    assert_eq!(schema.name, SCHEMA);
    assert_eq!(schema.catalog_name, CATALOG);

    let GetTableResponse::Success(table) = client
        .get_table(CATALOG, SCHEMA, "partitioned")
        .await
        .unwrap()
    else {
        panic!("expected the table");
    };
    assert_eq!(table.name, "partitioned");
    assert_eq!(table.table_type, TableType::External);
    assert_eq!(table.data_source_format, Some(DataSourceFormat::Delta));

    let storage_location = client
        .get_table_storage_location(Some(CATALOG.to_string()), SCHEMA, "partitioned")
        .await
        .unwrap();
    assert_eq!(storage_location, location);

    let GetTableResponse::Error(error) =
        client.get_table(CATALOG, SCHEMA, "missing").await.unwrap()
    else {
        panic!("expected an error");
    };
    assert_eq!(error.error_code, "TABLE_NOT_FOUND");

    // The server has no table summaries: they come from the list of tables.
    let ListTableSummariesResponse::Success { tables, .. } =
        client.list_table_summaries(CATALOG, SCHEMA).await.unwrap()
    else {
        panic!("expected the table summaries");
    };
    let mut names: Vec<_> = tables.iter().map(|t| t.full_name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["unity.default.partitioned", "unity.default.simple"]);
    assert!(tables.iter().all(|t| t.table_type == TableType::External));

    let ListTableSummariesResponse::Error(error) = client
        .list_table_summaries("missing", SCHEMA)
        .await
        .unwrap()
    else {
        panic!("expected an error");
    };
    assert_eq!(error.error_code, "CATALOG_NOT_FOUND");
}

#[tokio::test(flavor = "multi_thread")]
async fn read_table_on_local_file_system() {
    let server = UnityCatalogServer::start().await;
    server
        .register_table("partitioned", TestTables::Delta0_8_0Partitioned)
        .await;
    server.register_table("simple", TestTables::Simple).await;

    // The server gives no credentials for a local table.
    let table = server.load_table("partitioned").await;
    assert_eq!(table.version(), Some(0));
    assert_eq!(table.get_file_uris().unwrap().count(), 6);

    // This table has five commits, which the local file system lists in no fixed order.
    let table = server.load_table("simple").await;
    assert_eq!(table.version(), Some(4));
    assert_eq!(table.get_file_uris().unwrap().count(), 5);
}

#[cfg(feature = "datafusion")]
#[tokio::test(flavor = "multi_thread")]
async fn datafusion_catalog() {
    use std::sync::Arc;

    use datafusion::catalog::CatalogProvider;
    use datafusion::common::TableReference;
    use datafusion::prelude::SessionContext;
    use deltalake_catalog_unity::datafusion::UnityCatalogProvider;

    let server = UnityCatalogServer::start().await;
    server
        .register_table("partitioned", TestTables::Delta0_8_0Partitioned)
        .await;

    let catalog = UnityCatalogProvider::try_new(Arc::new(server.client()), CATALOG)
        .await
        .unwrap();
    assert_eq!(catalog.schema_names(), [SCHEMA]);
    assert_eq!(
        catalog.schema(SCHEMA).unwrap().table_names(),
        ["partitioned"]
    );

    let ctx = SessionContext::new();
    ctx.register_catalog(CATALOG, Arc::new(catalog));
    let rows = ctx
        .table(TableReference::full(CATALOG, SCHEMA, "partitioned"))
        .await
        .unwrap()
        .count()
        .await
        .unwrap();
    assert_eq!(rows, 7);
}
