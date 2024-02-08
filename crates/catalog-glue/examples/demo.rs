use deltalake_catalog_glue::*;
use deltalake_core::*;

#[tokio::main]
async fn main() {
    println!("Reading a table");

    let catalog = GlueDataCatalog::from_env()
        .await
        .expect("Failed to load catalog from the environment");
    println!("catalog: {catalog:?}");

    println!(
        "read: {:?}",
        catalog
            .get_table_storage_location(None, "database", "table")
            .await
            .expect("Failed")
    );
}
