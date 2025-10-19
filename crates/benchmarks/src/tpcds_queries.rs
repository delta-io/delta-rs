use std::collections::HashMap;
use std::sync::LazyLock;

use std::path::Path;
use std::sync::Arc;

use deltalake_core::datafusion::prelude::{ParquetReadOptions, SessionContext};
use deltalake_core::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::{StructField, StructType};
use deltalake_core::{DeltaOps, DeltaResult};
use tempfile::TempDir;
use tokio::fs::create_dir;
use url::Url;

macro_rules! include_query {
    ($path:literal) => {
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $path))
    };
}

pub const TPCDS_TABLE_NAMES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

pub async fn register_tpcds_tables(
    tmp_dir: &TempDir,
    parquet_dir: &Path,
) -> DeltaResult<SessionContext> {
    let ctx = SessionContext::new();
    for table_name in TPCDS_TABLE_NAMES {
        let parquet_path = parquet_dir
            .join(format!("{}.parquet", table_name))
            .to_str()
            .unwrap()
            .to_string();

        let parquet_df = ctx
            .read_parquet(parquet_path, ParquetReadOptions::default())
            .await?;

        let temp_table_dir = tmp_dir.path().join(table_name);
        create_dir(&temp_table_dir).await?;
        let temp_table_url = Url::from_directory_path(&temp_table_dir).unwrap();

        let schema = parquet_df.schema();
        let delta_schema: StructType = schema.as_arrow().try_into_kernel().unwrap();

        let batches = parquet_df.collect().await?;
        let fields: Vec<StructField> = delta_schema.fields().cloned().collect();
        let table = DeltaOps::try_from_uri(temp_table_url)
            .await?
            .create()
            .with_columns(fields)
            .await?;

        let table = DeltaOps(table).write(batches).await?;

        let snapshot = table.snapshot()?.snapshot().clone();
        let config = DeltaScanConfigBuilder::new().build(&snapshot)?;
        let provider = DeltaTableProvider::try_new(snapshot, table.log_store(), config)?;

        ctx.register_table(*table_name, Arc::new(provider))?;
    }

    Ok(ctx)
}

type QueryMap = HashMap<&'static str, &'static str>;

pub fn tpcds_queries() -> &'static QueryMap {
    static HASH_MAP: LazyLock<QueryMap> = LazyLock::new(|| {
        let mut map = HashMap::new();
        for (k, v) in TPCDS_QUERIES_ENTRIES {
            map.insert(*k, *v);
        }
        map
    });
    &HASH_MAP
}

pub fn tpcds_query(name: &str) -> Option<&'static str> {
    tpcds_queries().get(name).copied()
}

pub fn tpcds_query_names() -> Vec<&'static str> {
    TPCDS_QUERIES_ENTRIES.iter().map(|(k, _)| *k).collect()
}

static TPCDS_QUERIES_ENTRIES: &[(&str, &str)] = &[
    ("q1", include_query!("queries/tpcds/q1.sql")),
    ("q2", include_query!("queries/tpcds/q2.sql")),
    ("q3", include_query!("queries/tpcds/q3.sql")),
    ("q4", include_query!("queries/tpcds/q4.sql")),
    ("q5", include_query!("queries/tpcds/q5.sql")),
    ("q6", include_query!("queries/tpcds/q6.sql")),
    ("q7", include_query!("queries/tpcds/q7.sql")),
    ("q8", include_query!("queries/tpcds/q8.sql")),
    ("q9", include_query!("queries/tpcds/q9.sql")),
    ("q10", include_query!("queries/tpcds/q10.sql")),
    ("q11", include_query!("queries/tpcds/q11.sql")),
    ("q12", include_query!("queries/tpcds/q12.sql")),
    ("q13", include_query!("queries/tpcds/q13.sql")),
    ("q14", include_query!("queries/tpcds/q14.sql")),
    ("q15", include_query!("queries/tpcds/q15.sql")),
    ("q16", include_query!("queries/tpcds/q16.sql")),
    ("q17", include_query!("queries/tpcds/q17.sql")),
    ("q18", include_query!("queries/tpcds/q18.sql")),
    ("q19", include_query!("queries/tpcds/q19.sql")),
    ("q20", include_query!("queries/tpcds/q20.sql")),
    ("q21", include_query!("queries/tpcds/q21.sql")),
    ("q22", include_query!("queries/tpcds/q22.sql")),
    ("q23", include_query!("queries/tpcds/q23.sql")),
    ("q24", include_query!("queries/tpcds/q24.sql")),
    ("q25", include_query!("queries/tpcds/q25.sql")),
    ("q26", include_query!("queries/tpcds/q26.sql")),
    ("q27", include_query!("queries/tpcds/q27.sql")),
    ("q28", include_query!("queries/tpcds/q28.sql")),
    ("q29", include_query!("queries/tpcds/q29.sql")),
    ("q30", include_query!("queries/tpcds/q30.sql")),
    ("q31", include_query!("queries/tpcds/q31.sql")),
    ("q32", include_query!("queries/tpcds/q32.sql")),
    ("q33", include_query!("queries/tpcds/q33.sql")),
    ("q34", include_query!("queries/tpcds/q34.sql")),
    ("q35", include_query!("queries/tpcds/q35.sql")),
    ("q36", include_query!("queries/tpcds/q36.sql")),
    ("q37", include_query!("queries/tpcds/q37.sql")),
    ("q38", include_query!("queries/tpcds/q38.sql")),
    ("q39", include_query!("queries/tpcds/q39.sql")),
    ("q40", include_query!("queries/tpcds/q40.sql")),
    ("q41", include_query!("queries/tpcds/q41.sql")),
    ("q42", include_query!("queries/tpcds/q42.sql")),
    ("q43", include_query!("queries/tpcds/q43.sql")),
    ("q44", include_query!("queries/tpcds/q44.sql")),
    ("q45", include_query!("queries/tpcds/q45.sql")),
    ("q46", include_query!("queries/tpcds/q46.sql")),
    ("q47", include_query!("queries/tpcds/q47.sql")),
    ("q48", include_query!("queries/tpcds/q48.sql")),
    ("q49", include_query!("queries/tpcds/q49.sql")),
    ("q50", include_query!("queries/tpcds/q50.sql")),
    ("q51", include_query!("queries/tpcds/q51.sql")),
    ("q52", include_query!("queries/tpcds/q52.sql")),
    ("q53", include_query!("queries/tpcds/q53.sql")),
    ("q54", include_query!("queries/tpcds/q54.sql")),
    ("q55", include_query!("queries/tpcds/q55.sql")),
    ("q56", include_query!("queries/tpcds/q56.sql")),
    ("q57", include_query!("queries/tpcds/q57.sql")),
    ("q58", include_query!("queries/tpcds/q58.sql")),
    ("q59", include_query!("queries/tpcds/q59.sql")),
    ("q60", include_query!("queries/tpcds/q60.sql")),
    ("q61", include_query!("queries/tpcds/q61.sql")),
    ("q62", include_query!("queries/tpcds/q62.sql")),
    ("q63", include_query!("queries/tpcds/q63.sql")),
    ("q64", include_query!("queries/tpcds/q64.sql")),
    ("q65", include_query!("queries/tpcds/q65.sql")),
    ("q66", include_query!("queries/tpcds/q66.sql")),
    ("q67", include_query!("queries/tpcds/q67.sql")),
    ("q68", include_query!("queries/tpcds/q68.sql")),
    ("q69", include_query!("queries/tpcds/q69.sql")),
    ("q70", include_query!("queries/tpcds/q70.sql")),
    ("q71", include_query!("queries/tpcds/q71.sql")),
    // disabled due to upstream datafusion: https://github.com/apache/datafusion/issues/4763
    // ("q72", include_query!("queries/tpcds/q72.sql")),
    ("q73", include_query!("queries/tpcds/q73.sql")),
    ("q74", include_query!("queries/tpcds/q74.sql")),
    ("q75", include_query!("queries/tpcds/q75.sql")),
    ("q76", include_query!("queries/tpcds/q76.sql")),
    ("q77", include_query!("queries/tpcds/q77.sql")),
    ("q78", include_query!("queries/tpcds/q78.sql")),
    ("q79", include_query!("queries/tpcds/q79.sql")),
    ("q80", include_query!("queries/tpcds/q80.sql")),
    ("q81", include_query!("queries/tpcds/q81.sql")),
    ("q82", include_query!("queries/tpcds/q82.sql")),
    ("q83", include_query!("queries/tpcds/q83.sql")),
    ("q84", include_query!("queries/tpcds/q84.sql")),
    ("q85", include_query!("queries/tpcds/q85.sql")),
    ("q86", include_query!("queries/tpcds/q86.sql")),
    ("q87", include_query!("queries/tpcds/q87.sql")),
    ("q88", include_query!("queries/tpcds/q88.sql")),
    ("q89", include_query!("queries/tpcds/q89.sql")),
    ("q90", include_query!("queries/tpcds/q90.sql")),
    ("q91", include_query!("queries/tpcds/q91.sql")),
    ("q92", include_query!("queries/tpcds/q92.sql")),
    ("q93", include_query!("queries/tpcds/q93.sql")),
    ("q94", include_query!("queries/tpcds/q94.sql")),
    ("q95", include_query!("queries/tpcds/q95.sql")),
    ("q96", include_query!("queries/tpcds/q96.sql")),
    ("q97", include_query!("queries/tpcds/q97.sql")),
    ("q98", include_query!("queries/tpcds/q98.sql")),
    ("q99", include_query!("queries/tpcds/q99.sql")),
];
