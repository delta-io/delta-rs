use std::path::Path;

use datafusion::logical_expr::{cast, lit};
use datafusion::prelude::DataFrame;
use datafusion::prelude::ParquetReadOptions;
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::{StructField, StructType};
use deltalake_core::operations::merge::MergeBuilder;
use deltalake_core::DeltaResult;
use deltalake_core::{datafusion::prelude::SessionContext, DeltaOps, DeltaTable, DeltaTableError};
use tempfile::TempDir;
use url::Url;

pub type MergeOp = fn(DataFrame, DeltaTable) -> Result<MergeBuilder, DeltaTableError>;

#[derive(Debug, Clone)]
pub struct MergePerfParams {
    pub sample_matched_rows: f32,
    pub sample_not_matched_rows: f32,
}

pub fn merge_upsert(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    deltalake_core::DeltaOps(table)
        .merge(source, "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number")
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update
                .update("wr_returned_date_sk", "source.wr_returned_date_sk")
                .update("wr_returned_time_sk", "source.wr_returned_time_sk")
                .update("wr_item_sk", "source.wr_item_sk")
                .update("wr_refunded_customer_sk", "source.wr_refunded_customer_sk")
                .update("wr_refunded_cdemo_sk", "source.wr_refunded_cdemo_sk")
                .update("wr_refunded_hdemo_sk", "source.wr_refunded_hdemo_sk")
                .update("wr_refunded_addr_sk", "source.wr_refunded_addr_sk")
                .update("wr_returning_customer_sk", "source.wr_returning_customer_sk")
                .update("wr_returning_cdemo_sk", "source.wr_returning_cdemo_sk")
                .update("wr_returning_hdemo_sk", "source.wr_returning_hdemo_sk")
                .update("wr_returning_addr_sk", "source.wr_returning_addr_sk")
                .update("wr_web_page_sk", "source.wr_web_page_sk")
                .update("wr_reason_sk", "source.wr_reason_sk")
                .update("wr_order_number", "source.wr_order_number")
                .update("wr_return_quantity", "source.wr_return_quantity")
                .update("wr_return_amt", "source.wr_return_amt")
                .update("wr_return_tax", "source.wr_return_tax")
                .update("wr_return_amt_inc_tax", "source.wr_return_amt_inc_tax")
                .update("wr_fee", "source.wr_fee")
                .update("wr_return_ship_cost", "source.wr_return_ship_cost")
                .update("wr_refunded_cash", "source.wr_refunded_cash")
                .update("wr_reversed_charge", "source.wr_reversed_charge")
                .update("wr_account_credit", "source.wr_account_credit")
                .update("wr_net_loss", "source.wr_net_loss")
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("wr_returned_date_sk", "source.wr_returned_date_sk")
                .set("wr_returned_time_sk", "source.wr_returned_time_sk")
                .set("wr_item_sk", "source.wr_item_sk")
                .set("wr_refunded_customer_sk", "source.wr_refunded_customer_sk")
                .set("wr_refunded_cdemo_sk", "source.wr_refunded_cdemo_sk")
                .set("wr_refunded_hdemo_sk", "source.wr_refunded_hdemo_sk")
                .set("wr_refunded_addr_sk", "source.wr_refunded_addr_sk")
                .set("wr_returning_customer_sk", "source.wr_returning_customer_sk")
                .set("wr_returning_cdemo_sk", "source.wr_returning_cdemo_sk")
                .set("wr_returning_hdemo_sk", "source.wr_returning_hdemo_sk")
                .set("wr_returning_addr_sk", "source.wr_returning_addr_sk")
                .set("wr_web_page_sk", "source.wr_web_page_sk")
                .set("wr_reason_sk", "source.wr_reason_sk")
                .set("wr_order_number", "source.wr_order_number")
                .set("wr_return_quantity", "source.wr_return_quantity")
                .set("wr_return_amt", "source.wr_return_amt")
                .set("wr_return_tax", "source.wr_return_tax")
                .set("wr_return_amt_inc_tax", "source.wr_return_amt_inc_tax")
                .set("wr_fee", "source.wr_fee")
                .set("wr_return_ship_cost", "source.wr_return_ship_cost")
                .set("wr_refunded_cash", "source.wr_refunded_cash")
                .set("wr_reversed_charge", "source.wr_reversed_charge")
                .set("wr_account_credit", "source.wr_account_credit")
                .set("wr_net_loss", "source.wr_net_loss")
        })
}

pub fn merge_insert(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    deltalake_core::DeltaOps(table)
        .merge(source, "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number")
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| {
            insert
                .set("wr_returned_date_sk", "source.wr_returned_date_sk")
                .set("wr_returned_time_sk", "source.wr_returned_time_sk")
                .set("wr_item_sk", "source.wr_item_sk")
                .set("wr_refunded_customer_sk", "source.wr_refunded_customer_sk")
                .set("wr_refunded_cdemo_sk", "source.wr_refunded_cdemo_sk")
                .set("wr_refunded_hdemo_sk", "source.wr_refunded_hdemo_sk")
                .set("wr_refunded_addr_sk", "source.wr_refunded_addr_sk")
                .set("wr_returning_customer_sk", "source.wr_returning_customer_sk")
                .set("wr_returning_cdemo_sk", "source.wr_returning_cdemo_sk")
                .set("wr_returning_hdemo_sk", "source.wr_returning_hdemo_sk")
                .set("wr_returning_addr_sk", "source.wr_returning_addr_sk")
                .set("wr_web_page_sk", "source.wr_web_page_sk")
                .set("wr_reason_sk", "source.wr_reason_sk")
                .set("wr_order_number", "source.wr_order_number")
                .set("wr_return_quantity", "source.wr_return_quantity")
                .set("wr_return_amt", "source.wr_return_amt")
                .set("wr_return_tax", "source.wr_return_tax")
                .set("wr_return_amt_inc_tax", "source.wr_return_amt_inc_tax")
                .set("wr_fee", "source.wr_fee")
                .set("wr_return_ship_cost", "source.wr_return_ship_cost")
                .set("wr_refunded_cash", "source.wr_refunded_cash")
                .set("wr_reversed_charge", "source.wr_reversed_charge")
                .set("wr_account_credit", "source.wr_account_credit")
                .set("wr_net_loss", "source.wr_net_loss")
        })
}

pub fn merge_delete(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    deltalake_core::DeltaOps(table)
        .merge(source, "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number")
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_delete(|delete| delete)
}

/// Prepare source DataFrame and target Delta table from DuckDB-generated TPC-DS parquet.
/// Creates a temporary Delta table from web_returns.parquet as the target.
/// Returns (source_df, target_table) for benchmarking.
pub async fn prepare_source_and_table(
    params: &MergePerfParams,
    tmp_dir: &TempDir,
    parquet_dir: &Path,
) -> DeltaResult<(DataFrame, DeltaTable)> {
    let ctx = SessionContext::new();

    let parquet_path = parquet_dir
        .join("web_returns.parquet")
        .to_str()
        .unwrap()
        .to_owned();

    let parquet_df = ctx
        .read_parquet(&parquet_path, ParquetReadOptions::default())
        .await?;
    let temp_table_url = Url::from_directory_path(tmp_dir).unwrap();

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

    // Now prepare source DataFrame with sampling
    let source = ctx
        .read_parquet(&parquet_path, ParquetReadOptions::default())
        .await?;

    // Split matched and not-matched portions
    let matched = source
        .clone()
        .filter(datafusion::functions::expr_fn::random().lt_eq(lit(params.sample_matched_rows)))?;

    let rand = cast(
        datafusion::functions::expr_fn::random() * lit(u32::MAX),
        datafusion::arrow::datatypes::DataType::Int64,
    );
    let not_matched = source
        .filter(
            datafusion::functions::expr_fn::random().lt_eq(lit(params.sample_not_matched_rows)),
        )?
        .with_column("wr_item_sk", rand.clone())?
        .with_column("wr_order_number", rand)?;

    let source = matched.union(not_matched)?;
    Ok((source, table))
}
