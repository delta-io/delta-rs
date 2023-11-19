use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::{RecordBatch, StringArray, UInt32Array};
use chrono::Duration;
use clap::{command, Args, Parser, Subcommand};
use datafusion::{datasource::MemTable, prelude::DataFrame};
use datafusion_common::DataFusionError;
use datafusion_expr::{cast, col, lit, random};
use deltalake_core::protocol::SaveMode;
use deltalake_core::{
    arrow::{
        self,
        datatypes::{DataType, Field},
    },
    datafusion::prelude::{CsvReadOptions, SessionContext},
    delta_datafusion::{DeltaScanConfig, DeltaTableProvider},
    operations::merge::{MergeBuilder, MergeMetrics},
    DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError, ObjectStore, Path,
};
use serde_json::json;
use tokio::time::Instant;

/* Convert web_returns dataset from TPC DS's datagen utility into a Delta table
   This table will be partitioned on `wr_returned_date_sk`
*/
pub async fn convert_tpcds_web_returns(input_path: String, table_path: String) -> Result<(), ()> {
    let ctx = SessionContext::new();

    let schema = ArrowSchema::new(vec![
        Field::new("wr_returned_date_sk", DataType::Int64, true),
        Field::new("wr_returned_time_sk", DataType::Int64, true),
        Field::new("wr_item_sk", DataType::Int64, false),
        Field::new("wr_refunded_customer_sk", DataType::Int64, true),
        Field::new("wr_refunded_cdemo_sk", DataType::Int64, true),
        Field::new("wr_refunded_hdemo_sk", DataType::Int64, true),
        Field::new("wr_refunded_addr_sk", DataType::Int64, true),
        Field::new("wr_returning_customer_sk", DataType::Int64, true),
        Field::new("wr_returning_cdemo_sk", DataType::Int64, true),
        Field::new("wr_returning_hdemo_sk", DataType::Int64, true),
        Field::new("wr_returning_addr_sk", DataType::Int64, true),
        Field::new("wr_web_page_sk", DataType::Int64, true),
        Field::new("wr_reason_sk", DataType::Int64, true),
        Field::new("wr_order_number", DataType::Int64, false),
        Field::new("wr_return_quantity", DataType::Int32, true),
        Field::new("wr_return_amt", DataType::Decimal128(7, 2), true),
        Field::new("wr_return_tax", DataType::Decimal128(7, 2), true),
        Field::new("wr_return_amt_inc_tax", DataType::Decimal128(7, 2), true),
        Field::new("wr_fee", DataType::Decimal128(7, 2), true),
        Field::new("wr_return_ship_cost", DataType::Decimal128(7, 2), true),
        Field::new("wr_refunded_cash", DataType::Decimal128(7, 2), true),
        Field::new("wr_reversed_charge", DataType::Decimal128(7, 2), true),
        Field::new("wr_account_credit", DataType::Decimal128(7, 2), true),
        Field::new("wr_net_loss", DataType::Decimal128(7, 2), true),
    ]);

    let table = ctx
        .read_csv(
            input_path,
            CsvReadOptions {
                has_header: false,
                delimiter: b'|',
                file_extension: ".dat",
                schema: Some(&schema),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .write(table.collect().await.unwrap())
        .with_partition_columns(vec!["wr_returned_date_sk"])
        .await
        .unwrap();

    Ok(())
}

fn merge_upsert(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    DeltaOps(table)
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

fn merge_insert(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    DeltaOps(table)
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

fn merge_delete(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    DeltaOps(table)
        .merge(source, "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number")
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_delete(|delete| {
            delete
        })
}

async fn benchmark_merge_tpcds(
    path: String,
    parameters: MergePerfParams,
    merge: fn(DataFrame, DeltaTable) -> Result<MergeBuilder, DeltaTableError>,
) -> Result<(core::time::Duration, MergeMetrics), DataFusionError> {
    let table = DeltaTableBuilder::from_uri(path).load().await?;
    let file_count = table.state.files().len();

    let provider = DeltaTableProvider::try_new(
        table.state.clone(),
        table.log_store(),
        DeltaScanConfig {
            file_column_name: Some("file_path".to_string()),
        },
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("t1", Arc::new(provider))?;

    let files = ctx
        .sql("select file_path as file from t1 group by file")
        .await?
        .with_column("r", random())?
        .filter(col("r").lt_eq(lit(parameters.sample_files)))?;

    let file_sample = files.collect_partitioned().await?;
    let schema = file_sample.get(0).unwrap().get(0).unwrap().schema();
    let mem_table = Arc::new(MemTable::try_new(schema, file_sample)?);
    ctx.register_table("file_sample", mem_table)?;
    let file_sample_count = ctx.table("file_sample").await?.count().await?;

    let row_sample = ctx.table("t1").await?.join(
        ctx.table("file_sample").await?,
        datafusion_common::JoinType::Inner,
        &["file_path"],
        &["file"],
        None,
    )?;

    let matched = row_sample
        .clone()
        .filter(random().lt_eq(lit(parameters.sample_matched_rows)))?;

    let rand = cast(random() * lit(u32::MAX), DataType::Int64);
    let not_matched = row_sample
        .filter(random().lt_eq(lit(parameters.sample_not_matched_rows)))?
        .with_column("wr_item_sk", rand.clone())?
        .with_column("wr_order_number", rand)?;

    let source = matched.union(not_matched)?;

    let start = Instant::now();
    let (table, metrics) = merge(source, table)?.await?;
    let end = Instant::now();

    let duration = end.duration_since(start);

    println!("Total File count: {}", file_count);
    println!("File sample count: {}", file_sample_count);
    println!("{:?}", metrics);
    println!("Seconds: {}", duration.as_secs_f32());

    // Clean up and restore to original state.
    let (table, _) = DeltaOps(table).restore().with_version_to_restore(0).await?;
    let (table, _) = DeltaOps(table)
        .vacuum()
        .with_retention_period(Duration::seconds(0))
        .with_enforce_retention_duration(false)
        .await?;
    table
        .object_store()
        .delete(&Path::parse("_delta_log/00000000000000000001.json")?)
        .await?;
    table
        .object_store()
        .delete(&Path::parse("_delta_log/00000000000000000002.json")?)
        .await?;

    Ok((duration, metrics))
}

#[derive(Subcommand, Debug)]
enum Command {
    Convert(Convert),
    Bench(BenchArg),
    Standard(Standard),
    Compare(Compare),
    Show(Show),
}

#[derive(Debug, Args)]
struct Convert {
    tpcds_path: String,
    delta_path: String,
}

#[derive(Debug, Args)]
struct Standard {
    delta_path: String,
    samples: Option<u32>,
    output_path: Option<String>,
    group_id: Option<String>,
}

#[derive(Debug, Args)]
struct Compare {
    before_path: String,
    before_group_id: String,
    after_path: String,
    after_group_id: String,
}

#[derive(Debug, Args)]
struct Show {
    path: String,
}

#[derive(Debug, Args)]
struct BenchArg {
    table_path: String,
    #[command(subcommand)]
    name: MergeBench,
}

struct Bench {
    name: String,
    op: fn(DataFrame, DeltaTable) -> Result<MergeBuilder, DeltaTableError>,
    params: MergePerfParams,
}

impl Bench {
    fn new<S: ToString>(
        name: S,
        op: fn(DataFrame, DeltaTable) -> Result<MergeBuilder, DeltaTableError>,
        params: MergePerfParams,
    ) -> Self {
        Bench {
            name: name.to_string(),
            op,
            params,
        }
    }
}

#[derive(Debug, Args, Clone)]
struct MergePerfParams {
    pub sample_files: f32,
    pub sample_matched_rows: f32,
    pub sample_not_matched_rows: f32,
}

#[derive(Debug, Clone, Subcommand)]
enum MergeBench {
    Upsert(MergePerfParams),
    Delete(MergePerfParams),
    Insert(MergePerfParams),
}

#[derive(Parser, Debug)]
#[command(about)]
struct MergePrefArgs {
    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() {
    match MergePrefArgs::parse().command {
        Command::Convert(Convert {
            tpcds_path,
            delta_path,
        }) => {
            convert_tpcds_web_returns(tpcds_path, delta_path)
                .await
                .unwrap();
        }
        Command::Bench(BenchArg { table_path, name }) => {
            let (merge_op, params): (
                fn(DataFrame, DeltaTable) -> Result<MergeBuilder, DeltaTableError>,
                MergePerfParams,
            ) = match name {
                MergeBench::Upsert(params) => (merge_upsert, params),
                MergeBench::Delete(params) => (merge_delete, params),
                MergeBench::Insert(params) => (merge_insert, params),
            };

            benchmark_merge_tpcds(table_path, params, merge_op)
                .await
                .unwrap();
        }
        Command::Standard(Standard {
            delta_path,
            samples,
            output_path,
            group_id,
        }) => {
            let benches = vec![Bench::new(
                "delete_only_fileMatchedFraction_0.05_rowMatchedFraction_0.05",
                merge_delete,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.05,
                    sample_not_matched_rows: 0.0,
                },
            ),
            Bench::new(
                "multiple_insert_only_fileMatchedFraction_0.05_rowNotMatchedFraction_0.05",
                merge_insert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.00,
                    sample_not_matched_rows: 0.05,
                },
            ),
            Bench::new(
                "multiple_insert_only_fileMatchedFraction_0.05_rowNotMatchedFraction_0.50",
                merge_insert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.00,
                    sample_not_matched_rows: 0.50,
                },
            ),
            Bench::new(
                "multiple_insert_only_fileMatchedFraction_0.05_rowNotMatchedFraction_1.0",
                merge_insert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.00,
                    sample_not_matched_rows: 1.0,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_0.01_rowNotMatchedFraction_0.1",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.01,
                    sample_not_matched_rows: 0.1,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_0.0_rowNotMatchedFraction_0.1",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.00,
                    sample_not_matched_rows: 0.1,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_0.1_rowNotMatchedFraction_0.0",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.1,
                    sample_not_matched_rows: 0.0,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_0.1_rowNotMatchedFraction_0.01",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.1,
                    sample_not_matched_rows: 0.01,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_0.5_rowNotMatchedFraction_0.001",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.5,
                    sample_not_matched_rows: 0.001,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_0.99_rowNotMatchedFraction_0.001",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 0.99,
                    sample_not_matched_rows: 0.001,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.05_rowMatchedFraction_1.0_rowNotMatchedFraction_0.001",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.05,
                    sample_matched_rows: 1.0,
                    sample_not_matched_rows: 0.001,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_0.5_rowMatchedFraction_0.001_rowNotMatchedFraction_0.001",
                merge_upsert,
                MergePerfParams {
                    sample_files: 0.5,
                    sample_matched_rows: 0.001,
                    sample_not_matched_rows: 0.001,
                },
            ),
            Bench::new(
                "upsert_fileMatchedFraction_1.0_rowMatchedFraction_0.001_rowNotMatchedFraction_0.001",
                merge_upsert,
                MergePerfParams {
                    sample_files: 1.0,
                    sample_matched_rows: 0.001,
                    sample_not_matched_rows: 0.001,
                },
            )
            ];

            let num_samples = samples.unwrap_or(1);
            let group_id = group_id.unwrap_or(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .to_string(),
            );
            let output = output_path.unwrap_or("data/benchmarks".into());

            let mut group_ids = vec![];
            let mut name = vec![];
            let mut samples = vec![];
            let mut duration_ms = vec![];
            let mut data = vec![];

            for bench in benches {
                for sample in 0..num_samples {
                    println!("Test: {} Sample: {}", bench.name, sample);
                    let res =
                        benchmark_merge_tpcds(delta_path.clone(), bench.params.clone(), bench.op)
                            .await
                            .unwrap();

                    group_ids.push(group_id.clone());
                    name.push(bench.name.clone());
                    samples.push(sample);
                    duration_ms.push(res.0.as_millis() as u32);
                    data.push(json!(res.1).to_string());
                }
            }

            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("group_id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("sample", DataType::UInt32, false),
                Field::new("duration_ms", DataType::UInt32, false),
                Field::new("data", DataType::Utf8, true),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(group_ids)),
                    Arc::new(StringArray::from(name)),
                    Arc::new(UInt32Array::from(samples)),
                    Arc::new(UInt32Array::from(duration_ms)),
                    Arc::new(StringArray::from(data)),
                ],
            )
            .unwrap();

            DeltaOps::try_from_uri(output)
                .await
                .unwrap()
                .write(vec![batch])
                .with_save_mode(SaveMode::Append)
                .await
                .unwrap();
        }
        Command::Compare(Compare {
            before_path,
            before_group_id,
            after_path,
            after_group_id,
        }) => {
            let before_table = DeltaTableBuilder::from_uri(before_path)
                .load()
                .await
                .unwrap();
            let after_table = DeltaTableBuilder::from_uri(after_path)
                .load()
                .await
                .unwrap();

            let ctx = SessionContext::new();
            ctx.register_table("before", Arc::new(before_table))
                .unwrap();
            ctx.register_table("after", Arc::new(after_table)).unwrap();

            let before_stats = ctx
                .sql(&format!(
                    "
                select name as before_name,
                 avg(cast(duration_ms as float)) as before_duration_avg 
                from before where group_id = {} 
                group by name
            ",
                    before_group_id
                ))
                .await
                .unwrap();

            let after_stats = ctx
                .sql(&format!(
                    "
                select name as after_name,
                 avg(cast(duration_ms as float)) as after_duration_avg 
                from after where group_id = {} 
                group by name
            ",
                    after_group_id
                ))
                .await
                .unwrap();

            before_stats
                .join(
                    after_stats,
                    datafusion_common::JoinType::Inner,
                    &["before_name"],
                    &["after_name"],
                    None,
                )
                .unwrap()
                .select(vec![
                    col("before_name").alias("name"),
                    col("before_duration_avg"),
                    col("after_duration_avg"),
                    (col("before_duration_avg") / (col("after_duration_avg"))),
                ])
                .unwrap()
                .sort(vec![col("name").sort(true, true)])
                .unwrap()
                .show()
                .await
                .unwrap();
        }
        Command::Show(Show { path }) => {
            let stats = DeltaTableBuilder::from_uri(path).load().await.unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("stats", Arc::new(stats)).unwrap();

            ctx.sql("select * from stats")
                .await
                .unwrap()
                .show()
                .await
                .unwrap();
        }
    }
}
