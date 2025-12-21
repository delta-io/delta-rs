use std::fmt;
use std::path::Path;

use deltalake_core::datafusion::functions::expr_fn;
use deltalake_core::datafusion::{
    logical_expr::{cast, lit},
    prelude::{DataFrame, ParquetReadOptions, SessionContext},
};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::{StructField, StructType};
use deltalake_core::operations::merge::{InsertBuilder, MergeBuilder, MergeMetrics, UpdateBuilder};
use deltalake_core::{arrow, DeltaResult, DeltaTable, DeltaTableError};
use tempfile::TempDir;
use url::Url;

pub type MergeOp = fn(DataFrame, DeltaTable) -> Result<MergeBuilder, DeltaTableError>;

#[derive(Clone, Copy, Debug)]
pub struct MergePerfParams {
    pub sample_matched_rows: f32,
    pub sample_not_matched_rows: f32,
}

#[derive(Clone, Copy)]
pub enum MergeScenario {
    SingleInsertOnly,
    MultipleInsertOnly,
    DeleteOnly,
    Upsert,
}

type MergeValidator = fn(&MergeMetrics, &MergeTestCase) -> DeltaResult<()>;

#[derive(Clone, Copy)]
pub struct MergeTestCase {
    pub name: &'static str,
    pub scenario: MergeScenario,
    pub params: MergePerfParams,
    validator: MergeValidator,
}

impl fmt::Debug for MergeTestCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeTestCase")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl MergeTestCase {
    fn builder(
        &self,
        source: DataFrame,
        table: DeltaTable,
    ) -> Result<MergeBuilder, DeltaTableError> {
        match self.scenario {
            MergeScenario::SingleInsertOnly => merge_insert(source, table),
            MergeScenario::MultipleInsertOnly => merge_multiple_insert(source, table),
            MergeScenario::DeleteOnly => merge_delete(source, table),
            MergeScenario::Upsert => merge_upsert(source, table),
        }
    }

    pub async fn execute(
        &self,
        source: DataFrame,
        table: DeltaTable,
    ) -> DeltaResult<(DeltaTable, MergeMetrics)> {
        self.builder(source, table)?.await
    }

    pub fn validate(&self, metrics: &MergeMetrics) -> DeltaResult<()> {
        (self.validator)(metrics, self)
    }
}

fn validate_insert_only(metrics: &MergeMetrics, case: &MergeTestCase) -> DeltaResult<()> {
    ensure_zero(
        metrics.num_target_rows_updated,
        "num_target_rows_updated",
        case,
    )?;
    ensure_zero(
        metrics.num_target_rows_deleted,
        "num_target_rows_deleted",
        case,
    )
}

fn validate_delete_only(metrics: &MergeMetrics, case: &MergeTestCase) -> DeltaResult<()> {
    ensure_zero(
        metrics.num_target_rows_inserted,
        "num_target_rows_inserted",
        case,
    )?;
    ensure_zero(
        metrics.num_target_rows_updated,
        "num_target_rows_updated",
        case,
    )
}

fn validate_upsert(metrics: &MergeMetrics, case: &MergeTestCase) -> DeltaResult<()> {
    ensure_zero(
        metrics.num_target_rows_deleted,
        "num_target_rows_deleted",
        case,
    )
}

fn ensure_zero(value: usize, field: &str, case: &MergeTestCase) -> DeltaResult<()> {
    if value == 0 {
        Ok(())
    } else {
        Err(DeltaTableError::generic(format!(
            "case '{}' expected {} == 0, found {}",
            case.name, field, value
        )))
    }
}

const INSERT_ONLY_CASES: [MergeTestCase; 6] = [
    MergeTestCase {
        name: "single_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_0.05",
        scenario: MergeScenario::SingleInsertOnly,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 0.05,
        },
        validator: validate_insert_only,
    },
    MergeTestCase {
        name: "single_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_0.5",
        scenario: MergeScenario::SingleInsertOnly,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 0.5,
        },
        validator: validate_insert_only,
    },
    MergeTestCase {
        name: "single_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_1.0",
        scenario: MergeScenario::SingleInsertOnly,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 1.0,
        },
        validator: validate_insert_only,
    },
    MergeTestCase {
        name: "multiple_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_0.05",
        scenario: MergeScenario::MultipleInsertOnly,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 0.05,
        },
        validator: validate_insert_only,
    },
    MergeTestCase {
        name: "multiple_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_0.5",
        scenario: MergeScenario::MultipleInsertOnly,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 0.5,
        },
        validator: validate_insert_only,
    },
    MergeTestCase {
        name: "multiple_insert_only_filesMatchedFraction_0.05_rowsNotMatchedFraction_1.0",
        scenario: MergeScenario::MultipleInsertOnly,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 1.0,
        },
        validator: validate_insert_only,
    },
];

const DELETE_ONLY_CASES: [MergeTestCase; 1] = [MergeTestCase {
    name: "delete_only_filesMatchedFraction_0.05_rowsMatchedFraction_0.05",
    scenario: MergeScenario::DeleteOnly,
    params: MergePerfParams {
        sample_matched_rows: 0.05,
        sample_not_matched_rows: 0.0,
    },
    validator: validate_delete_only,
}];

const UPSERT_CASES: [MergeTestCase; 9] = [
    MergeTestCase {
        name: "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.0_rowsNotMatchedFraction_0.1",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.0,
            sample_not_matched_rows: 0.1,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name:
            "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.01_rowsNotMatchedFraction_0.1",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.1,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name: "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.1_rowsNotMatchedFraction_0.1",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.1,
            sample_not_matched_rows: 0.1,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name:
            "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.5_rowsNotMatchedFraction_0.001",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.5,
            sample_not_matched_rows: 0.001,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name:
            "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.99_rowsNotMatchedFraction_0.001",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.99,
            sample_not_matched_rows: 0.001,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name:
            "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_1.0_rowsNotMatchedFraction_0.001",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 1.0,
            sample_not_matched_rows: 0.001,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name: "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.1_rowsNotMatchedFraction_0.0",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.1,
            sample_not_matched_rows: 0.0,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name:
            "upsert_filesMatchedFraction_0.5_rowsMatchedFraction_0.01_rowsNotMatchedFraction_0.001",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.001,
        },
        validator: validate_upsert,
    },
    MergeTestCase {
        name:
            "upsert_filesMatchedFraction_1.0_rowsMatchedFraction_0.01_rowsNotMatchedFraction_0.001",
        scenario: MergeScenario::Upsert,
        params: MergePerfParams {
            sample_matched_rows: 0.01,
            sample_not_matched_rows: 0.001,
        },
        validator: validate_upsert,
    },
];

fn all_cases_iter() -> impl Iterator<Item = &'static MergeTestCase> {
    INSERT_ONLY_CASES
        .iter()
        .chain(DELETE_ONLY_CASES.iter())
        .chain(UPSERT_CASES.iter())
}

pub fn insert_only_cases() -> &'static [MergeTestCase] {
    &INSERT_ONLY_CASES
}

pub fn delete_only_cases() -> &'static [MergeTestCase] {
    &DELETE_ONLY_CASES
}

pub fn upsert_cases() -> &'static [MergeTestCase] {
    &UPSERT_CASES
}

pub fn merge_case_names() -> Vec<&'static str> {
    all_cases_iter().map(|case| case.name).collect()
}

pub fn merge_case_by_name(name: &str) -> Option<&'static MergeTestCase> {
    all_cases_iter().find(|case| case.name.eq_ignore_ascii_case(name))
}

pub fn merge_test_cases() -> Vec<&'static MergeTestCase> {
    all_cases_iter().collect()
}

fn apply_insert_projection(builder: InsertBuilder) -> InsertBuilder {
    builder
        .set("wr_returned_date_sk", "source.wr_returned_date_sk")
        .set("wr_returned_time_sk", "source.wr_returned_time_sk")
        .set("wr_item_sk", "source.wr_item_sk")
        .set("wr_refunded_customer_sk", "source.wr_refunded_customer_sk")
        .set("wr_refunded_cdemo_sk", "source.wr_refunded_cdemo_sk")
        .set("wr_refunded_hdemo_sk", "source.wr_refunded_hdemo_sk")
        .set("wr_refunded_addr_sk", "source.wr_refunded_addr_sk")
        .set(
            "wr_returning_customer_sk",
            "source.wr_returning_customer_sk",
        )
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
}

fn apply_update_projection(builder: UpdateBuilder) -> UpdateBuilder {
    builder
        .update("wr_returned_date_sk", "source.wr_returned_date_sk")
        .update("wr_returned_time_sk", "source.wr_returned_time_sk")
        .update("wr_item_sk", "source.wr_item_sk")
        .update("wr_refunded_customer_sk", "source.wr_refunded_customer_sk")
        .update("wr_refunded_cdemo_sk", "source.wr_refunded_cdemo_sk")
        .update("wr_refunded_hdemo_sk", "source.wr_refunded_hdemo_sk")
        .update("wr_refunded_addr_sk", "source.wr_refunded_addr_sk")
        .update(
            "wr_returning_customer_sk",
            "source.wr_returning_customer_sk",
        )
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
}

pub fn merge_upsert(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    table
        .merge(
            source,
            "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number",
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(apply_update_projection)?
        .when_not_matched_insert(apply_insert_projection)
}

pub fn merge_insert(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    table
        .merge(
            source,
            "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number",
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(apply_insert_projection)
}

fn merge_multiple_insert(
    source: DataFrame,
    table: DeltaTable,
) -> Result<MergeBuilder, DeltaTableError> {
    table
        .merge(
            source,
            "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number",
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| {
            apply_insert_projection(insert.predicate("source.wr_item_sk % 2 = 0"))
        })?
        .when_not_matched_insert(apply_insert_projection)
}

pub fn merge_delete(source: DataFrame, table: DeltaTable) -> Result<MergeBuilder, DeltaTableError> {
    table
        .merge(
            source,
            "source.wr_item_sk = target.wr_item_sk and source.wr_order_number = target.wr_order_number",
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_delete(|delete| delete)
}

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
    let table = DeltaTable::try_from_url(temp_table_url)
        .await?
        .create()
        .with_columns(fields)
        .await?;

    let table = table.write(batches).await?;

    let source = ctx
        .read_parquet(&parquet_path, ParquetReadOptions::default())
        .await?;

    let matched = source
        .clone()
        .filter(expr_fn::random().lt_eq(lit(params.sample_matched_rows)))?;

    let rand = cast(
        expr_fn::random() * lit(u32::MAX),
        arrow::datatypes::DataType::Int64,
    );
    let not_matched = source
        .filter(expr_fn::random().lt_eq(lit(params.sample_not_matched_rows)))?
        .with_column("wr_item_sk", rand.clone())?
        .with_column("wr_order_number", rand)?;

    let source = matched.union(not_matched)?;
    Ok((source, table))
}
