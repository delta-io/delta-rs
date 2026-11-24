//! Pruning-equivalence fuzzer.
//!
//! Each seed writes a small Delta table full of edge values (NaN, -0.0, nulls, extremes, files
//! where min equals max) and checks generated predicates two ways against the same rows loaded
//! into a DataFusion `MemTable`, which has no Delta statistics and prunes nothing:
//!
//! - read: `SELECT ... WHERE p` returns the same rows from the Delta table as from the reference.
//! - delete: after `delete(p)`, the table holds exactly the reference rows where `p` is not true.
//!
//! A failure is shrunk to the fewest files and rows that still fail, then printed.
//!
//! ```text
//! FUZZ_SEEDS=500 cargo test -p deltalake-core --features datafusion --test it_datafusion \
//!     pruning_fuzz -- --ignored --nocapture
//! ```
//!
//! `FUZZ_SEED=<n>` replays one seed.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::StringArray;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableBuilder};
use object_store::memory::InMemory;
use url::Url;

/// SplitMix64, so a seed means the same run regardless of the `rand` version.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }

    fn percent(&mut self, percent: u64) -> bool {
        self.next_u64() % 100 < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const FLOATS: [f64; 8] = [
    f64::NAN,
    -0.0,
    0.0,
    9.0,
    -1.5,
    2.5,
    f64::INFINITY,
    f64::NEG_INFINITY,
];
const INTS: [i32; 7] = [-1, 0, 5, 7, 8, 9, i32::MAX];
const STRINGS: [&str; 4] = ["", "a", "b", "ab"];
/// Unscaled `decimal(38,18)` values: 1.123456789012345678 and its negation have no exact `f64`.
const DECIMALS: [i128; 5] = [
    1_123_456_789_012_345_678,
    -1_123_456_789_012_345_678,
    1_000_000_000_000_000_000,
    2_500_000_000_000_000_000,
    0,
];
const PARTITION_FLOATS: [f64; 4] = [-0.0, 0.0, 5.0, 1.5];
const PARTITION_STRINGS: [&str; 3] = ["x", "y", "z"];

const FLOAT_LITERALS: [&str; 9] = [
    "-0.0",
    "0.0",
    "0",
    "9.0",
    "9",
    "2.5",
    "-1.5",
    "CAST('NaN' AS DOUBLE)",
    "CAST('inf' AS DOUBLE)",
];
const INT_LITERALS: [&str; 7] = ["5", "7", "8.5", "0", "-1", "9", "2147483647"];
const STRING_LITERALS: [&str; 4] = ["''", "'a'", "'b'", "'ab'"];
const DECIMAL_LITERALS: [&str; 5] = [
    "CAST('1.123456789012345678' AS DECIMAL(38, 18))",
    "CAST('-1.123456789012345678' AS DECIMAL(38, 18))",
    "CAST('1' AS DECIMAL(38, 18))",
    "2.5",
    "0",
];
const PARTITION_FLOAT_LITERALS: [&str; 5] = ["0.0", "-0.0", "0", "5.0", "1.5"];
const PARTITION_STRING_LITERALS: [&str; 3] = ["'x'", "'y'", "'z'"];
const BASE_COLUMNS: &str = "id, float_value, int_value, string_value, bool_value, decimal_value";
const SEEDS_PASSING_ON_MAIN: [u64; 30] = [
    0, 3, 6, 7, 8, 9, 11, 14, 15, 17, 18, 21, 28, 29, 30, 31, 32, 33, 34, 39, 41, 42, 43, 44, 45,
    47, 48, 49, 50, 51,
];
const COMPARISONS: [&str; 6] = ["=", "!=", "<", "<=", ">", ">="];

#[derive(Clone, Copy, Debug, PartialEq)]
enum Partition {
    None,
    Float,
    Text,
}

#[derive(Clone, Debug)]
struct Row {
    id: i64,
    float_value: Option<f64>,
    int_value: Option<i32>,
    string_value: Option<String>,
    bool_value: Option<bool>,
    decimal_value: Option<i128>,
    partition_float: Option<f64>,
    partition_string: Option<String>,
}

#[derive(Clone, Debug)]
struct Case {
    partition: Partition,
    files: Vec<Vec<Row>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Oracle {
    Read,
    Delete,
}

/// How one column is filled within a file, chosen to produce interesting statistics.
#[derive(Clone, Copy)]
enum Fill {
    Random,
    Constant,
    NanPlusOne,
}

fn schema(partition: Partition) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("float_value", DataType::Float64, true),
        Field::new("int_value", DataType::Int32, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("bool_value", DataType::Boolean, true),
        Field::new("decimal_value", DataType::Decimal128(38, 18), true),
    ];
    match partition {
        Partition::None => {}
        Partition::Float => fields.push(Field::new("partition_float", DataType::Float64, true)),
        Partition::Text => fields.push(Field::new("partition_string", DataType::Utf8, true)),
    }
    Arc::new(ArrowSchema::new(fields))
}

fn columns(partition: Partition) -> String {
    match partition {
        Partition::None => BASE_COLUMNS.to_string(),
        Partition::Float => format!("{BASE_COLUMNS}, partition_float"),
        Partition::Text => format!("{BASE_COLUMNS}, partition_string"),
    }
}

fn batch(partition: Partition, rows: &[Row]) -> RecordBatch {
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.id))),
        Arc::new(Float64Array::from_iter(
            rows.iter().map(|row| row.float_value),
        )),
        Arc::new(Int32Array::from_iter(rows.iter().map(|row| row.int_value))),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.string_value.clone()),
        )),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| row.bool_value),
        )),
        Arc::new(
            Decimal128Array::from_iter(rows.iter().map(|row| row.decimal_value))
                .with_precision_and_scale(38, 18)
                .unwrap(),
        ),
    ];
    match partition {
        Partition::None => {}
        Partition::Float => arrays.push(Arc::new(Float64Array::from_iter(
            rows.iter().map(|row| row.partition_float),
        ))),
        Partition::Text => arrays.push(Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.partition_string.clone()),
        ))),
    }
    RecordBatch::try_new(schema(partition), arrays).unwrap()
}

/// Pick a value for one column of one row given the file's fill for that column.
fn value<T: Copy>(
    rng: &mut Rng,
    fill: Fill,
    pool: &[T],
    constant: T,
    other: T,
    row: usize,
) -> Option<T> {
    match fill {
        Fill::Random if rng.percent(15) => None,
        Fill::Random => Some(*rng.pick(pool)),
        Fill::Constant => Some(constant),
        Fill::NanPlusOne => Some(if row == 0 { constant } else { other }),
    }
}

fn generate_case(rng: &mut Rng) -> Case {
    let partition = *rng.pick(&[
        Partition::None,
        Partition::None,
        Partition::Float,
        Partition::Text,
    ]);
    let mut files = vec![];
    let mut next_id = 1;
    for _ in 0..1 + rng.below(4) {
        let fills = [Fill::Random, Fill::Constant, Fill::NanPlusOne];
        let float_fill = *rng.pick(&fills);
        let int_fill = *rng.pick(&fills[..2]);
        let string_fill = *rng.pick(&fills[..2]);
        let float_constant = if matches!(float_fill, Fill::NanPlusOne) {
            f64::NAN
        } else {
            *rng.pick(&FLOATS)
        };
        let float_other = *rng.pick(&FLOATS);
        let int_constant = *rng.pick(&INTS);
        let string_constant = *rng.pick(&STRINGS);
        let decimal_fill = *rng.pick(&fills[..2]);
        let decimal_constant = *rng.pick(&DECIMALS);
        let partition_float = *rng.pick(&PARTITION_FLOATS);
        let partition_string = *rng.pick(&PARTITION_STRINGS);
        let mut rows = vec![];
        for row in 0..1 + rng.below(4) {
            rows.push(Row {
                id: next_id,
                float_value: value(rng, float_fill, &FLOATS, float_constant, float_other, row),
                int_value: value(rng, int_fill, &INTS, int_constant, int_constant, row),
                string_value: value(
                    rng,
                    string_fill,
                    &STRINGS,
                    string_constant,
                    string_constant,
                    row,
                )
                .map(str::to_string),
                bool_value: if rng.percent(20) {
                    None
                } else {
                    Some(rng.percent(50))
                },
                decimal_value: value(
                    rng,
                    decimal_fill,
                    &DECIMALS,
                    decimal_constant,
                    decimal_constant,
                    row,
                ),
                partition_float: (partition == Partition::Float).then_some(partition_float),
                partition_string: (partition == Partition::Text)
                    .then(|| partition_string.to_string()),
            });
            next_id += 1;
        }
        files.push(rows);
    }
    Case { partition, files }
}

fn atom(rng: &mut Rng, partition: Partition) -> String {
    let mut choices = vec![
        "float_value",
        "float_value",
        "int_value",
        "string_value",
        "bool_value",
        "decimal_value",
    ];
    match partition {
        Partition::None => {}
        Partition::Float => choices.extend(["partition_float", "partition_float"]),
        Partition::Text => choices.push("partition_string"),
    }
    let column = *rng.pick(&choices);
    let literals: &[&str] = match column {
        "float_value" => &FLOAT_LITERALS,
        "int_value" => &INT_LITERALS,
        "string_value" => &STRING_LITERALS,
        "decimal_value" => &DECIMAL_LITERALS,
        "partition_float" => &PARTITION_FLOAT_LITERALS,
        "partition_string" => &PARTITION_STRING_LITERALS,
        _ => &["TRUE", "FALSE"],
    };
    let numeric = matches!(
        column,
        "float_value" | "int_value" | "decimal_value" | "partition_float"
    );
    match rng.below(9) {
        6 => format!("{column} = NULL"),
        7 if numeric => format!(
            "{column} + 1 {} {}",
            rng.pick(&COMPARISONS),
            rng.pick(literals)
        ),
        7 => format!(
            "CASE WHEN {column} = {} THEN true ELSE false END",
            rng.pick(literals)
        ),
        8 => format!(
            "NOT coalesce({column} {} {}, false)",
            rng.pick(&COMPARISONS),
            rng.pick(literals)
        ),
        0 => format!("{column} IS NULL"),
        1 => format!("{column} IS NOT NULL"),
        2 => format!(
            "{column} IN ({}, {})",
            rng.pick(literals),
            rng.pick(literals)
        ),
        3 => {
            let not = if rng.percent(50) { "NOT " } else { "" };
            format!("{column} IS {not}DISTINCT FROM {}", rng.pick(literals))
        }
        _ if column == "bool_value" => format!("{column} = {}", rng.pick(literals)),
        _ => format!("{column} {} {}", rng.pick(&COMPARISONS), rng.pick(literals)),
    }
}

fn predicate(rng: &mut Rng, partition: Partition, depth: u32) -> String {
    if depth == 0 || rng.percent(50) {
        return atom(rng, partition);
    }
    match rng.below(3) {
        0 => format!("NOT ({})", predicate(rng, partition, depth - 1)),
        1 => format!(
            "({}) AND ({})",
            predicate(rng, partition, depth - 1),
            predicate(rng, partition, depth - 1)
        ),
        _ => format!(
            "({}) OR ({})",
            predicate(rng, partition, depth - 1),
            predicate(rng, partition, depth - 1)
        ),
    }
}

/// Write `case` as a Delta table on a fresh in-memory store, one commit per file.
async fn write_table(case: &Case) -> DeltaResult<DeltaTable> {
    let url = Url::parse("memory:///fuzz").unwrap();
    let store = Arc::new(InMemory::new());
    let mut table = DeltaTableBuilder::from_url(url.clone())?
        .with_storage_backend(store, url)
        .build()?;
    let partition_columns: Vec<String> = match case.partition {
        Partition::None => vec![],
        Partition::Float => vec!["partition_float".into()],
        Partition::Text => vec!["partition_string".into()],
    };
    for (index, rows) in case.files.iter().enumerate() {
        let mode = if index == 0 {
            SaveMode::ErrorIfExists
        } else {
            SaveMode::Append
        };
        table = table
            .write(vec![batch(case.partition, rows)])
            .with_save_mode(mode)
            .with_partition_columns(partition_columns.clone())
            .await?;
    }
    Ok(table)
}

/// Run `sql` and return each row as one string, sorted, so results compare as multisets.
async fn rows_of(context: &SessionContext, sql: &str) -> datafusion::error::Result<Vec<String>> {
    let batches = context.sql(sql).await?.collect().await?;
    let mut rows = vec![];
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).unwrap())
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    Ok(rows)
}

fn reference(case: &Case) -> SessionContext {
    let context = SessionContext::new();
    let batches: Vec<RecordBatch> = case
        .files
        .iter()
        .map(|rows| batch(case.partition, rows))
        .collect();
    let table = MemTable::try_new(schema(case.partition), vec![batches]).unwrap();
    context.register_table("t", Arc::new(table)).unwrap();
    context
}

async fn delta_context(table: &DeltaTable) -> SessionContext {
    let context = SessionContext::new();
    table.update_datafusion_session(&context.state()).unwrap();
    context
        .register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    context
}

/// Check `predicate` with `oracle` on `case`. `None` means it agrees with the reference, or that
/// the reference itself can't run it; `Some` describes the mismatch.
async fn check(case: &Case, oracle: Oracle, predicate: &str) -> Option<String> {
    let columns = columns(case.partition);
    let table = match write_table(case).await {
        Ok(table) => table,
        Err(err) => return Some(format!("write failed: {err}")),
    };
    match oracle {
        Oracle::Read => {
            let sql = format!("SELECT {columns} FROM t WHERE {predicate}");
            let expected = rows_of(&reference(case), &sql).await.ok()?;
            match rows_of(&delta_context(&table).await, &sql).await {
                Ok(actual) if actual == expected => None,
                Ok(actual) => Some(diff(&expected, &actual)),
                Err(err) => Some(format!("read failed: {err}\nexpected: {expected:?}")),
            }
        }
        Oracle::Delete => {
            let sql = format!("SELECT {columns} FROM t WHERE ({predicate}) IS NOT TRUE");
            let expected = rows_of(&reference(case), &sql).await.ok()?;
            let table = match table.delete().with_predicate(predicate).await {
                Ok((table, _)) => table,
                Err(err) => {
                    return Some(format!("delete failed: {err}\nexpected left: {expected:?}"));
                }
            };
            let all = format!("SELECT {columns} FROM t");
            match rows_of(&delta_context(&table).await, &all).await {
                Ok(actual) if actual == expected => None,
                Ok(actual) => Some(diff(&expected, &actual)),
                Err(err) => Some(format!("read after delete failed: {err}")),
            }
        }
    }
}

fn diff(expected: &[String], actual: &[String]) -> String {
    let expected_set: BTreeSet<_> = expected.iter().collect();
    let actual_set: BTreeSet<_> = actual.iter().collect();
    let missing: Vec<_> = expected_set.difference(&actual_set).collect();
    let extra: Vec<_> = actual_set.difference(&expected_set).collect();
    format!("missing rows: {missing:?}\nextra rows: {extra:?}")
}

/// Remove files, then rows, while the same oracle still fails on `predicate`.
async fn shrink(mut case: Case, oracle: Oracle, predicate: &str) -> (Case, String) {
    let Some(mut failure) = check(&case, oracle, predicate).await else {
        return (case, "did not fail again when rerun".to_string());
    };
    let mut file = 0;
    while file < case.files.len() && case.files.len() > 1 {
        let mut smaller = case.clone();
        smaller.files.remove(file);
        match check(&smaller, oracle, predicate).await {
            Some(found) => (case, failure) = (smaller, found),
            None => file += 1,
        }
    }
    for file in 0..case.files.len() {
        let mut row = 0;
        while row < case.files[file].len() && case.files[file].len() > 1 {
            let mut smaller = case.clone();
            smaller.files[file].remove(row);
            match check(&smaller, oracle, predicate).await {
                Some(found) => (case, failure) = (smaller, found),
                None => row += 1,
            }
        }
    }
    (case, failure)
}

fn describe(case: &Case) -> String {
    let mut lines = vec![format!("partition: {:?}", case.partition)];
    for (index, rows) in case.files.iter().enumerate() {
        lines.push(format!("file {index}:"));
        for row in rows {
            lines.push(format!(
                "  id={} float_value={:?} int_value={:?} string_value={:?} bool_value={:?} decimal_value={:?} partition_float={:?} partition_string={:?}",
                row.id,
                row.float_value,
                row.int_value,
                row.string_value,
                row.bool_value,
                row.decimal_value,
                row.partition_float,
                row.partition_string
            ));
        }
    }
    lines.join("\n")
}

/// Run one seed: a table, a few read predicates and one delete. Returns shrunk failures.
async fn run_seed(seed: u64) -> Vec<String> {
    let mut rng = Rng(seed);
    let case = generate_case(&mut rng);
    let mut checks: Vec<(Oracle, String)> = (0..6)
        .map(|_| (Oracle::Read, predicate(&mut rng, case.partition, 2)))
        .collect();
    for _ in 0..3 {
        checks.push((Oracle::Delete, predicate(&mut rng, case.partition, 2)));
    }
    let mut failures = vec![];
    for (oracle, predicate) in checks {
        if check(&case, oracle, &predicate).await.is_some() {
            let (smallest, failure) = shrink(case.clone(), oracle, &predicate).await;
            failures.push(format!(
                "seed {seed} {oracle:?} `{predicate}`\n{}\n{failure}",
                describe(&smallest)
            ));
        }
    }
    failures
}

async fn check_seeds(seeds: impl IntoIterator<Item = u64>) {
    let mut failures = vec![];
    for seed in seeds {
        failures.extend(run_seed(seed).await);
    }
    for failure in &failures {
        println!("----\n{failure}");
    }
    assert!(failures.is_empty(), "{} failing checks", failures.len());
}

#[tokio::test]
#[ignore = "long-running fuzzer; run with --ignored"]
async fn pruning_fuzz() {
    match std::env::var("FUZZ_SEED") {
        Ok(seed) => check_seeds([seed.parse().unwrap()]).await,
        Err(_) => {
            let count = std::env::var("FUZZ_SEEDS").map_or(200, |count| count.parse().unwrap());
            check_seeds(0..count).await;
        }
    }
}

#[tokio::test]
async fn pruning_fuzz_passing_seeds() {
    check_seeds(SEEDS_PASSING_ON_MAIN).await;
}
