// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// This product includes software from the Datafusion project (Apache 2.0)
// https://github.com/apache/arrow-datafusion
// Display functions and required macros were pulled from https://github.com/apache/arrow-datafusion/blob/ddb95497e2792015d5a5998eec79aac8d37df1eb/datafusion/expr/src/expr.rs

//! Utility functions for Datafusion's Expressions
use std::fmt::{self, Display, Error, Formatter, Write};
use std::sync::Arc;

use arrow_array::{Array, GenericListArray};
use arrow_schema::{DataType, Field};
use chrono::{DateTime, NaiveDate};
use datafusion::common::Result as DFResult;
use datafusion::common::{config::ConfigOptions, DFSchema, Result, ScalarValue, TableReference};
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::functions_nested::make_array::MakeArray;
use datafusion::functions_nested::planner::{FieldAccessPlanner, NestedFunctionPlanner};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{
    AggregateUDF, Between, BinaryExpr, Cast, Expr, Like, ScalarFunctionArgs, TableSource,
};
// Needed for MakeParquetArray
use datafusion::functions::core::planner::CoreFunctionPlanner;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::ast::escape_quoted_string;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use tracing::log::*;

use super::DeltaParserOptions;
use crate::{DeltaResult, DeltaTableError};

/// This struct is like Datafusion's MakeArray but ensures that `element` is used rather than `item
/// as the field name within the list.
#[derive(Debug)]
struct MakeParquetArray {
    /// The actual upstream UDF, which we're just totally cheating and using
    actual: MakeArray,
    /// Aliases for this UDF
    aliases: Vec<String>,
}

impl MakeParquetArray {
    pub fn new() -> Self {
        let actual = MakeArray::default();
        let aliases = vec!["make_array".into(), "make_list".into()];
        Self { actual, aliases }
    }
}

impl ScalarUDFImpl for MakeParquetArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "make_parquet_array"
    }

    fn signature(&self) -> &Signature {
        self.actual.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let r_type = match arg_types.len() {
            0 => Ok(DataType::List(Arc::new(Field::new(
                "element",
                DataType::Int32,
                true,
            )))),
            _ => {
                // At this point, all the type in array should be coerced to the same one
                Ok(DataType::List(Arc::new(Field::new(
                    "element",
                    arg_types[0].to_owned(),
                    true,
                ))))
            }
        };
        debug!("MakeParquetArray return_type -> {r_type:?}");
        r_type
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut data_type = DataType::Null;
        for arg in &args.args {
            data_type = arg.data_type();
        }

        match self.actual.invoke_with_args(args)? {
            ColumnarValue::Scalar(ScalarValue::List(df_array)) => {
                let field = Arc::new(Field::new("element", data_type, true));
                let result = Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                    GenericListArray::<i32>::try_new(
                        field,
                        df_array.offsets().clone(),
                        arrow_array::make_array(df_array.values().into_data()),
                        None,
                    )?,
                ))));
                debug!("MakeParquetArray;invoke returning: {result:?}");
                result
            }
            others => {
                error!("Unexpected response inside MakeParquetArray! {others:?}");
                Ok(others)
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.actual.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.actual.documentation()
    }
}

/// This exists because the NestedFunctionPlanner, _not_ the UserDefinedFunctionPlanner, handles the
/// insertion of "make_array" which is used to turn [100] into List<field=element, values=[100]>
///
/// **screaming intensifies**
#[derive(Debug)]
struct CustomNestedFunctionPlanner {
    original: NestedFunctionPlanner,
}

impl Default for CustomNestedFunctionPlanner {
    fn default() -> Self {
        Self {
            original: NestedFunctionPlanner,
        }
    }
}

use datafusion::logical_expr::planner::{PlannerResult, RawBinaryExpr};
impl ExprPlanner for CustomNestedFunctionPlanner {
    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        let udf = Arc::new(ScalarUDF::from(MakeParquetArray::new()));

        Ok(PlannerResult::Planned(udf.call(exprs)))
    }
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        self.original.plan_binary_op(expr, schema)
    }
    fn plan_make_map(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        self.original.plan_make_map(args)
    }
    fn plan_any(&self, expr: RawBinaryExpr) -> Result<PlannerResult<RawBinaryExpr>> {
        self.original.plan_any(expr)
    }
}

pub(crate) struct DeltaContextProvider<'a> {
    state: SessionState,
    /// Keeping this around just to make use of the 'a lifetime
    _original: &'a SessionState,
    planners: Vec<Arc<dyn ExprPlanner>>,
}

impl<'a> DeltaContextProvider<'a> {
    fn new(state: &'a SessionState) -> Self {
        // default planners are [CoreFunctionPlanner, NestedFunctionPlanner, FieldAccessPlanner,
        // UserDefinedFunctionPlanner]
        let planners: Vec<Arc<dyn ExprPlanner>> = vec![
            Arc::new(CoreFunctionPlanner::default()),
            Arc::new(CustomNestedFunctionPlanner::default()),
            Arc::new(FieldAccessPlanner),
            Arc::new(datafusion::functions::planner::UserDefinedFunctionPlanner),
        ];
        // Disable the above for testing
        //let planners = state.expr_planners();
        let new_state = SessionStateBuilder::new_from_existing(state.clone())
            .with_expr_planners(planners.clone())
            .build();
        DeltaContextProvider {
            planners,
            state: new_state,
            _original: state,
        }
    }
}

impl ContextProvider for DeltaContextProvider<'_> {
    fn get_table_source(&self, _name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        self.planners.as_slice()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<datafusion::logical_expr::ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, _var: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

/// Parse a string predicate into an `Expr`
pub fn parse_predicate_expression(
    schema: &DFSchema,
    expr: impl AsRef<str>,
    df_state: &SessionState,
) -> DeltaResult<Expr> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, expr.as_ref());
    let tokens = tokenizer
        .tokenize()
        .map_err(|err| DeltaTableError::GenericError {
            source: Box::new(err),
        })?;
    let sql = Parser::new(dialect)
        .with_tokens(tokens)
        .parse_expr()
        .map_err(|err| DeltaTableError::GenericError {
            source: Box::new(err),
        })?;

    let context_provider = DeltaContextProvider::new(df_state);
    let sql_to_rel =
        SqlToRel::new_with_options(&context_provider, DeltaParserOptions::default().into());

    Ok(sql_to_rel.sql_to_expr(sql, schema, &mut Default::default())?)
}

struct SqlFormat<'a> {
    expr: &'a Expr,
}

macro_rules! expr_vec_fmt {
    ( $ARRAY:expr ) => {{
        $ARRAY
            .iter()
            .map(|e| format!("{}", SqlFormat { expr: e }))
            .collect::<Vec<String>>()
            .join(", ")
    }};
}

struct BinaryExprFormat<'a> {
    expr: &'a BinaryExpr,
}

impl Display for BinaryExprFormat<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Put parentheses around child binary expressions so that we can see the difference
        // between `(a OR b) AND c` and `a OR (b AND c)`. We only insert parentheses when needed,
        // based on operator precedence. For example, `(a AND b) OR c` and `a AND b OR c` are
        // equivalent and the parentheses are not necessary.

        fn write_child(f: &mut Formatter<'_>, expr: &Expr, precedence: u8) -> fmt::Result {
            match expr {
                Expr::BinaryExpr(child) => {
                    let p = child.op.precedence();
                    if p == 0 || p < precedence {
                        write!(f, "({})", BinaryExprFormat { expr: child })?;
                    } else {
                        write!(f, "{}", BinaryExprFormat { expr: child })?;
                    }
                }
                _ => write!(f, "{}", SqlFormat { expr })?,
            }
            Ok(())
        }

        let precedence = self.expr.op.precedence();
        write_child(f, self.expr.left.as_ref(), precedence)?;
        write!(f, " {} ", self.expr.op)?;
        write_child(f, self.expr.right.as_ref(), precedence)
    }
}

impl Display for SqlFormat<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.expr {
            Expr::Column(c) => write!(f, "{}", c.quoted_flat_name()),
            Expr::Literal(v, _) => write!(f, "{}", ScalarValueFormat { scalar: v }),
            Expr::Case(case) => {
                write!(f, "CASE ")?;
                if let Some(e) = &case.expr {
                    write!(f, "{} ", SqlFormat { expr: e })?;
                }
                for (w, t) in &case.when_then_expr {
                    write!(
                        f,
                        "WHEN {} THEN {} ",
                        SqlFormat { expr: w },
                        SqlFormat { expr: t }
                    )?;
                }
                if let Some(e) = &case.else_expr {
                    write!(f, "ELSE {} ", SqlFormat { expr: e })?;
                }
                write!(f, "END")
            }
            Expr::Not(expr) => write!(f, "NOT {}", SqlFormat { expr }),
            Expr::Negative(expr) => write!(f, "(- {})", SqlFormat { expr }),
            Expr::IsNull(expr) => write!(f, "{} IS NULL", SqlFormat { expr }),
            Expr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", SqlFormat { expr }),
            Expr::IsTrue(expr) => write!(f, "{} IS TRUE", SqlFormat { expr }),
            Expr::IsFalse(expr) => write!(f, "{} IS FALSE", SqlFormat { expr }),
            Expr::IsUnknown(expr) => write!(f, "{} IS UNKNOWN", SqlFormat { expr }),
            Expr::IsNotTrue(expr) => write!(f, "{} IS NOT TRUE", SqlFormat { expr }),
            Expr::IsNotFalse(expr) => write!(f, "{} IS NOT FALSE", SqlFormat { expr }),
            Expr::IsNotUnknown(expr) => write!(f, "{} IS NOT UNKNOWN", SqlFormat { expr }),
            Expr::BinaryExpr(expr) => write!(f, "{}", BinaryExprFormat { expr }),
            Expr::ScalarFunction(func) => fmt_function(f, func.func.name(), false, &func.args),
            Expr::Cast(Cast { expr, data_type }) => {
                write!(f, "arrow_cast({}, '{data_type}')", SqlFormat { expr })
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(
                        f,
                        "{} NOT BETWEEN {} AND {}",
                        SqlFormat { expr },
                        SqlFormat { expr: low },
                        SqlFormat { expr: high }
                    )
                } else {
                    write!(
                        f,
                        "{} BETWEEN {} AND {}",
                        SqlFormat { expr },
                        SqlFormat { expr: low },
                        SqlFormat { expr: high }
                    )
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                write!(f, "{}", SqlFormat { expr })?;
                let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(
                        f,
                        " {op_name} {} ESCAPE '{char}'",
                        SqlFormat { expr: pattern }
                    )
                } else {
                    write!(f, " {op_name} {}", SqlFormat { expr: pattern })
                }
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            }) => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " SIMILAR TO {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " SIMILAR TO {pattern}")
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT IN ({})", expr_vec_fmt!(list))
                } else {
                    write!(f, "{expr} IN ({})", expr_vec_fmt!(list))
                }
            }
            _ => Err(fmt::Error),
        }
    }
}

/// Format an `Expr` to a parsable SQL expression
pub fn fmt_expr_to_sql(expr: &Expr) -> Result<String, DeltaTableError> {
    let mut s = String::new();
    write!(&mut s, "{}", SqlFormat { expr }).map_err(|_| {
        DeltaTableError::Generic("Unable to convert expression to string".to_owned())
    })?;
    Ok(s)
}

fn fmt_function(f: &mut fmt::Formatter, fun: &str, distinct: bool, args: &[Expr]) -> fmt::Result {
    let args: Vec<String> = args
        .iter()
        .map(|arg| format!("{}", SqlFormat { expr: arg }))
        .collect();

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{fun}({distinct_str}{})", args.join(", "))
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{e}"),
            None => write!($F, "NULL"),
        }
    }};
}

/// Epoch days from ce calander until 1970-01-01
pub const EPOCH_DAYS_FROM_CE: i32 = 719_163;

struct ScalarValueFormat<'a> {
    scalar: &'a ScalarValue,
}

impl fmt::Display for ScalarValueFormat<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.scalar {
            ScalarValue::Boolean(e) => format_option!(f, e)?,
            ScalarValue::Float32(e) => format_option!(f, e)?,
            ScalarValue::Float64(e) => format_option!(f, e)?,
            ScalarValue::Int8(e) => format_option!(f, e)?,
            ScalarValue::Int16(e) => format_option!(f, e)?,
            ScalarValue::Int32(e) => format_option!(f, e)?,
            ScalarValue::Int64(e) => format_option!(f, e)?,
            ScalarValue::UInt8(e) => format_option!(f, e)?,
            ScalarValue::UInt16(e) => format_option!(f, e)?,
            ScalarValue::UInt32(e) => format_option!(f, e)?,
            ScalarValue::UInt64(e) => format_option!(f, e)?,
            ScalarValue::Decimal128(e, precision, scale) => match e {
                Some(e) => write!(f, "'{e}'::decimal({precision}, {scale})",)?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Date32(e) => match e {
                Some(e) => write!(
                    f,
                    "'{}'::date",
                    NaiveDate::from_num_days_from_ce_opt(EPOCH_DAYS_FROM_CE + (*e)).ok_or(Error)?
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Date64(e) => match e {
                Some(e) => write!(
                    f,
                    "'{}'::date",
                    DateTime::from_timestamp_millis(*e)
                        .ok_or(Error)?
                        .date_naive()
                        .format("%Y-%m-%d")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::TimestampMicrosecond(e, tz) => match e {
                Some(e) => match tz {
                    Some(_tz) => write!(
                        f,
                        "arrow_cast('{}', 'Timestamp(Microsecond, Some(\"UTC\"))')",
                        DateTime::from_timestamp_micros(*e)
                            .ok_or(Error)?
                            .format("%Y-%m-%dT%H:%M:%S%.6f")
                    )?,
                    None => write!(
                        f,
                        "arrow_cast('{}', 'Timestamp(Microsecond, None)')",
                        DateTime::from_timestamp_micros(*e)
                            .ok_or(Error)?
                            .format("%Y-%m-%dT%H:%M:%S%.6f")
                    )?,
                },
                None => write!(f, "NULL")?,
            },
            ScalarValue::Utf8(e) | ScalarValue::LargeUtf8(e) | ScalarValue::Utf8View(e) => {
                match e {
                    Some(e) => write!(f, "'{}'", escape_quoted_string(e, '\''))?,
                    None => write!(f, "NULL")?,
                }
            }
            ScalarValue::Binary(e)
            | ScalarValue::FixedSizeBinary(_, e)
            | ScalarValue::LargeBinary(e)
            | ScalarValue::BinaryView(e) => match e {
                Some(l) => write!(
                    f,
                    "decode('{}', 'hex')",
                    l.iter()
                        .map(|v| format!("{v:02x}"))
                        .collect::<Vec<_>>()
                        .join("")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Null => write!(f, "NULL")?,
            _ => return Err(Error),
        };
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use arrow_schema::DataType as ArrowDataType;
    use datafusion::common::{Column, ScalarValue, ToDFSchema};
    use datafusion::functions::core::arrow_cast;
    use datafusion::functions::core::expr_ext::FieldAccessor;
    use datafusion::functions::encoding::expr_fn::decode;
    use datafusion::functions::expr_fn::substring;
    use datafusion::functions_nested::expr_ext::{IndexAccessor, SliceAccessor};
    use datafusion::functions_nested::expr_fn::cardinality;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{col, lit, BinaryExpr, Cast, Expr, ExprSchemable};
    use datafusion::prelude::SessionContext;

    use crate::delta_datafusion::{DataFusionMixins, DeltaSessionContext};
    use crate::kernel::{ArrayType, DataType, PrimitiveType, StructField, StructType};
    use crate::{DeltaOps, DeltaTable};

    use super::fmt_expr_to_sql;

    struct ParseTest {
        expr: Expr,
        expected: String,
        override_expected_expr: Option<Expr>,
    }

    macro_rules! simple {
        ( $EXPR:expr, $STR:expr ) => {{
            ParseTest {
                expr: $EXPR,
                expected: $STR,
                override_expected_expr: None,
            }
        }};
    }

    async fn setup_table() -> DeltaTable {
        let schema = StructType::new(vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "value".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "value2".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "Value3".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "modified".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "active".to_string(),
                DataType::Primitive(PrimitiveType::Boolean),
                true,
            ),
            StructField::new(
                "money".to_string(),
                DataType::Primitive(PrimitiveType::decimal(12, 2).unwrap()),
                true,
            ),
            StructField::new(
                "_date".to_string(),
                DataType::Primitive(PrimitiveType::Date),
                true,
            ),
            StructField::new(
                "_timestamp".to_string(),
                DataType::Primitive(PrimitiveType::Timestamp),
                true,
            ),
            StructField::new(
                "_timestamp_ntz".to_string(),
                DataType::Primitive(PrimitiveType::TimestampNtz),
                true,
            ),
            StructField::new(
                "_binary".to_string(),
                DataType::Primitive(PrimitiveType::Binary),
                true,
            ),
            StructField::new(
                "_decimal".to_string(),
                DataType::Primitive(PrimitiveType::decimal(2, 2).unwrap()),
                true,
            ),
            StructField::new(
                "_struct".to_string(),
                DataType::Struct(Box::new(StructType::new(vec![
                    StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
                    StructField::new(
                        "nested",
                        DataType::Struct(Box::new(StructType::new(vec![StructField::new(
                            "b",
                            DataType::Primitive(PrimitiveType::Integer),
                            true,
                        )]))),
                        true,
                    ),
                ]))),
                true,
            ),
            StructField::new(
                "_list".to_string(),
                DataType::Array(Box::new(ArrayType::new(
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                ))),
                true,
            ),
        ]);

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        table
    }

    #[tokio::test]
    async fn test_expr_sql() {
        let table = setup_table().await;

        // String expression that we output must be parsable for conflict resolution.
        let tests = vec![
            ParseTest {
                expr: Expr::Cast(Cast {
                    expr: Box::new(lit(1_i64)),
                    data_type: ArrowDataType::Int32
                }),
                expected: "arrow_cast(1, 'Int32')".to_string(),
                override_expected_expr: Some(
                    datafusion::logical_expr::Expr::ScalarFunction(
                        ScalarFunction {
                            func: arrow_cast(),
                            args: vec![
                                lit(ScalarValue::Int64(Some(1))),
                                lit(ScalarValue::Utf8(Some("Int32".into())))
                            ]
                        }
                    )
                ),
            },
            simple!(
                Expr::Column(Column::from_qualified_name_ignore_case("Value3")).eq(lit(3_i64)),
                "\"Value3\" = 3".to_string()
            ),
            simple!(col("active").is_true(), "active IS TRUE".to_string()),
            simple!(col("active"), "active".to_string()),
            simple!(col("active").eq(lit(true)), "active = true".to_string()),
            simple!(col("active").is_null(), "active IS NULL".to_string()),
            simple!(
                col("modified").eq(lit("2021-02-03")),
                "modified = '2021-02-03'".to_string()
            ),
            simple!(
                col("modified").eq(lit("'validate ' escapi\\ng'")),
                "modified = '''validate '' escapi\\ng'''".to_string()
            ),
            simple!(col("money").gt(lit(0.10)), "money > 0.1".to_string()),
            ParseTest {
                expr: col("_binary").eq(lit(ScalarValue::Binary(Some(vec![0xAA, 0x00, 0xFF])))),
                expected: "_binary = decode('aa00ff', 'hex')".to_string(),
                override_expected_expr: Some(col("_binary").eq(decode(lit("aa00ff"), lit("hex")))),
            },
            simple!(
                col("value").between(lit(20_i64), lit(30_i64)),
                "value BETWEEN 20 AND 30".to_string()
            ),
            simple!(
                col("value").not_between(lit(20_i64), lit(30_i64)),
                "value NOT BETWEEN 20 AND 30".to_string()
            ),
            simple!(
                col("modified").like(lit("abc%")),
                "modified LIKE 'abc%'".to_string()
            ),
            simple!(
                col("modified").not_like(lit("abc%")),
                "modified NOT LIKE 'abc%'".to_string()
            ),
            simple!(
                (((col("value") * lit(2_i64) + col("value2")) / lit(3_i64)) - col("value"))
                    .gt(lit(0_i64)),
                "(value * 2 + value2) / 3 - value > 0".to_string()
            ),
            simple!(
                col("modified").in_list(vec![lit("a"), lit("c")], false),
                "modified IN ('a', 'c')".to_string()
            ),
            simple!(
                col("modified").in_list(vec![lit("a"), lit("c")], true),
                "modified NOT IN ('a', 'c')".to_string()
            ),
            // Validate order of operations is maintained
            simple!(
                col("modified")
                    .eq(lit("value"))
                    .and(col("value").eq(lit(1_i64)))
                    .or(col("modified")
                        .eq(lit("value2"))
                        .and(col("value").gt(lit(1_i64)))),
                "modified = 'value' AND value = 1 OR modified = 'value2' AND value > 1".to_string()
            ),
            simple!(
                col("modified")
                    .eq(lit("value"))
                    .or(col("value").eq(lit(1_i64)))
                    .and(
                        col("modified")
                            .eq(lit("value2"))
                            .or(col("value").gt(lit(1_i64))),
                    ),
                "(modified = 'value' OR value = 1) AND (modified = 'value2' OR value > 1)"
                    .to_string()
            ),
            // Validate functions are correctly parsed
            simple!(
                substring(col("modified"), lit(0_i64), lit(4_i64)).eq(lit("2021")),
                "substr(modified, 0, 4) = '2021'".to_string()
            ),
            ParseTest {
                expr: col("value").cast_to(
                        &arrow_schema::DataType::Utf8,
                        &table
                            .snapshot()
                            .unwrap()
                            .input_schema()
                            .unwrap()
                            .as_ref()
                            .to_owned()
                            .to_dfschema()
                            .unwrap()
                    )
                    .unwrap()
                    .eq(lit("1")),
                expected: "arrow_cast(value, 'Utf8') = '1'".to_string(),
                override_expected_expr: Some(
                    datafusion::logical_expr::Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(datafusion::logical_expr::Expr::ScalarFunction(
                            ScalarFunction {
                                func: arrow_cast(),
                                args: vec![
                                    col("value"),
                                    lit(ScalarValue::Utf8(Some("Utf8".into())))
                                ]
                            }
                        )),
                        op: datafusion::logical_expr::Operator::Eq,
                        right: Box::new(lit(ScalarValue::Utf8(Some("1".into()))))
                    })
                ),
            },
            simple!(
                col("_struct").field("a").eq(lit(20_i64)),
                "get_field(_struct, 'a') = 20".to_string()
            ),
            simple!(
                col("_struct").field("nested").field("b").eq(lit(20_i64)),
                "get_field(get_field(_struct, 'nested'), 'b') = 20".to_string()
            ),
            simple!(
                col("_list").index(lit(1_i64)).eq(lit(20_i64)),
                "array_element(_list, 1) = 20".to_string()
            ),
            simple!(
                cardinality(col("_list").range(col("value"), lit(10_i64))),
                "cardinality(array_slice(_list, value, 10))".to_string()
            ),
            ParseTest {
                expr: col("_timestamp_ntz").gt(lit(ScalarValue::TimestampMicrosecond(Some(1262304000000000), None))),
                expected: "_timestamp_ntz > arrow_cast('2010-01-01T00:00:00.000000', 'Timestamp(Microsecond, None)')".to_string(),
                override_expected_expr: Some(col("_timestamp_ntz").gt(
                    datafusion::logical_expr::Expr::ScalarFunction(
                        ScalarFunction {
                            func: arrow_cast(),
                            args: vec![
                                lit(ScalarValue::Utf8(Some("2010-01-01T00:00:00.000000".into()))),
                                lit(ScalarValue::Utf8(Some("Timestamp(Microsecond, None)".into())))
                            ]
                        }
                    )
                )),
            },
            ParseTest {
                expr: col("_timestamp").gt(lit(ScalarValue::TimestampMicrosecond(
                    Some(1262304000000000),
                    Some("UTC".into())
                ))),
                expected: "_timestamp > arrow_cast('2010-01-01T00:00:00.000000', 'Timestamp(Microsecond, Some(\"UTC\"))')".to_string(),
                override_expected_expr: Some(col("_timestamp").gt(
                    datafusion::logical_expr::Expr::ScalarFunction(
                        ScalarFunction {
                            func: arrow_cast(),
                            args: vec![
                                lit(ScalarValue::Utf8(Some("2010-01-01T00:00:00.000000".into()))),
                                lit(ScalarValue::Utf8(Some("Timestamp(Microsecond, Some(\"UTC\"))".into())))
                            ]
                        }
                    )
                )),
            },
            ParseTest {
                expr: col("_date").eq(lit(ScalarValue::Date32(Some(18262)))),
                expected: "_date = '2020-01-01'::date".to_string(),
                override_expected_expr: Some(col("_date").eq(
                    Expr::Cast(
                        Cast {
                            expr: Box::from(lit("2020-01-01")),
                            data_type: arrow_schema::DataType::Date32
                        }
                    )
                )),
            },
            ParseTest {
                expr: col("_decimal").eq(lit(ScalarValue::Decimal128(Some(1),2,2))),
                expected: "_decimal = '1'::decimal(2, 2)".to_string(),
                override_expected_expr: Some(col("_decimal").eq(
                    Expr::Cast(
                        Cast {
                            expr: Box::from(lit("1")),
                            data_type: arrow_schema::DataType::Decimal128(2, 2)
                        }
                    )
                )),
            },
        ];

        let session: SessionContext = DeltaSessionContext::default().into();

        for test in tests {
            let actual = fmt_expr_to_sql(&test.expr).unwrap();
            assert_eq!(test.expected, actual);

            let actual_expr = table
                .snapshot()
                .unwrap()
                .parse_predicate_expression(actual, &session.state())
                .unwrap();

            match test.override_expected_expr {
                None => assert_eq!(test.expr, actual_expr),
                Some(expr) => assert_eq!(expr, actual_expr),
            }
        }

        let unsupported_types = vec![
            simple!(
                col("_timestamp").gt(lit(ScalarValue::TimestampMillisecond(Some(100), None))),
                "".to_string()
            ),
            simple!(
                col("_timestamp").gt(lit(ScalarValue::TimestampMillisecond(
                    Some(100),
                    Some("UTC".into())
                ))),
                "".to_string()
            ),
        ];

        for test in unsupported_types {
            assert!(fmt_expr_to_sql(&test.expr).is_err());
        }
    }
}
