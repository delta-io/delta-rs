//! Utility functions to determine early filters for file/partition pruning
use datafusion::functions_aggregate::expr_fn::{max, min};
use std::collections::HashMap;

use datafusion::execution::context::SessionState;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{ScalarValue, TableReference};
use datafusion_expr::expr::{InList, Placeholder};
use datafusion_expr::{Aggregate, BinaryExpr, LogicalPlan, Operator};
use datafusion_expr::{Between, Expr};

use either::{Left, Right};

use itertools::Itertools;

use crate::delta_datafusion::execute_plan_to_batch;
use crate::table::state::DeltaTableState;
use crate::DeltaResult;

#[derive(Debug)]
enum ReferenceTableCheck {
    HasReference(String),
    NoReference,
    Unknown,
}
impl ReferenceTableCheck {
    fn has_reference(&self) -> bool {
        matches!(self, ReferenceTableCheck::HasReference(_))
    }
}

fn references_table(expr: &Expr, table: &TableReference) -> ReferenceTableCheck {
    let res = match expr {
        Expr::Alias(alias) => references_table(&alias.expr, table),
        Expr::Column(col) => col
            .relation
            .as_ref()
            .map(|rel| {
                if rel == table {
                    ReferenceTableCheck::HasReference(col.name.to_owned())
                } else {
                    ReferenceTableCheck::NoReference
                }
            })
            .unwrap_or(ReferenceTableCheck::NoReference),
        Expr::Negative(neg) => references_table(neg, table),
        Expr::Cast(cast) => references_table(&cast.expr, table),
        Expr::TryCast(try_cast) => references_table(&try_cast.expr, table),
        Expr::ScalarFunction(func) => {
            if func.args.len() == 1 {
                references_table(&func.args[0], table)
            } else {
                ReferenceTableCheck::Unknown
            }
        }
        Expr::IsNull(inner) => references_table(inner, table),
        Expr::Literal(_) => ReferenceTableCheck::NoReference,
        _ => ReferenceTableCheck::Unknown,
    };
    res
}

fn construct_placeholder(
    binary: BinaryExpr,
    source_left: bool,
    is_partition_column: bool,
    column_name: String,
    placeholders: &mut Vec<PredicatePlaceholder>,
) -> Option<Expr> {
    if is_partition_column {
        let placeholder_name = format!("{column_name}_{}", placeholders.len());
        let placeholder = Expr::Placeholder(Placeholder {
            id: placeholder_name.clone(),
            data_type: None,
        });

        let (left, right, source_expr): (Box<Expr>, Box<Expr>, Expr) = if source_left {
            (placeholder.into(), binary.clone().right, *binary.left)
        } else {
            (binary.clone().left, placeholder.into(), *binary.right)
        };

        let replaced = Expr::BinaryExpr(BinaryExpr {
            left,
            op: binary.op,
            right,
        });

        placeholders.push(PredicatePlaceholder {
            expr: source_expr,
            alias: placeholder_name,
            is_aggregate: false,
        });

        Some(replaced)
    } else {
        match binary.op {
            Operator::Eq => {
                let name_min = format!("{column_name}_{}_min", placeholders.len());
                let placeholder_min = Expr::Placeholder(Placeholder {
                    id: name_min.clone(),
                    data_type: None,
                });
                let name_max = format!("{column_name}_{}_max", placeholders.len());
                let placeholder_max = Expr::Placeholder(Placeholder {
                    id: name_max.clone(),
                    data_type: None,
                });
                let (source_expr, target_expr) = if source_left {
                    (*binary.left, *binary.right)
                } else {
                    (*binary.right, *binary.left)
                };
                let replaced = Expr::Between(Between {
                    expr: target_expr.into(),
                    negated: false,
                    low: placeholder_min.into(),
                    high: placeholder_max.into(),
                });

                placeholders.push(PredicatePlaceholder {
                    expr: min(source_expr.clone()),
                    alias: name_min,
                    is_aggregate: true,
                });
                placeholders.push(PredicatePlaceholder {
                    expr: max(source_expr),
                    alias: name_max,
                    is_aggregate: true,
                });
                Some(replaced)
            }
            _ => None,
        }
    }
}

fn replace_placeholders(expr: Expr, placeholders: &HashMap<String, ScalarValue>) -> Expr {
    expr.transform(&|expr| match expr {
        Expr::Placeholder(Placeholder { id, .. }) => {
            let value = placeholders[&id].clone();
            // Replace the placeholder with the value
            Ok(Transformed::yes(Expr::Literal(value)))
        }
        _ => Ok(Transformed::no(expr)),
    })
    .unwrap()
    .data
}

pub(crate) struct PredicatePlaceholder {
    pub expr: Expr,
    pub alias: String,
    pub is_aggregate: bool,
}

/// Takes the predicate provided and does three things:
///
/// 1. for any relations between a source column and a partition target column,
///    replace source with a placeholder matching the name of the partition
///    columns
///
/// 2. for any is equal relations between a source column and a non-partition target column,
///    replace source with is between expression with min(source_column) and max(source_column) placeholders
///
/// 3. for any other relation with a source column, remove them.
///
/// For example, for the predicate:
///
/// `source.date = target.date and source.id = target.id and frob > 42`
///
/// where `date` is a partition column, would result in the expr:
///
/// `$date_0 = target.date and target.id between $id_1_min and $id_1_max and frob > 42`
///
/// This leaves us with a predicate that we can push into delta scan after expanding it out to
/// a conjunction between the distinct partitions in the source input.
///
pub(crate) fn generalize_filter(
    predicate: Expr,
    partition_columns: &Vec<String>,
    source_name: &TableReference,
    target_name: &TableReference,
    placeholders: &mut Vec<PredicatePlaceholder>,
) -> Option<Expr> {
    match predicate {
        Expr::BinaryExpr(binary) => {
            if references_table(&binary.right, source_name).has_reference() {
                if let ReferenceTableCheck::HasReference(left_target) =
                    references_table(&binary.left, target_name)
                {
                    return construct_placeholder(
                        binary,
                        false,
                        partition_columns.contains(&left_target),
                        left_target,
                        placeholders,
                    );
                }
                return None;
            }
            if references_table(&binary.left, source_name).has_reference() {
                if let ReferenceTableCheck::HasReference(right_target) =
                    references_table(&binary.right, target_name)
                {
                    return construct_placeholder(
                        binary,
                        true,
                        partition_columns.contains(&right_target),
                        right_target,
                        placeholders,
                    );
                }
                return None;
            }

            let left = generalize_filter(
                *binary.left,
                partition_columns,
                source_name,
                target_name,
                placeholders,
            );
            let right = generalize_filter(
                *binary.right,
                partition_columns,
                source_name,
                target_name,
                placeholders,
            );

            match (left, right) {
                (None, None) => None,
                (None, Some(one_side)) | (Some(one_side), None) => {
                    // in the case of an AND clause, it's safe to generalize the filter down to just one side of the AND.
                    // this is because this filter will be more permissive than the actual predicate, so we know that
                    // we will catch all data that could be matched by the predicate. For OR this is not the case - we
                    // could potentially eliminate one side of the predicate and the filter would only match half the
                    // cases that would have satisfied the match predicate.
                    match binary.op {
                        Operator::And => Some(one_side),
                        Operator::Or => None,
                        _ => None,
                    }
                }
                (Some(l), Some(r)) => Expr::BinaryExpr(BinaryExpr {
                    left: l.into(),
                    op: binary.op,
                    right: r.into(),
                })
                .into(),
            }
        }
        Expr::InList(in_list) => {
            let compare_expr = match generalize_filter(
                *in_list.expr,
                partition_columns,
                source_name,
                target_name,
                placeholders,
            ) {
                Some(expr) => expr,
                None => return None, // Return early
            };

            let mut list_expr = Vec::new();
            for item in in_list.list.into_iter() {
                match item {
                    // If it's a literal just immediately push it in list_expr so we can avoid the unnecessary generalizing
                    Expr::Literal(_) => list_expr.push(item),
                    _ => {
                        if let Some(item) = generalize_filter(
                            item.clone(),
                            partition_columns,
                            source_name,
                            target_name,
                            placeholders,
                        ) {
                            list_expr.push(item)
                        }
                    }
                }
            }
            if !list_expr.is_empty() {
                Expr::InList(InList {
                    expr: compare_expr.into(),
                    list: list_expr,
                    negated: in_list.negated,
                })
                .into()
            } else {
                None
            }
        }
        other => match references_table(&other, source_name) {
            ReferenceTableCheck::HasReference(col) => {
                let placeholder_name = format!("{col}_{}", placeholders.len());

                let placeholder = Expr::Placeholder(Placeholder {
                    id: placeholder_name.clone(),
                    data_type: None,
                });

                placeholders.push(PredicatePlaceholder {
                    expr: other,
                    alias: placeholder_name,
                    is_aggregate: true,
                });
                Some(placeholder)
            }
            ReferenceTableCheck::NoReference => Some(other),
            ReferenceTableCheck::Unknown => None,
        },
    }
}

pub(crate) async fn try_construct_early_filter(
    join_predicate: Expr,
    table_snapshot: &DeltaTableState,
    session_state: &SessionState,
    source: &LogicalPlan,
    source_name: &TableReference,
    target_name: &TableReference,
) -> DeltaResult<Option<Expr>> {
    let table_metadata = table_snapshot.metadata();
    let partition_columns = &table_metadata.partition_columns;

    let mut placeholders = Vec::default();

    match generalize_filter(
        join_predicate,
        partition_columns,
        source_name,
        target_name,
        &mut placeholders,
    ) {
        None => Ok(None),
        Some(filter) => {
            if placeholders.is_empty() {
                // if we haven't recognised any source predicates in the join predicate, return our filter with static only predicates
                Ok(Some(filter))
            } else {
                // if we have some filters, which depend on the source df, then collect the placeholders values from the source data
                // We aggregate the distinct values for partitions with the group_columns and stats(min, max) for dynamic filter as agg_columns
                // Can be translated into `SELECT partition1 as part1_0, min(id) as id_1_min, max(id) as id_1_max FROM source GROUP BY partition1`
                let (agg_columns, group_columns) = placeholders.into_iter().partition_map(|p| {
                    if p.is_aggregate {
                        Left(p.expr.alias(p.alias))
                    } else {
                        Right(p.expr.alias(p.alias))
                    }
                });
                let distinct_partitions = LogicalPlan::Aggregate(Aggregate::try_new(
                    source.clone().into(),
                    group_columns,
                    agg_columns,
                )?);
                let execution_plan = session_state
                    .create_physical_plan(&distinct_partitions)
                    .await?;
                let items = execute_plan_to_batch(session_state, execution_plan).await?;
                let placeholder_names = items
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect_vec();
                let expr = (0..items.num_rows())
                    .map(|i| {
                        let replacements = placeholder_names
                            .iter()
                            .map(|placeholder| {
                                let col = items.column_by_name(placeholder).unwrap();
                                let value = ScalarValue::try_from_array(col, i)?;
                                DeltaResult::Ok((placeholder.to_owned(), value))
                            })
                            .try_collect()?;
                        Ok(replace_placeholders(filter.clone(), &replacements))
                    })
                    .collect::<DeltaResult<Vec<_>>>()?
                    .into_iter()
                    .reduce(Expr::or);
                Ok(expr)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::operations::merge::tests::setup_table;
    use crate::operations::merge::try_construct_early_filter;
    use crate::writer::test_utils::get_arrow_schema;

    use arrow::record_batch::RecordBatch;

    use datafusion::datasource::provider_as_source;

    use datafusion::prelude::*;
    use datafusion_common::Column;
    use datafusion_common::ScalarValue;
    use datafusion_common::TableReference;
    use datafusion_expr::col;

    use datafusion_expr::Expr;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_expr::Operator;

    use std::sync::Arc;

    #[tokio::test]
    async fn test_try_construct_early_filter_with_partitions_expands() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["id"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }));

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let split_pred = {
            fn split(expr: Expr, parts: &mut Vec<(String, String)>) {
                match expr {
                    Expr::BinaryExpr(ex) if ex.op == Operator::Or => {
                        split(*ex.left, parts);
                        split(*ex.right, parts);
                    }
                    Expr::BinaryExpr(ex) if ex.op == Operator::Eq => {
                        let col = match *ex.right {
                            Expr::Column(col) => col.name,
                            ex => panic!("expected column in pred, got {ex}!"),
                        };

                        let value = match *ex.left {
                            Expr::Literal(ScalarValue::Utf8(Some(value))) => value,
                            ex => panic!("expected value in predicate, got {ex}!"),
                        };

                        parts.push((col, value))
                    }

                    expr => panic!("expected either = or OR, got {expr}"),
                }
            }

            let mut parts = vec![];
            split(pred.unwrap(), &mut parts);
            parts.sort();
            parts
        };

        let expected_pred_parts = [
            ("id".to_owned(), "B".to_owned()),
            ("id".to_owned(), "C".to_owned()),
            ("id".to_owned(), "X".to_owned()),
        ];

        assert_eq!(split_pred, expected_pred_parts);
    }

    #[tokio::test]
    async fn test_try_construct_early_filter_with_range() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }));

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let filter = col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        })
        .between(
            Expr::Literal(ScalarValue::Utf8(Some("B".to_string()))),
            Expr::Literal(ScalarValue::Utf8(Some("C".to_string()))),
        );
        assert_eq!(pred.unwrap(), filter);
    }

    #[tokio::test]
    async fn test_try_construct_early_filter_with_partition_and_range() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();
        let source = ctx.read_batch(batch).unwrap();

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }))
        .and(
            col(Column {
                relation: Some(source_name.clone()),
                name: "modified".to_owned(),
            })
            .eq(col(Column {
                relation: Some(target_name.clone()),
                name: "modified".to_owned(),
            })),
        );

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let filter = col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        })
        .between(
            Expr::Literal(ScalarValue::Utf8(Some("B".to_string()))),
            Expr::Literal(ScalarValue::Utf8(Some("C".to_string()))),
        )
        .and(
            Expr::Literal(ScalarValue::Utf8(Some("2023-07-04".to_string()))).eq(col(Column {
                relation: Some(target_name.clone()),
                name: "modified".to_owned(),
            })),
        );
        assert_eq!(pred.unwrap(), filter);
    }

    #[tokio::test]
    async fn test_try_construct_early_filter_with_is_in_literals() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2023-07-04",
                    "2023-07-05",
                    "2023-07-05",
                ])),
            ],
        )
        .unwrap();
        let source_df = ctx.read_batch(batch).unwrap();

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source_plan = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source_df.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }))
        .and(col("modified".to_owned()).in_list(
            vec![lit("2023-07-05"), lit("2023-07-06"), lit("2023-07-07")],
            false,
        ));

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source_plan,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let filter = col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        })
        .between(
            Expr::Literal(ScalarValue::Utf8(Some("A".to_string()))),
            Expr::Literal(ScalarValue::Utf8(Some("C".to_string()))),
        )
        .and(
            col(Column {
                relation: None,
                name: "modified".to_owned(),
            })
            .in_list(
                vec![
                    Expr::Literal(ScalarValue::Utf8(Some("2023-07-05".to_string()))),
                    Expr::Literal(ScalarValue::Utf8(Some("2023-07-06".to_string()))),
                    Expr::Literal(ScalarValue::Utf8(Some("2023-07-07".to_string()))),
                ],
                false,
            ),
        );
        assert_eq!(pred.unwrap(), filter);
    }

    #[tokio::test]
    async fn test_try_construct_early_filter_with_is_in_columns() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2023-07-04",
                    "2023-07-05",
                    "2023-07-05",
                ])),
            ],
        )
        .unwrap();
        let source_df = ctx.read_batch(batch).unwrap();

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source_plan = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source_df.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }))
        .and(col("modified".to_owned()).in_list(
            vec![
                col(Column {
                    relation: Some(target_name.clone()),
                    name: "id".to_owned(),
                }),
                col(Column {
                    relation: Some(target_name.clone()),
                    name: "modified".to_owned(),
                }),
            ],
            false,
        ));

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source_plan,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let filter = col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        })
        .between(
            Expr::Literal(ScalarValue::Utf8(Some("A".to_string()))),
            Expr::Literal(ScalarValue::Utf8(Some("C".to_string()))),
        )
        .and(
            col(Column {
                relation: None,
                name: "modified".to_owned(),
            })
            .in_list(
                vec![
                    col(Column {
                        relation: Some(target_name.clone()),
                        name: "id".to_owned(),
                    }),
                    col(Column {
                        relation: Some(target_name.clone()),
                        name: "modified".to_owned(),
                    }),
                ],
                false,
            ),
        );
        assert_eq!(pred.unwrap(), filter);
    }

    #[tokio::test]
    async fn test_try_construct_early_filter_with_is_in_ident_and_cols() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["modified"])).await;

        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 0);

        let ctx = SessionContext::new();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2023-07-04",
                    "2023-07-05",
                    "2023-07-05",
                ])),
            ],
        )
        .unwrap();
        let source_df = ctx.read_batch(batch).unwrap();

        let source_name = TableReference::parse_str("source");
        let target_name = TableReference::parse_str("target");

        let source_plan = LogicalPlanBuilder::scan(
            source_name.clone(),
            provider_as_source(source_df.into_view()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let join_predicate = col(Column {
            relation: Some(source_name.clone()),
            name: "id".to_owned(),
        })
        .eq(col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        }))
        .and(ident("source.id").in_list(
            vec![
                col(Column {
                    relation: Some(target_name.clone()),
                    name: "id".to_owned(),
                }),
                col(Column {
                    relation: Some(target_name.clone()),
                    name: "modified".to_owned(),
                }),
            ],
            false,
        ));

        let pred = try_construct_early_filter(
            join_predicate,
            table.snapshot().unwrap(),
            &ctx.state(),
            &source_plan,
            &source_name,
            &target_name,
        )
        .await
        .unwrap();

        assert!(pred.is_some());

        let filter = col(Column {
            relation: Some(target_name.clone()),
            name: "id".to_owned(),
        })
        .between(
            Expr::Literal(ScalarValue::Utf8(Some("A".to_string()))),
            Expr::Literal(ScalarValue::Utf8(Some("C".to_string()))),
        )
        .and(ident("source.id").in_list(
            vec![
                col(Column {
                    relation: Some(target_name.clone()),
                    name: "id".to_owned(),
                }),
                col(Column {
                    relation: Some(target_name.clone()),
                    name: "modified".to_owned(),
                }),
            ],
            false,
        ));
        assert_eq!(pred.unwrap(), filter);
    }
}
