use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use delta_kernel::table_configuration::TableConfiguration;

use crate::delta_datafusion::engine::to_predicate;

/// Determine if and how a filter expression can be pushed down
///
/// A predicate may be pushed down into the kernel scan for file level skipping as well as into
/// the inner parquet scan for row group level skipping based on parquet/datafusion.
pub(super) fn can_pushdown_filter(
    filter: &Expr,
    config: &TableConfiguration,
) -> TableProviderFilterPushDown {
    if to_predicate(filter).is_ok() {
        let only_partition_refs = filter
            .column_refs()
            .iter()
            .all(|col| config.metadata().partition_columns().contains(&col.name));
        if only_partition_refs {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        }
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

pub(super) fn can_pushdown_filters(
    filter: &[&Expr],
    config: &TableConfiguration,
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .map(|f| can_pushdown_filter(f, config))
        .collect()
}
