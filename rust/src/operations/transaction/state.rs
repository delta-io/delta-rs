use crate::action::Add;
use crate::delta_datafusion::to_correct_scalar_value;
use crate::schema;
use crate::table_state::DeltaTableState;
use crate::DeltaResult;
use arrow::array::ArrayRef;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::Column;
use datafusion_expr::Expr;
use itertools::Either;
use std::convert::TryFrom;
use std::sync::Arc;

impl DeltaTableState {
    pub fn files_matching_predicate(
        &self,
        filters: &[Expr],
    ) -> DeltaResult<impl Iterator<Item = &Add>> {
        if let Some(Some(predicate)) =
            (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
        {
            let arrow_schema = Arc::new(<ArrowSchema as TryFrom<&schema::Schema>>::try_from(
                self.schema().unwrap(),
            )?);

            let pruning_predicate = PruningPredicate::try_new(predicate, arrow_schema)?;
            let files_to_prune = pruning_predicate.prune(self)?;
            Ok(Either::Left(
                self.files()
                    .iter()
                    .zip(files_to_prune.into_iter())
                    .filter_map(
                        |(action, keep_file)| {
                            if keep_file {
                                Some(action)
                            } else {
                                None
                            }
                        },
                    ),
            ))
        } else {
            Ok(Either::Right(self.files().iter()))
        }
    }
}

pub struct AddContainer<'a> {
    inner: &'a Vec<Add>,
    schema: ArrowSchemaRef,
}

impl<'a> AddContainer<'a> {
    pub fn new(adds: &'a Vec<Add>, schema: ArrowSchemaRef) -> Self {
        Self {
            inner: adds,
            schema,
        }
    }
}

impl<'a> PruningStatistics for AddContainer<'a> {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.schema.field_with_name(&column.name).ok()?.data_type();
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .min_values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                    .unwrap_or(ScalarValue::Null)
            } else {
                ScalarValue::Null
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.schema.field_with_name(&column.name).ok()?.data_type();
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .max_values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                    .unwrap_or(ScalarValue::Null)
            } else {
                ScalarValue::Null
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.inner.len()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .null_count
                    .get(&column.name)
                    .map(|f| ScalarValue::UInt64(f.as_value().map(|val| val as u64)))
                    .unwrap_or(ScalarValue::UInt64(None))
            } else {
                ScalarValue::UInt64(None)
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }
}

impl PruningStatistics for DeltaTableState {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let arrow_schema = Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(self.schema().unwrap()).ok()?,
        );
        let container = AddContainer::new(self.files(), arrow_schema);
        container.min_values(column)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let arrow_schema = Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(self.schema().unwrap()).ok()?,
        );
        let container = AddContainer::new(self.files(), arrow_schema);
        container.max_values(column)
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.files().len()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let arrow_schema = Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(self.schema().unwrap()).ok()?,
        );
        let container = AddContainer::new(self.files(), arrow_schema);
        container.null_counts(column)
    }
}
