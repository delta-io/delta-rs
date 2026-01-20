use std::sync::Arc;

use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, DataType, Fields, Schema};
use datafusion::{
    common::{Column, Result, exec_datafusion_err},
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::{Expr, cast, lit, try_cast},
    scalar::ScalarValue,
};
use delta_kernel::{engine::arrow_conversion::TryIntoKernel as _, schema::StructType};
use itertools::Itertools as _;

use crate::{
    kernel::{Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, new_metadata},
    merge_arrow_schema,
    operations::write::SchemaMode,
    protocol::SaveMode,
};

/// update the write plan for writes to existing tables.
///
/// This function will update the plan to manage schema drift
pub(super) fn handle_schema_evolution(
    snapshot: &EagerSnapshot,
    source: LogicalPlan,
    save_mode: SaveMode,
    schema_mode: Option<SchemaMode>,
    safe_cast: bool,
    partition_columns: &[String],
) -> Result<(LogicalPlan, Vec<Action>)> {
    let source_schema: Arc<Schema> = source.schema().inner().clone();
    let mut schema_drift = false;

    // Schema merging code should be aware of columns that can be generated during write
    // so they might be empty in the batch, but the will exist in the input_schema()
    // in this case we have to insert the generated column and it's type in the schema of the batch
    let new_schema = {
        let table_schema = snapshot.arrow_schema();
        if let Err(schema_err) = try_cast_schema(source_schema.fields(), table_schema.fields()) {
            schema_drift = true;
            if save_mode == SaveMode::Overwrite && schema_mode == Some(SchemaMode::Overwrite) {
                None // we overwrite anyway, so no need to cast
            } else if schema_mode == Some(SchemaMode::Merge) {
                Some(merge_arrow_schema(
                    table_schema.clone(),
                    source_schema.clone(),
                    schema_drift,
                )?)
            } else {
                return Err(schema_err.into());
            }
        } else if save_mode == SaveMode::Overwrite && schema_mode == Some(SchemaMode::Overwrite) {
            None // we overwrite anyway, so no need to cast
        } else {
            // Schema needs to be merged so that utf8/binary/list types are preserved from the batch side if both table
            // and batch contains such type. Other types are preserved from the table side.
            // At this stage it will never introduce more fields since try_cast_batch passed correctly.
            Some(merge_arrow_schema(
                table_schema.clone(),
                source_schema.clone(),
                schema_drift,
            )?)
        }
    };

    let Some(new_schema) = new_schema else {
        return Ok((source, vec![]));
    };

    let should_update_schema = match schema_mode {
        Some(SchemaMode::Merge) if schema_drift => true,
        Some(SchemaMode::Overwrite) if save_mode == SaveMode::Overwrite => {
            let delta_schema: StructType = source.schema().as_arrow().try_into_kernel()?;
            &delta_schema != snapshot.schema().as_ref()
        }
        _ => false,
    };

    let mut actions = Vec::new();
    if should_update_schema {
        let schema_struct: StructType = source.schema().as_arrow().try_into_kernel()?;
        // Verify if delta schema changed
        if &schema_struct != snapshot.schema().as_ref() {
            let current_protocol = snapshot.protocol();
            let configuration = snapshot.metadata().configuration().clone();
            let new_protocol = current_protocol
                .clone()
                .apply_column_metadata_to_protocol(&schema_struct)
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .move_table_properties_into_features(&configuration);

            let mut metadata = new_metadata(&schema_struct, partition_columns, configuration)
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let existing_metadata_id = snapshot.metadata().id().to_string();

            if !existing_metadata_id.is_empty() {
                metadata = metadata
                    .with_table_id(existing_metadata_id)
                    .map_err(|e| exec_datafusion_err!("{e}"))?;
            }
            let schema_action = Action::Metadata(metadata);
            actions.push(schema_action);
            if current_protocol != &new_protocol {
                actions.push(new_protocol.into())
            }
        }
    };

    // create a new projection casting existing fields to the target data types
    // and imputing missing fields with null values.
    let projection = new_schema
        .fields()
        .iter()
        .map(|f| {
            if source_schema.index_of(f.name()).is_ok() {
                let cast_fn = if safe_cast { try_cast } else { cast };
                cast_fn(
                    Expr::Column(Column::from_name(f.name())),
                    f.data_type().clone(),
                )
                .alias(f.name())
            } else {
                cast(
                    lit(ScalarValue::Null).alias(f.name()),
                    f.data_type().clone(),
                )
                .alias(f.name())
            }
        })
        .collect_vec();

    Ok((
        LogicalPlanBuilder::new(source)
            .project(projection)?
            .build()?,
        actions,
    ))
}

pub(crate) fn try_cast_schema(from_fields: &Fields, to_fields: &Fields) -> Result<(), ArrowError> {
    if from_fields.len() != to_fields.len() {
        return Err(ArrowError::SchemaError(format!(
            "Cannot cast schema, number of fields does not match: {} vs {}",
            from_fields.len(),
            to_fields.len()
        )));
    }

    from_fields
        .iter()
        .map(|f| {
            if let Some((_, target_field)) = to_fields.find(f.name()) {
                if let (DataType::Struct(fields0), DataType::Struct(fields1)) =
                    (f.data_type(), target_field.data_type())
                {
                    try_cast_schema(fields0, fields1)
                } else {
                    match (f.data_type(), target_field.data_type()) {
                        (
                            DataType::Decimal128(left_precision, left_scale) | DataType::Decimal256(left_precision, left_scale),
                            DataType::Decimal128(right_precision, right_scale)
                        ) => {
                            if left_precision <= right_precision && left_scale <= right_scale {
                                Ok(())
                            } else {
                                Err(ArrowError::SchemaError(format!(
                                    "Cannot cast field {} from {} to {}",
                                    f.name(),
                                    f.data_type(),
                                    target_field.data_type()
                                )))
                            }
                        },
                        (
                            _,
                            DataType::Decimal256(_, _),
                        ) => {
                            unreachable!("Target field can never be Decimal 256. According to the protocol: 'The precision and scale can be up to 38.'")
                        },
                        (left, right) => {
                            if !can_cast_types(left, right) {
                                Err(ArrowError::SchemaError(format!(
                                    "Cannot cast field {} from {} to {}",
                                    f.name(),
                                    f.data_type(),
                                    target_field.data_type()
                                )))
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
            } else {
                Err(ArrowError::SchemaError(format!(
                    "Field {} not found in schema",
                    f.name()
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(())
}
