use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::MapArray;
use arrow_array::*;
use arrow_schema::{Field as ArrowField, Fields};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::DataType;
use delta_kernel::schema::PrimitiveType;

use super::parse::collect_map;
use crate::kernel::arrow::extract::{self as ex};
use crate::kernel::StructType;
use crate::{DeltaResult, DeltaTableError};

pub(crate) fn parse_partitions(
    batch: &RecordBatch,
    partition_schema: &StructType,
    raw_path: &str,
) -> DeltaResult<StructArray> {
    let partitions =
        ex::extract_and_cast_opt::<MapArray>(batch, raw_path).ok_or(DeltaTableError::generic(
            "No partitionValues column found in files batch. This is unexpected.",
        ))?;
    let num_rows = batch.num_rows();

    let mut values = partition_schema
        .fields()
        .map(|f| {
            (
                f.name().to_string(),
                Vec::<Scalar>::with_capacity(partitions.len()),
            )
        })
        .collect::<HashMap<_, _>>();

    for i in 0..partitions.len() {
        if partitions.is_null(i) {
            return Err(DeltaTableError::generic(
                "Expected potentially empty partition values map, but found a null value.",
            ));
        }
        let data: HashMap<_, _> = collect_map(&partitions.value(i))
            .ok_or(DeltaTableError::generic(
                "Failed to collect partition values from map array.",
            ))?
            .map(|(k, v)| {
                let field = partition_schema
                    .fields()
                    .find(|field| field.physical_name().eq(k.as_str()))
                    .ok_or(DeltaTableError::generic(format!(
                        "Partition column {k} not found in schema."
                    )))?;
                let field_type = match field.data_type() {
                    DataType::Primitive(p) => Ok(p),
                    _ => Err(DeltaTableError::generic(
                        "nested partitioning values are not supported",
                    )),
                }?;
                Ok::<_, DeltaTableError>((
                    k,
                    v.map(|vv| field_type.parse_scalar(vv.as_str()))
                        .transpose()?
                        .unwrap_or(Scalar::Null(field.data_type().clone())),
                ))
            })
            .collect::<Result<_, _>>()?;

        partition_schema.fields().for_each(|f| {
            let value = data
                .get(f.name())
                .cloned()
                .unwrap_or(Scalar::Null(f.data_type().clone()));
            values.get_mut(f.name()).unwrap().push(value);
        });
    }

    let columns = partition_schema
        .fields()
        .map(|f| {
            let values = values.get(f.name()).unwrap();
            match f.data_type() {
                DataType::Primitive(p) => {
                    // Safety: we created the Scalars above using the parsing function of the same PrimitiveType
                    // should this fail, it's a bug in our code, and we should panic
                    let arr = match p {
                        PrimitiveType::String => {
                            Arc::new(StringArray::from_iter(values.iter().map(|v| match v {
                                Scalar::String(s) => Some(s.clone()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Long => {
                            Arc::new(Int64Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Long(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Integer => {
                            Arc::new(Int32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Integer(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Short => {
                            Arc::new(Int16Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Short(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Byte => {
                            Arc::new(Int8Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Byte(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Float => {
                            Arc::new(Float32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Float(f) => Some(*f),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Double => {
                            Arc::new(Float64Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Double(f) => Some(*f),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Boolean => {
                            Arc::new(BooleanArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Boolean(b) => Some(*b),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Binary => {
                            Arc::new(BinaryArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Binary(b) => Some(b.clone()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Date => {
                            Arc::new(Date32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Date(d) => Some(*d),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }

                        PrimitiveType::Timestamp => Arc::new(
                            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Timestamp(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_timezone("UTC"),
                        ) as ArrayRef,
                        PrimitiveType::TimestampNtz => Arc::new(
                            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::TimestampNtz(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            })),
                        ) as ArrayRef,
                        PrimitiveType::Decimal(decimal) => Arc::new(
                            Decimal128Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Decimal(decimal) => Some(decimal.bits()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_precision_and_scale(decimal.precision(), decimal.scale() as i8)?,
                        ) as ArrayRef,
                    };
                    Ok(arr)
                }
                _ => Err(DeltaTableError::generic(
                    "complex partitioning values are not supported",
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StructArray::try_new_with_length(
        Fields::from(
            partition_schema
                .fields()
                .map(|f| f.try_into_arrow())
                .collect::<Result<Vec<ArrowField>, _>>()?,
        ),
        columns,
        None,
        num_rows,
    )?)
}
