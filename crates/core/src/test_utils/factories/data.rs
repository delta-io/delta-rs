use std::collections::HashMap;
use std::sync::Arc;

use arrow_arith::aggregate::{max as arrow_max, max_string, min as arrow_min, min_string};
use arrow_array::*;
use arrow_schema::DataType as ArrowDataType;
use bytes::Bytes;
use delta_kernel::expressions::Scalar;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};

use super::super::TestResult;
use super::FileStats;
use crate::kernel::scalars::ScalarExt;
use crate::kernel::{DataType, PrimitiveType, StructType};

pub struct DataFactory;

impl DataFactory {
    pub fn record_batch(
        schema: &StructType,
        length: usize,
        bounds: &HashMap<&str, (&str, &str)>,
    ) -> TestResult<RecordBatch> {
        generate_random_batch(schema, length, bounds)
    }

    pub fn file_stats(batch: &RecordBatch) -> TestResult<FileStats> {
        get_stats(batch)
    }

    pub fn array(
        data_type: DataType,
        length: usize,
        min_val: Option<String>,
        max_val: Option<String>,
    ) -> TestResult<ArrayRef> {
        generate_random_array(data_type, length, min_val, max_val)
    }
}

fn generate_random_batch(
    schema: &StructType,
    length: usize,
    bounds: &HashMap<&str, (&str, &str)>,
) -> TestResult<RecordBatch> {
    schema
        .fields()
        .map(|field| {
            let (min_val, max_val) =
                if let Some((min_val, max_val)) = bounds.get(field.name().as_str()) {
                    (*min_val, *max_val)
                } else {
                    // NOTE providing illegal strings will resolve to default bounds,
                    // an empty string will resolve to null.
                    ("$%&", "$%&")
                };
            generate_random_array(
                field.data_type().clone(),
                length,
                Some(min_val.to_string()),
                Some(max_val.to_string()),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(|columns| RecordBatch::try_new(Arc::new(schema.try_into().unwrap()), columns).unwrap())
}

pub fn generate_random_array(
    data_type: DataType,
    length: usize,
    min_val: Option<String>,
    max_val: Option<String>,
) -> TestResult<ArrayRef> {
    use DataType::*;
    use PrimitiveType::*;
    let mut rng = rand::thread_rng();

    match data_type {
        Primitive(Integer) => {
            let min_val = min_val
                .and_then(|min| Integer.parse_scalar(&min).ok())
                .unwrap_or(Scalar::Integer(-10));
            let max_val = max_val
                .and_then(|max| Integer.parse_scalar(&max).ok())
                .unwrap_or(Scalar::Integer(10));
            let between = match (min_val, max_val) {
                (Scalar::Integer(min), Scalar::Integer(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Int32Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        Primitive(Long) => {
            let min_val = min_val
                .and_then(|min| Long.parse_scalar(&min).ok())
                .unwrap_or(Scalar::Long(-10));
            let max_val = max_val
                .and_then(|max| Long.parse_scalar(&max).ok())
                .unwrap_or(Scalar::Long(10));
            let between = match (min_val, max_val) {
                (Scalar::Long(min), Scalar::Long(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Int64Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        Primitive(Float) => {
            let min_val = min_val
                .and_then(|min| Float.parse_scalar(&min).ok())
                .unwrap_or(Scalar::Float(-10.1));
            let max_val = max_val
                .and_then(|max| Float.parse_scalar(&max).ok())
                .unwrap_or(Scalar::Float(10.1));
            let between = match (min_val, max_val) {
                (Scalar::Float(min), Scalar::Float(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Float32Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        Primitive(Double) => {
            let min_val = min_val
                .and_then(|min| Double.parse_scalar(&min).ok())
                .unwrap_or(Scalar::Double(-10.1));
            let max_val = max_val
                .and_then(|max| Double.parse_scalar(&max).ok())
                .unwrap_or(Scalar::Double(10.1));
            let between = match (min_val, max_val) {
                (Scalar::Double(min), Scalar::Double(max)) => Uniform::from(min..=max),
                _ => unreachable!(),
            };
            let arr = Float64Array::from(
                (0..length)
                    .map(|_| between.sample(&mut rng))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        Primitive(String) => {
            let arr = StringArray::from(
                (0..length)
                    .map(|_| Alphanumeric.sample_string(&mut rng, 3))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr))
        }
        _ => todo!(),
    }
}

fn get_stats(batch: &RecordBatch) -> TestResult<FileStats> {
    use ArrowDataType::*;

    let mut file_stats = FileStats::new(batch.num_rows() as i64);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(i);
        let stats = match array.data_type() {
            Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                let min = Scalar::Byte(arrow_min(array).unwrap());
                let max = Scalar::Byte(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                let min = Scalar::Short(arrow_min(array).unwrap());
                let max = Scalar::Short(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let min = Scalar::Integer(arrow_min(array).unwrap());
                let max = Scalar::Integer(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let min = Scalar::Long(arrow_min(array).unwrap());
                let max = Scalar::Long(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let min = Scalar::Float(arrow_min(array).unwrap());
                let max = Scalar::Float(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let min = Scalar::Double(arrow_min(array).unwrap());
                let max = Scalar::Double(arrow_max(array).unwrap());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let min = Scalar::String(min_string(array).unwrap().into());
                let max = Scalar::String(max_string(array).unwrap().into());
                let null_count = Scalar::Long(array.null_count() as i64);
                Some((null_count, min, max))
            }
            Struct(_) => None,
            _ => todo!(),
        };
        if let Some((null_count, min, max)) = stats {
            file_stats
                .null_count
                .insert(field.name().to_string(), null_count.to_json());
            file_stats
                .min_values
                .insert(field.name().to_string(), min.to_json());
            file_stats
                .max_values
                .insert(field.name().to_string(), max.to_json());
        }
    }
    Ok(file_stats)
}

pub(crate) fn get_parquet_bytes(batch: &RecordBatch) -> TestResult<Bytes> {
    let mut data: Vec<u8> = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(props))?;
    writer.write(batch)?;
    // writer must be closed to write footer
    writer.close()?;
    Ok(data.into())
}
