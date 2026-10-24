//! Converts kernel [`Scalar`](delta_kernel::expressions::Scalar) values to DataFusion
//! [`ScalarValue`]s.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::common::ScalarValue;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::utils::SingleRowListArrayBuilder;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{ArrayData, MapData, Scalar, StructData};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Error as KernelError};

/// Converts a Delta kernel Scalar value to a DataFusion ScalarValue.
///
/// Handles type mapping between Delta Lake's scalar types and DataFusion's scalar types,
/// including primitive types, temporal types, nested containers, and null values.
///
/// # Errors
/// Returns an error for interval scalars, which are not yet supported; for a type with no Arrow
/// representation (e.g. a shredded variant); or if building the backing Arrow array for a nested
/// container otherwise fails.
pub fn to_datafusion_scalar(scalar: &Scalar) -> DeltaResult<ScalarValue> {
    Ok(match scalar {
        Scalar::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Scalar::String(value) => ScalarValue::Utf8(Some(value.clone())),
        Scalar::Byte(value) => ScalarValue::Int8(Some(*value)),
        Scalar::Short(value) => ScalarValue::Int16(Some(*value)),
        Scalar::Integer(value) => ScalarValue::Int32(Some(*value)),
        Scalar::Long(value) => ScalarValue::Int64(Some(*value)),
        Scalar::Float(value) => ScalarValue::Float32(Some(*value)),
        Scalar::Double(value) => ScalarValue::Float64(Some(*value)),
        Scalar::Timestamp(value) => {
            ScalarValue::TimestampMicrosecond(Some(*value), Some("UTC".into()))
        }
        #[cfg(feature = "nanosecond-timestamps")]
        Scalar::TimestampNanos(value) => {
            ScalarValue::TimestampNanosecond(Some(*value), Some("UTC".into()))
        }
        #[cfg(feature = "nanosecond-timestamps")]
        Scalar::TimestampNanosNtz(value) => ScalarValue::TimestampNanosecond(Some(*value), None),
        Scalar::TimestampNtz(value) => ScalarValue::TimestampMicrosecond(Some(*value), None),
        Scalar::Date(value) => ScalarValue::Date32(Some(*value)),
        Scalar::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        // scale() is 0..=38, so the i8 cast never truncates.
        Scalar::Decimal(data) => {
            ScalarValue::Decimal128(Some(data.bits()), data.precision(), data.scale() as i8)
        }
        Scalar::Struct(data) => struct_to_df_scalar(data)?,
        Scalar::Array(data) => array_to_df_scalar(data)?,
        Scalar::Map(data) => map_to_df_scalar(data)?,
        Scalar::Null(data_type) => datatype_to_df_null_scalar(data_type)?,
        Scalar::IntervalYearMonth(_) | Scalar::IntervalDayTime(_) => {
            return Err(KernelError::unsupported(
                "interval scalars are not supported in the DataFusion executor",
            ));
        }
    })
}

/// Builds a typed-null `ScalarValue` from a kernel type.
fn datatype_to_df_null_scalar(data_type: &DataType) -> DeltaResult<ScalarValue> {
    let arrow_type: ArrowDataType = data_type.try_into_arrow()?;
    arrow_type.try_into().map_err(KernelError::generic_err)
}

/// Builds a `ScalarValue::List` holding a single list row of the converted elements.
fn array_to_df_scalar(data: &ArrayData) -> DeltaResult<ScalarValue> {
    let elements: DeltaResult<Vec<ScalarValue>> = data
        .array_elements()
        .iter()
        .map(to_datafusion_scalar)
        .collect();
    // Name the list's element field from kernel's own ArrayType->Arrow conversion
    let element_field: ArrowField = data.array_type().try_into_arrow()?;
    let element_array = df_scalars_to_arrow_array(elements?, element_field.data_type())?;
    let list = SingleRowListArrayBuilder::new(element_array)
        .with_field(&element_field)
        .build_list_array();
    Ok(ScalarValue::List(Arc::new(list)))
}

/// Builds a `ScalarValue::Struct` from the struct's fields and converted values.
fn struct_to_df_scalar(data: &StructData) -> DeltaResult<ScalarValue> {
    let mut builder = ScalarStructBuilder::new();
    for (field, value) in data.fields().iter().zip(data.values()) {
        let arrow_field: ArrowField = field.try_into_arrow()?;
        builder = builder.with_scalar(arrow_field, to_datafusion_scalar(value)?);
    }
    builder.build().map_err(KernelError::generic_err)
}

/// Builds a `ScalarValue::Map` holding a single map row of the converted key/value pairs.
fn map_to_df_scalar(data: &MapData) -> DeltaResult<ScalarValue> {
    let map_type = data.map_type();
    let entries_field: ArrowField = map_type.try_into_arrow()?;
    let ArrowDataType::Struct(kv_fields) = entries_field.data_type() else {
        return Err(KernelError::generic("map entries type is not a struct"));
    };
    let [key_field, value_field] = kv_fields.as_ref() else {
        return Err(KernelError::generic(
            "map entries struct must have exactly a key and value field",
        ));
    };

    let pairs = data.pairs();
    let converted: DeltaResult<(Vec<ScalarValue>, Vec<ScalarValue>)> = pairs
        .iter()
        .map(|(key, value)| Ok((to_datafusion_scalar(key)?, to_datafusion_scalar(value)?)))
        .collect();
    let (keys, values) = converted?;
    let key_array = df_scalars_to_arrow_array(keys, key_field.data_type())?;
    let value_array = df_scalars_to_arrow_array(values, value_field.data_type())?;

    let entries = StructArray::try_new(kv_fields.clone(), vec![key_array, value_array], None)
        .map_err(KernelError::generic_err)?;
    let offsets = OffsetBuffer::from_lengths([pairs.len()]);
    let map_array = MapArray::try_new(Arc::new(entries_field), offsets, entries, None, false)
        .map_err(KernelError::generic_err)?;
    Ok(ScalarValue::Map(Arc::new(map_array)))
}

/// Collects converted scalars into a single Arrow column. [`ScalarValue::iter_to_array`] infers
/// the type from the first element, so an empty column falls back to `arrow_type`.
fn df_scalars_to_arrow_array(
    scalars: Vec<ScalarValue>,
    arrow_type: &ArrowDataType,
) -> DeltaResult<ArrayRef> {
    if scalars.is_empty() {
        Ok(new_empty_array(arrow_type))
    } else {
        ScalarValue::iter_to_array(scalars).map_err(KernelError::generic_err)
    }
}

#[cfg(test)]
mod scalar_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, AsArray, Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use datafusion::common::ScalarValue as DFScalarValue;
    use datafusion::common::scalar::ScalarStructBuilder;
    use delta_kernel::expressions::{
        ArrayData as KernelArrayData, MapData as KernelMapData, Scalar as KernelScalar,
        StructData as KernelStructData,
    };
    use delta_kernel::schema::{
        ArrayType, DataType as KernelDataType, MapType, StructField, StructType,
    };
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

    fn assert_rendered(value: &DFScalarValue, expected: &[&str]) {
        let table = pretty_format_columns("c", &[value.to_array().unwrap()])
            .unwrap()
            .to_string();
        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(
            actual, expected,
            "\nexpected:\n{expected:#?}\nactual:\n{actual:#?}"
        );
    }

    fn sample_struct_type() -> StructType {
        StructType::try_new([
            StructField::not_null("a", KernelDataType::INTEGER),
            StructField::nullable("b", KernelDataType::STRING),
        ])
        .unwrap()
    }

    fn sample_struct_scalar() -> KernelScalar {
        KernelScalar::Struct(
            KernelStructData::try_new(
                sample_struct_type().fields().cloned().collect(),
                vec![KernelScalar::Integer(1), KernelScalar::String("x".into())],
            )
            .unwrap(),
        )
    }

    fn sample_map_type() -> MapType {
        MapType::new(KernelDataType::STRING, KernelDataType::INTEGER, false)
    }

    fn sample_map_scalar() -> KernelScalar {
        KernelScalar::Map(
            KernelMapData::try_new(
                sample_map_type(),
                [(KernelScalar::String("k".into()), KernelScalar::Integer(1))],
            )
            .unwrap(),
        )
    }

    fn sample_int_array_scalar() -> KernelScalar {
        KernelScalar::Array(
            KernelArrayData::try_new(
                ArrayType::new(KernelDataType::INTEGER, false),
                [KernelScalar::Integer(1), KernelScalar::Integer(2)],
            )
            .unwrap(),
        )
    }

    // === Directly-converted primitive arms ===

    #[rstest]
    #[case::integer(KernelScalar::Integer(42), DFScalarValue::Int32(Some(42)))]
    #[case::long(
        KernelScalar::Long(9_876_543_210),
        DFScalarValue::Int64(Some(9_876_543_210))
    )]
    #[case::short(KernelScalar::Short(7), DFScalarValue::Int16(Some(7)))]
    #[case::byte(KernelScalar::Byte(3), DFScalarValue::Int8(Some(3)))]
    #[case::float(KernelScalar::Float(1.25), DFScalarValue::Float32(Some(1.25)))]
    #[case::double(KernelScalar::Double(99.99), DFScalarValue::Float64(Some(99.99)))]
    #[case::boolean(KernelScalar::Boolean(true), DFScalarValue::Boolean(Some(true)))]
    #[case::string(KernelScalar::String("hi".into()), DFScalarValue::Utf8(Some("hi".into())))]
    #[case::binary(KernelScalar::Binary(b"abc".to_vec()), DFScalarValue::Binary(Some(b"abc".to_vec())))]
    #[case::date(KernelScalar::Date(20178), DFScalarValue::Date32(Some(20178)))]
    #[case::timestamp(
        KernelScalar::Timestamp(1_000_000),
        DFScalarValue::TimestampMicrosecond(Some(1_000_000), Some("UTC".into()))
    )]
    #[case::timestamp_ntz(
        KernelScalar::TimestampNtz(1_000_000),
        DFScalarValue::TimestampMicrosecond(Some(1_000_000), None)
    )]
    #[case::decimal(
        KernelScalar::decimal(12345, 10, 2).unwrap(),
        DFScalarValue::Decimal128(Some(12345), 10, 2)
    )]
    fn primitive_scalar_converts_to_matching_value(
        #[case] scalar: KernelScalar,
        #[case] expected: DFScalarValue,
    ) {
        assert_eq!(to_datafusion_scalar(&scalar).unwrap(), expected);
    }

    #[test]
    fn nan_and_infinity_are_preserved() {
        match to_datafusion_scalar(&KernelScalar::Double(f64::NAN)).unwrap() {
            DFScalarValue::Float64(Some(v)) => assert!(v.is_nan()),
            other => panic!("expected Float64 NaN, got {other:?}"),
        }
        assert_eq!(
            to_datafusion_scalar(&KernelScalar::Float(f32::INFINITY)).unwrap(),
            DFScalarValue::Float32(Some(f32::INFINITY))
        );
    }

    // === Typed nulls: datatype_to_df_null_scalar ===

    #[rstest]
    #[case::integer(KernelDataType::INTEGER, DFScalarValue::Int32(None))]
    #[case::long(KernelDataType::LONG, DFScalarValue::Int64(None))]
    #[case::string(KernelDataType::STRING, DFScalarValue::Utf8(None))]
    #[case::boolean(KernelDataType::BOOLEAN, DFScalarValue::Boolean(None))]
    #[case::date(KernelDataType::DATE, DFScalarValue::Date32(None))]
    #[case::timestamp(
        KernelDataType::TIMESTAMP,
        DFScalarValue::TimestampMicrosecond(None, Some("UTC".into()))
    )]
    #[case::timestamp_ntz(
        KernelDataType::TIMESTAMP_NTZ,
        DFScalarValue::TimestampMicrosecond(None, None)
    )]
    fn typed_null_scalar_converts_to_typed_null_value(
        #[case] data_type: KernelDataType,
        #[case] expected: DFScalarValue,
    ) {
        assert_eq!(
            to_datafusion_scalar(&KernelScalar::Null(data_type)).unwrap(),
            expected
        );
    }

    #[test]
    fn null_struct_with_non_null_subfields_converts_to_null_struct() {
        let struct_type = StructType::try_new([
            StructField::not_null("a", KernelDataType::INTEGER),
            StructField::not_null("b", KernelDataType::STRING),
        ])
        .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::null(struct_type)).unwrap();
        assert!(matches!(value, DFScalarValue::Struct(_)), "got {value:?}");
        assert!(value.is_null(), "expected a null struct, got {value:?}");
    }

    // Container nulls take DataFusion's nested `try_new_null` paths (distinct from the
    // primitive arms), so assert both the variant and that the value reads back as null.
    #[rstest]
    #[case::array(KernelDataType::from(ArrayType::new(KernelDataType::INTEGER, true)))]
    #[case::map(KernelDataType::from(MapType::new(
        KernelDataType::STRING,
        KernelDataType::INTEGER,
        false
    )))]
    fn null_container_converts_to_typed_null(#[case] data_type: KernelDataType) {
        let value = to_datafusion_scalar(&KernelScalar::Null(data_type)).unwrap();
        assert!(value.is_null(), "expected a null value, got {value:?}");
    }

    #[test]
    fn null_decimal_converts_to_typed_null_decimal() {
        let value =
            to_datafusion_scalar(&KernelScalar::Null(KernelDataType::decimal(10, 2).unwrap()));
        assert_eq!(value.unwrap(), DFScalarValue::Decimal128(None, 10, 2));
    }

    // A shredded (non-unshredded) variant has no Arrow representation in kernel's type
    // conversion, so a typed null of that type surfaces an error.
    #[test]
    fn unrepresentable_type_returns_error() {
        let shredded_variant =
            KernelDataType::variant_type([StructField::not_null("x", KernelDataType::INTEGER)])
                .unwrap();
        to_datafusion_scalar(&KernelScalar::Null(shredded_variant)).unwrap_err();
    }

    // === Arrays: array_to_df_scalar ===

    // The list's element field is named "element" (kernel's LIST_ARRAY_ROOT), not DataFusion's
    // default "item"; the expected value is built to match kernel's ArrayType->Arrow
    // conversion.
    #[test]
    fn array_scalar_converts_to_list_with_matching_elements() {
        let array = KernelArrayData::try_new(
            ArrayType::new(KernelDataType::INTEGER, false),
            [KernelScalar::Integer(1), KernelScalar::Integer(2)],
        )
        .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Array(array)).unwrap();
        let element_field = ArrowField::new("element", ArrowDataType::Int32, false);
        let list = ListArray::new(
            Arc::new(element_field),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        );
        let expected = DFScalarValue::List(Arc::new(list));
        assert_eq!(value, expected);
    }

    #[rstest]
    #[case::array_of_structs(
        ArrayType::new(sample_struct_type(), false),
        vec![sample_struct_scalar()],
        &[
            "+----------------+",
            "| c              |",
            "+----------------+",
            "| [{a: 1, b: x}] |",
            "+----------------+",
        ]
    )]
    #[case::array_of_maps(
        ArrayType::new(sample_map_type(), false),
        vec![sample_map_scalar()],
        &[
            "+----------+",
            "| c        |",
            "+----------+",
            "| [{k: 1}] |",
            "+----------+",
        ]
    )]
    #[case::array_of_arrays(
        ArrayType::new(ArrayType::new(KernelDataType::INTEGER, false), false),
        vec![sample_int_array_scalar()],
        &[
            "+----------+",
            "| c        |",
            "+----------+",
            "| [[1, 2]] |",
            "+----------+",
        ]
    )]
    fn nested_array_converts_to_list(
        #[case] array_type: ArrayType,
        #[case] elements: Vec<KernelScalar>,
        #[case] expected: &[&str],
    ) {
        let data = KernelArrayData::try_new(array_type, elements).unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Array(data)).unwrap();
        assert!(matches!(value, DFScalarValue::List(_)), "got {value:?}");
        assert_rendered(&value, expected);
    }

    // A nullable-element array carrying a null: the element field must stay nullable (so the
    // null round-trips) rather than inheriting the values' non-null inference.
    #[test]
    fn array_with_null_element_keeps_nullable_element_field() {
        let data = KernelArrayData::try_new(
            ArrayType::new(KernelDataType::INTEGER, true),
            [
                KernelScalar::Integer(1),
                KernelScalar::Null(KernelDataType::INTEGER),
            ],
        )
        .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Array(data)).unwrap();
        let DFScalarValue::List(list) = &value else {
            panic!("expected List, got {value:?}");
        };
        let values = list.values();
        assert!(
            values.is_null(1),
            "second element should be null, got {values:?}"
        );
        assert_rendered(
            &value,
            &[
                "+-------+",
                "| c     |",
                "+-------+",
                "| [1, ] |",
                "+-------+",
            ],
        );
    }

    // The empty-column path in `df_scalars_to_arrow_array` (element type from the declared
    // `ArrayType`, not from a first element) must still produce a typed empty list.
    #[test]
    fn empty_array_converts_to_empty_list() {
        let empty: Vec<KernelScalar> = vec![];
        let data = KernelArrayData::try_new(ArrayType::new(KernelDataType::INTEGER, false), empty)
            .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Array(data)).unwrap();
        let DFScalarValue::List(list) = &value else {
            panic!("expected List, got {value:?}");
        };
        assert_eq!(
            list.values().len(),
            0,
            "expected an empty list, got {list:?}"
        );
    }

    // === Structs: struct_to_df_scalar ===

    // Field names and nullability are part of struct equality, so asserting against a
    // hand-built expected value pins them too.
    #[test]
    fn struct_scalar_converts_to_struct_with_matching_fields() {
        let data = KernelStructData::try_new(
            vec![
                StructField::not_null("a", KernelDataType::INTEGER),
                StructField::nullable("b", KernelDataType::STRING),
            ],
            vec![KernelScalar::Integer(1), KernelScalar::String("x".into())],
        )
        .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Struct(data)).unwrap();
        let expected = ScalarStructBuilder::new()
            .with_scalar(
                ArrowField::new("a", ArrowDataType::Int32, false),
                DFScalarValue::Int32(Some(1)),
            )
            .with_scalar(
                ArrowField::new("b", ArrowDataType::Utf8, true),
                DFScalarValue::Utf8(Some("x".into())),
            )
            .build()
            .unwrap();
        assert_eq!(value, expected);
    }

    // A struct field whose value is itself a nested container (struct or array).
    #[rstest]
    #[case::nested_struct(
        StructField::not_null("inner", sample_struct_type()),
        sample_struct_scalar(),
        &[
            "+-----------------------+",
            "| c                     |",
            "+-----------------------+",
            "| {inner: {a: 1, b: x}} |",
            "+-----------------------+",
        ]
    )]
    #[case::array_field(
        StructField::not_null("arr", ArrayType::new(KernelDataType::INTEGER, false)),
        sample_int_array_scalar(),
        &[
            "+---------------+",
            "| c             |",
            "+---------------+",
            "| {arr: [1, 2]} |",
            "+---------------+",
        ]
    )]
    fn struct_with_container_field_converts_to_struct(
        #[case] field: StructField,
        #[case] value: KernelScalar,
        #[case] expected: &[&str],
    ) {
        let data = KernelStructData::try_new(vec![field], vec![value]).unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Struct(data)).unwrap();
        assert!(matches!(value, DFScalarValue::Struct(_)), "got {value:?}");
        assert_rendered(&value, expected);
    }

    // A present (non-null) struct that carries a null in a NULLABLE field.
    #[test]
    fn present_struct_with_null_nullable_subfield_converts() {
        let data = KernelStructData::try_new(
            vec![
                StructField::not_null("a", KernelDataType::INTEGER),
                StructField::nullable("b", KernelDataType::STRING),
            ],
            vec![
                KernelScalar::Integer(1),
                KernelScalar::Null(KernelDataType::STRING),
            ],
        )
        .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Struct(data)).unwrap();
        let DFScalarValue::Struct(array) = &value else {
            panic!("expected Struct, got {value:?}");
        };
        assert!(!value.is_null(), "struct itself is present, got {value:?}");
        // Subfield `b` must be an actual null
        let b = array.column_by_name("b").unwrap();
        assert!(b.is_null(0), "subfield b should be null, got {b:?}");
    }

    // === Maps: map_to_df_scalar ===

    // No symmetric DFScalarValue map constructor exists, so read the entries back directly
    // rather than asserting against a hand-built expected value.
    #[rstest]
    #[case::single(vec![(KernelScalar::String("k".into()), KernelScalar::Integer(1))], vec![("k", 1)])]
    #[case::empty(vec![], vec![])]
    fn map_scalar_converts_to_map_with_matching_pairs(
        #[case] pairs: Vec<(KernelScalar, KernelScalar)>,
        #[case] expected: Vec<(&str, i32)>,
    ) {
        let data = KernelMapData::try_new(
            MapType::new(KernelDataType::STRING, KernelDataType::INTEGER, false),
            pairs,
        )
        .unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Map(data)).unwrap();
        let DFScalarValue::Map(map) = &value else {
            panic!("expected Map, got {value:?}");
        };
        let keys = map.keys().as_string::<i32>();
        let values = map.values().as_primitive::<Int32Type>();
        let actual: Vec<(&str, i32)> = (0..keys.len())
            .map(|i| (keys.value(i), values.value(i)))
            .collect();
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::map_of_structs(
        MapType::new(sample_struct_type(), sample_struct_type(), false),
        vec![(sample_struct_scalar(), sample_struct_scalar())],
        &[
            "+------------------------------+",
            "| c                            |",
            "+------------------------------+",
            "| {{a: 1, b: x}: {a: 1, b: x}} |",
            "+------------------------------+",
        ]
    )]
    #[case::map_of_maps(
        MapType::new(sample_map_type(), sample_map_type(), false),
        vec![(sample_map_scalar(), sample_map_scalar())],
        &[
            "+------------------+",
            "| c                |",
            "+------------------+",
            "| {{k: 1}: {k: 1}} |",
            "+------------------+",
        ]
    )]
    #[case::map_of_arrays(
        MapType::new(
            ArrayType::new(KernelDataType::INTEGER, false),
            ArrayType::new(KernelDataType::INTEGER, false),
            false,
        ),
        vec![(sample_int_array_scalar(), sample_int_array_scalar())],
        &[
            "+------------------+",
            "| c                |",
            "+------------------+",
            "| {[1, 2]: [1, 2]} |",
            "+------------------+",
        ]
    )]
    fn nested_map_converts_to_map(
        #[case] map_type: MapType,
        #[case] pairs: Vec<(KernelScalar, KernelScalar)>,
        #[case] expected: &[&str],
    ) {
        let data = KernelMapData::try_new(map_type, pairs).unwrap();
        let value = to_datafusion_scalar(&KernelScalar::Map(data)).unwrap();
        assert!(matches!(value, DFScalarValue::Map(_)), "got {value:?}");
        assert_rendered(&value, expected);
    }
}
