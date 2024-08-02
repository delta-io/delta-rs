extern crate pyo3;

use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema,
};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::pyarrow::PyArrowType;
use deltalake::kernel::{
    ArrayType as DeltaArrayType, DataType, MapType as DeltaMapType, MetadataValue,
    PrimitiveType as DeltaPrimitve, StructField, StructType as DeltaStructType, StructTypeExt,
};
use pyo3::exceptions::{PyException, PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;

use crate::utils::warn;

// PyO3 doesn't yet support converting classes with inheritance with Python
// objects within Rust code, which we need here. So for now, we implement
// the types with no inheritance. Later, we may add inheritance.
// See: https://github.com/PyO3/pyo3/issues/1836

// Decimal is separate special case, since it has parameters

fn schema_type_to_python(schema_type: DataType, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    match schema_type {
        DataType::Primitive(data_type) => Ok((PrimitiveType::new(data_type.to_string())?)
            .into_py(py)
            .into_bound(py)),
        DataType::Array(array_type) => {
            let array_type: ArrayType = (*array_type).into();
            Ok(array_type.into_py(py).into_bound(py))
        }
        DataType::Map(map_type) => {
            let map_type: MapType = (*map_type).into();
            Ok(map_type.into_py(py).into_bound(py))
        }
        DataType::Struct(struct_type) => {
            let struct_type: StructType = (*struct_type).into();
            Ok(struct_type.into_py(py).into_bound(py))
        }
    }
}

fn python_type_to_schema(ob: &Bound<'_, PyAny>) -> PyResult<DataType> {
    if let Ok(data_type) = ob.extract::<PrimitiveType>() {
        return Ok(DataType::Primitive(data_type.inner_type));
    }
    if let Ok(array_type) = ob.extract::<ArrayType>() {
        return Ok(array_type.into());
    }
    if let Ok(map_type) = ob.extract::<MapType>() {
        return Ok(map_type.into());
    }
    if let Ok(struct_type) = ob.extract::<StructType>() {
        return Ok(struct_type.into());
    }
    if let Ok(raw_primitive) = ob.extract::<String>() {
        // Pass through PrimitiveType::new() to do validation
        return PrimitiveType::new(raw_primitive)
            .map(|data_type| DataType::Primitive(data_type.inner_type));
    }
    Err(PyValueError::new_err("Invalid data type"))
}

#[pyclass(module = "deltalake._internal")]
#[derive(Clone)]
pub struct PrimitiveType {
    inner_type: DeltaPrimitve,
}

impl TryFrom<DataType> for PrimitiveType {
    type Error = PyErr;
    fn try_from(value: DataType) -> PyResult<Self> {
        match value {
            DataType::Primitive(type_name) => Self::new(type_name.to_string()),
            _ => Err(PyTypeError::new_err("Type is not primitive")),
        }
    }
}

#[pymethods]
impl PrimitiveType {
    #[new]
    #[pyo3(signature = (data_type))]
    fn new(data_type: String) -> PyResult<Self> {
        let data_type: DeltaPrimitve =
            serde_json::from_str(&format!("\"{data_type}\"")).map_err(|_| {
                if data_type.starts_with("decimal") {
                    PyValueError::new_err(format!(
                        "invalid type string: {data_type}, precision/scale can't be larger than 38"
                    ))
                } else {
                    PyValueError::new_err(format!("invalid type string: {data_type}"))
                }
            })?;

        Ok(Self {
            inner_type: data_type,
        })
    }

    #[getter]
    fn get_type(&self) -> PyResult<String> {
        Ok(self.inner_type.to_string())
    }

    fn __richcmp__(&self, other: PrimitiveType, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.inner_type == other.inner_type),
            pyo3::basic::CompareOp::Ne => Ok(self.inner_type != other.inner_type),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PrimitiveType(\"{}\")", &self.inner_type))
    }

    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        let inner_type = DataType::Primitive(self.inner_type.clone());
        serde_json::to_string(&inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: DataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        let inner_type = DataType::Primitive(self.inner_type.clone());
        Ok(PyArrowType((&inner_type).try_into().map_err(
            |err: ArrowError| PyException::new_err(err.to_string()),
        )?))
    }

    #[pyo3(text_signature = "(data_type)")]
    #[staticmethod]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: DataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

#[pyclass(module = "deltalake._internal")]
#[derive(Clone)]
pub struct ArrayType {
    inner_type: DeltaArrayType,
}

impl From<DeltaArrayType> for ArrayType {
    fn from(inner_type: DeltaArrayType) -> Self {
        Self { inner_type }
    }
}

impl From<ArrayType> for DataType {
    fn from(arr: ArrayType) -> DataType {
        DataType::Array(Box::new(arr.inner_type))
    }
}

impl TryFrom<DataType> for ArrayType {
    type Error = PyErr;
    fn try_from(value: DataType) -> PyResult<Self> {
        match value {
            DataType::Array(inner_type) => Ok(Self {
                inner_type: *inner_type,
            }),
            _ => Err(PyTypeError::new_err("Type is not an array")),
        }
    }
}

#[pymethods]
impl ArrayType {
    #[new]
    #[pyo3(signature = (element_type, contains_null = true))]
    fn new(element_type: &Bound<'_, PyAny>, contains_null: bool) -> PyResult<Self> {
        let inner_type = DeltaArrayType::new(python_type_to_schema(element_type)?, contains_null);
        Ok(Self { inner_type })
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let type_repr: String = schema_type_to_python(self.inner_type.element_type().clone(), py)?
            .call_method0("__repr__")?
            .extract()?;
        Ok(format!(
            "ArrayType({}, contains_null={})",
            type_repr,
            if self.inner_type.contains_null() {
                "True"
            } else {
                "False"
            },
        ))
    }

    fn __richcmp__(&self, other: ArrayType, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.inner_type == other.inner_type),
            pyo3::basic::CompareOp::Ne => Ok(self.inner_type != other.inner_type),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }

    #[getter]
    fn get_type(&self) -> String {
        "array".to_string()
    }

    #[getter]
    fn element_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        schema_type_to_python(self.inner_type.element_type().to_owned(), py)
    }

    #[getter]
    fn contains_null<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.inner_type.contains_null().into_py(py).into_bound(py))
    }

    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: DataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        Ok(PyArrowType(
            (&DataType::Array(Box::new(self.inner_type.clone())))
                .try_into()
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        ))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: DataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

#[pyclass(module = "deltalake._internal")]
#[derive(Clone)]
pub struct MapType {
    inner_type: DeltaMapType,
}

impl From<DeltaMapType> for MapType {
    fn from(inner_type: DeltaMapType) -> Self {
        Self { inner_type }
    }
}

impl From<MapType> for DataType {
    fn from(map: MapType) -> DataType {
        DataType::Map(Box::new(map.inner_type))
    }
}

impl TryFrom<DataType> for MapType {
    type Error = PyErr;
    fn try_from(value: DataType) -> PyResult<Self> {
        match value {
            DataType::Map(inner_type) => Ok(Self {
                inner_type: *inner_type,
            }),
            _ => Err(PyTypeError::new_err("Type is not a map")),
        }
    }
}

#[pymethods]
impl MapType {
    #[new]
    #[pyo3(signature = (key_type, value_type, value_contains_null = true))]
    fn new<'py>(
        key_type: &Bound<'py, PyAny>,
        value_type: &Bound<'py, PyAny>,
        value_contains_null: bool,
    ) -> PyResult<Self> {
        let inner_type = DeltaMapType::new(
            python_type_to_schema(key_type)?,
            python_type_to_schema(value_type)?,
            value_contains_null,
        );
        Ok(Self { inner_type })
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let key_repr: String = schema_type_to_python(self.inner_type.key_type().clone(), py)?
            .call_method0("__repr__")?
            .extract()?;
        let value_repr: String = schema_type_to_python(self.inner_type.value_type().clone(), py)?
            .call_method0("__repr__")?
            .extract()?;
        Ok(format!(
            "MapType({}, {}, value_contains_null={})",
            key_repr,
            value_repr,
            if self.inner_type.value_contains_null() {
                "True"
            } else {
                "False"
            }
        ))
    }

    fn __richcmp__(&self, other: MapType, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.inner_type == other.inner_type),
            pyo3::basic::CompareOp::Ne => Ok(self.inner_type != other.inner_type),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }

    #[getter]
    fn get_type(&self) -> String {
        "map".to_string()
    }

    #[getter]
    fn key_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        schema_type_to_python(self.inner_type.key_type().to_owned(), py)
    }

    #[getter]
    fn value_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        schema_type_to_python(self.inner_type.value_type().to_owned(), py)
    }

    #[getter]
    fn value_contains_null<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self
            .inner_type
            .value_contains_null()
            .into_py(py)
            .into_bound(py))
    }

    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: DataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        Ok(PyArrowType(
            (&DataType::Map(Box::new(self.inner_type.clone())))
                .try_into()
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        ))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: DataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

#[pyclass(module = "deltalake._internal")]
#[derive(Clone)]
pub struct Field {
    pub inner: StructField,
}

#[pymethods]
impl Field {
    #[new]
    #[pyo3(signature = (name, r#type, nullable = true, metadata = None))]
    fn new<'py>(
        name: String,
        r#type: &Bound<'py, PyAny>,
        nullable: bool,
        metadata: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Self> {
        let py = r#type.py();
        let ty = python_type_to_schema(r#type)?;

        // Serialize and de-serialize JSON (it needs to be valid JSON anyways)
        let metadata: HashMap<String, serde_json::Value> = if let Some(json) = metadata {
            let json_dumps = PyModule::import_bound(py, "json")?.getattr("dumps")?;
            let metadata_json: String = json_dumps.call1((json,))?.extract()?;
            let metadata_json = Some(metadata_json)
                .filter(|x| x != "null")
                .unwrap_or_else(|| "{}".to_string());
            serde_json::from_str(&metadata_json)
                .map_err(|err| PyValueError::new_err(err.to_string()))?
        } else {
            HashMap::new()
        };

        let mut inner = StructField::new(name, ty, nullable);
        inner = inner.with_metadata(metadata.iter().map(|(k, v)| {
            (
                k,
                if let serde_json::Value::Number(n) = v {
                    n.as_i64().map_or_else(
                        || MetadataValue::String(v.to_string()),
                        |i| {
                            i32::try_from(i)
                                .ok()
                                .map(MetadataValue::Number)
                                .unwrap_or_else(|| MetadataValue::String(v.to_string()))
                        },
                    )
                } else {
                    MetadataValue::String(v.to_string())
                },
            )
        }));

        Ok(Self { inner })
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[getter]
    fn get_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        schema_type_to_python(self.inner.data_type().clone(), py)
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    #[getter]
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let json_loads = PyModule::import_bound(py, "json")?.getattr("loads")?;
        let metadata_json: String = serde_json::to_string(self.inner.metadata())
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(json_loads
            .call1((metadata_json,))?
            .to_object(py)
            .bind(py)
            .to_owned())
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let type_repr: String = schema_type_to_python(self.inner.data_type().clone(), py)?
            .call_method0("__repr__")?
            .extract()?;

        let metadata = self.inner.metadata();
        let maybe_metadata = if metadata.is_empty() {
            "".to_string()
        } else {
            let metadata_repr: String = self.metadata(py)?.call_method0("__repr__")?.extract()?;
            format!(", metadata={metadata_repr}")
        };
        Ok(format!(
            "Field({}, {}, nullable={}{})",
            self.inner.name(),
            type_repr,
            if self.inner.is_nullable() {
                "True"
            } else {
                "False"
            },
            maybe_metadata,
        ))
    }

    fn __richcmp__(&self, other: Field, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.inner == other.inner),
            pyo3::basic::CompareOp::Ne => Ok(self.inner != other.inner),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }

    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(field_json)")]
    fn from_json(field_json: String) -> PyResult<Self> {
        let field: StructField = serde_json::from_str(&field_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self { inner: field })
    }

    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowField>> {
        Ok(PyArrowType((&self.inner).try_into().map_err(
            |err: ArrowError| PyException::new_err(err.to_string()),
        )?))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(field)")]
    fn from_pyarrow(field: PyArrowType<ArrowField>) -> PyResult<Self> {
        Ok(Self {
            inner: StructField::try_from(&field.0)
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        })
    }
}

#[pyclass(subclass, module = "deltalake._internal")]
#[derive(Clone)]
pub struct StructType {
    inner_type: DeltaStructType,
}

impl From<DeltaStructType> for StructType {
    fn from(inner_type: DeltaStructType) -> Self {
        Self { inner_type }
    }
}

impl From<StructType> for DataType {
    fn from(str: StructType) -> DataType {
        DataType::Struct(Box::new(str.inner_type))
    }
}

impl TryFrom<DataType> for StructType {
    type Error = PyErr;
    fn try_from(value: DataType) -> PyResult<Self> {
        match value {
            DataType::Struct(inner_type) => Ok(Self {
                inner_type: *inner_type,
            }),
            _ => Err(PyTypeError::new_err("Type is not a struct")),
        }
    }
}
#[pymethods]
impl StructType {
    #[new]
    fn new(fields: Vec<PyRef<Field>>) -> Self {
        let fields: Vec<StructField> = fields
            .into_iter()
            .map(|field| field.inner.clone())
            .collect();
        let inner_type = DeltaStructType::new(fields);
        Self { inner_type }
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let inner_data: Vec<String> = self
            .inner_type
            .fields()
            .map(|field| {
                let field = Field {
                    inner: field.clone(),
                };
                field.__repr__(py)
            })
            .collect::<PyResult<_>>()?;
        Ok(format!("StructType([{}])", inner_data.join(", ")))
    }

    fn __richcmp__(&self, other: StructType, cmp: pyo3::basic::CompareOp) -> PyResult<bool> {
        match cmp {
            pyo3::basic::CompareOp::Eq => Ok(self.inner_type == other.inner_type),
            pyo3::basic::CompareOp::Ne => Ok(self.inner_type != other.inner_type),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported.",
            )),
        }
    }

    /// The string "struct"
    #[getter]
    fn get_type(&self) -> String {
        "struct".to_string()
    }

    #[getter]
    fn fields(&self) -> Vec<Field> {
        self.inner_type
            .fields()
            .map(|field| Field {
                inner: field.clone(),
            })
            .collect::<Vec<Field>>()
    }

    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: DataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        Ok(PyArrowType(
            (&DataType::Struct(Box::new(self.inner_type.clone())))
                .try_into()
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        ))
    }

    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: DataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

pub fn schema_to_pyobject(schema: DeltaStructType, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let fields = schema.fields().map(|field| Field {
        inner: field.clone(),
    });

    let py_schema = PyModule::import_bound(py, "deltalake.schema")?.getattr("Schema")?;

    py_schema.call1((fields.collect::<Vec<_>>(),))
}

/// A Delta Lake schema
///
/// Create using a list of :class:`Field`:
///
/// >>> Schema([Field("x", "integer"), Field("y", "string")])
/// Schema([Field(x, PrimitiveType("integer"), nullable=True), Field(y, PrimitiveType("string"), nullable=True)])
///
/// Or create from a PyArrow schema:
///
/// >>> import pyarrow as pa
/// >>> Schema.from_pyarrow(pa.schema({"x": pa.int32(), "y": pa.string()}))
/// Schema([Field(x, PrimitiveType("integer"), nullable=True), Field(y, PrimitiveType("string"), nullable=True)])
#[pyclass(extends = StructType, name = "Schema", module = "deltalake._internal")]
pub struct PySchema;

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (fields))]
    fn new(fields: Vec<PyRef<Field>>) -> PyResult<(Self, StructType)> {
        let fields: Vec<StructField> = fields
            .into_iter()
            .map(|field| field.inner.clone())
            .collect();
        let inner_type = DeltaStructType::new(fields);
        Ok((Self {}, StructType { inner_type }))
    }

    fn __repr__(self_: PyRef<'_, Self>, py: Python) -> PyResult<String> {
        let super_ = self_.as_ref();
        let inner_data: Vec<String> = super_
            .inner_type
            .fields()
            .map(|field| {
                let field = Field {
                    inner: field.clone(),
                };
                field.__repr__(py)
            })
            .collect::<PyResult<_>>()?;
        Ok(format!("Schema([{}])", inner_data.join(", ")))
    }

    fn json<'py>(self_: PyRef<'_, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        warn(
            py,
            "DeprecationWarning",
            "Schema.json() is deprecated. Use json.loads(Schema.to_json()) instead.",
            Some(2),
        )?;

        let super_ = self_.as_ref();
        let json = super_.to_json()?;
        let json_loads = PyModule::import_bound(py, "json")?.getattr("loads")?;
        json_loads.call1((json.into_py(py),))
    }

    #[pyo3(signature = (as_large_types = false))]
    fn to_pyarrow(
        self_: PyRef<'_, Self>,
        as_large_types: bool,
    ) -> PyResult<PyArrowType<ArrowSchema>> {
        let super_ = self_.as_ref();
        let res: ArrowSchema = (&super_.inner_type.clone())
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        fn convert_to_large_type(field: ArrowFieldRef, dt: ArrowDataType) -> ArrowFieldRef {
            let field = field.as_ref().clone();
            match dt {
                ArrowDataType::Utf8 => field.with_data_type(ArrowDataType::LargeUtf8).into(),

                ArrowDataType::Binary => field.with_data_type(ArrowDataType::LargeBinary).into(),

                ArrowDataType::List(f) => {
                    let sub_field = convert_to_large_type(f.clone(), f.data_type().clone());
                    field
                        .with_data_type(ArrowDataType::LargeList(sub_field))
                        .into()
                }

                ArrowDataType::FixedSizeList(f, size) => {
                    let sub_field = convert_to_large_type(f.clone(), f.data_type().clone());
                    field
                        .with_data_type(ArrowDataType::FixedSizeList(sub_field, size))
                        .into()
                }

                ArrowDataType::Map(f, sorted) => {
                    let sub_field = convert_to_large_type(f.clone(), f.data_type().clone());
                    field
                        .with_data_type(ArrowDataType::Map(sub_field, sorted))
                        .into()
                }

                ArrowDataType::Struct(fields) => {
                    let sub_fields = fields
                        .iter()
                        .map(|f| {
                            let dt: ArrowDataType = f.data_type().clone();
                            convert_to_large_type(f.clone(), dt)
                        })
                        .collect();

                    field
                        .with_data_type(ArrowDataType::Struct(sub_fields))
                        .into()
                }

                _ => field.into(),
            }
        }

        if as_large_types {
            let schema = ArrowSchema::new(
                res.fields
                    .iter()
                    .map(|f| {
                        let dt: ArrowDataType = f.data_type().clone();
                        convert_to_large_type(f.clone(), dt)
                    })
                    .collect::<Vec<ArrowFieldRef>>(),
            );

            Ok(PyArrowType(schema))
        } else {
            Ok(PyArrowType(res))
        }
    }

    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(
        data_type: PyArrowType<ArrowSchema>,
        py: Python<'_>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner_type: DeltaStructType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        schema_to_pyobject(inner_type, py)
    }

    #[pyo3(text_signature = "($self)")]
    fn to_json(self_: PyRef<'_, Self>) -> PyResult<String> {
        let super_ = self_.as_ref();
        super_.to_json()
    }

    #[staticmethod]
    #[pyo3(text_signature = "(schema_json)")]
    fn from_json(schema_json: String, py: Python) -> PyResult<Py<Self>> {
        let data_type: DataType = serde_json::from_str(&schema_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        if let DataType::Struct(inner_type) = data_type {
            Py::new(
                py,
                (
                    Self {},
                    StructType {
                        inner_type: *inner_type,
                    },
                ),
            )
        } else {
            Err(PyTypeError::new_err("Type is not a struct"))
        }
    }

    #[getter]
    fn invariants(self_: PyRef<'_, Self>) -> PyResult<Vec<(String, String)>> {
        let super_ = self_.as_ref();
        let invariants = super_
            .inner_type
            .get_invariants()
            .map_err(|err| PyException::new_err(err.to_string()))?;
        Ok(invariants
            .into_iter()
            .map(|invariant| (invariant.field_name, invariant.invariant_sql))
            .collect())
    }
}
