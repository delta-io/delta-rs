extern crate pyo3;

use crate::pyo3::types::IntoPyDict;
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use deltalake::arrow::error::ArrowError;
use deltalake::schema::{
    Schema, SchemaDataType, SchemaField, SchemaTypeArray, SchemaTypeMap, SchemaTypeStruct,
};
use lazy_static::lazy_static;
use pyo3::exceptions::{PyException, PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::{PyRef, PyResult};
use regex::Regex;
use std::collections::HashMap;

// PyO3 doesn't yet support converting classes with inheritance with Python
// objects within Rust code, which we need here. So for now, we implement
// the types with no inheritance. Later, we may add inheritance.
// See: https://github.com/PyO3/pyo3/issues/1836

// Decimal is separate special case, since it has parameters
const VALID_PRIMITIVE_TYPES: [&str; 11] = [
    "string",
    "long",
    "integer",
    "short",
    "byte",
    "float",
    "double",
    "boolean",
    "binary",
    "date",
    "timestamp",
];

fn try_parse_decimal_type(data_type: &str) -> Option<(usize, usize)> {
    lazy_static! {
        static ref DECIMAL_REGEX: Regex = Regex::new(r"\((\d{1,2}),(\d{1,2})\)").unwrap();
    }
    let extract = DECIMAL_REGEX.captures(data_type)?;
    let precision = extract
        .get(1)
        .and_then(|v| v.as_str().parse::<usize>().ok())?;
    let scale = extract
        .get(2)
        .and_then(|v| v.as_str().parse::<usize>().ok())?;
    Some((precision, scale))
}

fn schema_type_to_python(schema_type: SchemaDataType, py: Python) -> PyResult<PyObject> {
    match schema_type {
        SchemaDataType::primitive(data_type) => Ok((PrimitiveType::new(data_type)?).into_py(py)),
        SchemaDataType::array(array_type) => {
            let array_type: ArrayType = array_type.into();
            Ok(array_type.into_py(py))
        }
        SchemaDataType::map(map_type) => {
            let map_type: MapType = map_type.into();
            Ok(map_type.into_py(py))
        }
        SchemaDataType::r#struct(struct_type) => {
            let struct_type: StructType = struct_type.into();
            Ok(struct_type.into_py(py))
        }
    }
}

fn python_type_to_schema(ob: PyObject, py: Python) -> PyResult<SchemaDataType> {
    if let Ok(data_type) = ob.extract::<PrimitiveType>(py) {
        return Ok(SchemaDataType::primitive(data_type.inner_type));
    }
    if let Ok(array_type) = ob.extract::<ArrayType>(py) {
        return Ok(array_type.into());
    }
    if let Ok(map_type) = ob.extract::<MapType>(py) {
        return Ok(map_type.into());
    }
    if let Ok(struct_type) = ob.extract::<StructType>(py) {
        return Ok(struct_type.into());
    }
    if let Ok(raw_primitive) = ob.extract::<String>(py) {
        // Pass through PrimitiveType::new() to do validation
        return PrimitiveType::new(raw_primitive)
            .map(|data_type| SchemaDataType::primitive(data_type.inner_type));
    }
    Err(PyValueError::new_err("Invalid data type"))
}

#[pyclass(module = "deltalake.schema", text_signature = "(data_type: str)")]
#[derive(Clone)]
pub struct PrimitiveType {
    inner_type: String,
}

impl TryFrom<SchemaDataType> for PrimitiveType {
    type Error = PyErr;
    fn try_from(value: SchemaDataType) -> PyResult<Self> {
        match value {
            SchemaDataType::primitive(type_name) => Self::new(type_name),
            _ => Err(PyTypeError::new_err("Type is not primitive")),
        }
    }
}

#[pymethods]
impl PrimitiveType {
    #[new]
    fn new(data_type: String) -> PyResult<Self> {
        if data_type.starts_with("decimal") {
            if try_parse_decimal_type(&data_type).is_none() {
                Err(PyValueError::new_err(format!(
                    "invalid decimal type: {}",
                    data_type
                )))
            } else {
                Ok(Self {
                    inner_type: data_type,
                })
            }
        } else if !VALID_PRIMITIVE_TYPES
            .iter()
            .any(|&valid| data_type == valid)
        {
            Err(PyValueError::new_err(format!(
                "data_type must be one of decimal(<precision>, <scale>), {}.",
                VALID_PRIMITIVE_TYPES.join(", ")
            )))
        } else {
            Ok(Self {
                inner_type: data_type,
            })
        }
    }

    #[getter]
    fn get_type(&self) -> PyResult<String> {
        Ok(self.inner_type.clone())
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

    fn to_json(&self) -> PyResult<String> {
        let inner_type = SchemaDataType::primitive(self.inner_type.clone());
        serde_json::to_string(&inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    fn to_pyarrow(&self) -> PyResult<ArrowDataType> {
        let inner_type = SchemaDataType::primitive(self.inner_type.clone());
        (&inner_type)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_pyarrow(data_type: ArrowDataType) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

#[pyclass(
    module = "deltalake.schema",
    text_signature = "(element_type: DataType, contains_null: bool)"
)]
#[derive(Clone)]
pub struct ArrayType {
    inner_type: SchemaTypeArray,
}

impl From<SchemaTypeArray> for ArrayType {
    fn from(inner_type: SchemaTypeArray) -> Self {
        Self { inner_type }
    }
}

impl From<ArrayType> for SchemaDataType {
    fn from(arr: ArrayType) -> SchemaDataType {
        SchemaDataType::array(arr.inner_type)
    }
}

impl TryFrom<SchemaDataType> for ArrayType {
    type Error = PyErr;
    fn try_from(value: SchemaDataType) -> PyResult<Self> {
        match value {
            SchemaDataType::array(inner_type) => Ok(Self { inner_type }),
            _ => Err(PyTypeError::new_err("Type is not an array")),
        }
    }
}

#[pymethods]
impl ArrayType {
    #[new]
    #[args(contains_bool = true)]
    fn new(element_type: PyObject, contains_null: bool, py: Python) -> PyResult<Self> {
        let inner_type = SchemaTypeArray::new(
            Box::new(python_type_to_schema(element_type, py)?),
            contains_null,
        );
        Ok(Self { inner_type })
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let type_repr: String =
            schema_type_to_python(self.inner_type.get_element_type().clone(), py)?
                .call_method0(py, "__repr__")?
                .extract(py)?;
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
    fn element_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner_type.get_element_type().to_owned(), py)
    }

    #[getter]
    fn contains_null(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.inner_type.contains_null().into_py(py))
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    fn to_pyarrow(&self) -> PyResult<ArrowDataType> {
        (&SchemaDataType::array(self.inner_type.clone()))
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_pyarrow(data_type: ArrowDataType) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

#[pyclass(
    module = "deltalake.schema",
    text_signature = "(key_type: DataType, value_type: DataType, value_contains_null: bool)"
)]
#[derive(Clone)]
pub struct MapType {
    inner_type: SchemaTypeMap,
}

impl From<SchemaTypeMap> for MapType {
    fn from(inner_type: SchemaTypeMap) -> Self {
        Self { inner_type }
    }
}

impl From<MapType> for SchemaDataType {
    fn from(map: MapType) -> SchemaDataType {
        SchemaDataType::map(map.inner_type)
    }
}

impl TryFrom<SchemaDataType> for MapType {
    type Error = PyErr;
    fn try_from(value: SchemaDataType) -> PyResult<Self> {
        match value {
            SchemaDataType::map(inner_type) => Ok(Self { inner_type }),
            _ => Err(PyTypeError::new_err("Type is not a map")),
        }
    }
}

#[pymethods]
impl MapType {
    #[new]
    #[args(value_contains_null = true)]
    fn new(
        key_type: PyObject,
        value_type: PyObject,
        value_contains_null: bool,
        py: Python,
    ) -> PyResult<Self> {
        let inner_type = SchemaTypeMap::new(
            Box::new(python_type_to_schema(key_type, py)?),
            Box::new(python_type_to_schema(value_type, py)?),
            value_contains_null,
        );
        Ok(Self { inner_type })
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let key_repr: String = schema_type_to_python(self.inner_type.get_key_type().clone(), py)?
            .call_method0(py, "__repr__")?
            .extract(py)?;
        let value_repr: String =
            schema_type_to_python(self.inner_type.get_value_type().clone(), py)?
                .call_method0(py, "__repr__")?
                .extract(py)?;
        Ok(format!(
            "MapType({}, {}, value_contains_null={})",
            key_repr,
            value_repr,
            if self.inner_type.get_value_contains_null() {
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
    fn key_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner_type.get_key_type().to_owned(), py)
    }

    #[getter]
    fn value_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner_type.get_value_type().to_owned(), py)
    }

    #[getter]
    fn value_contains_null(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.inner_type.get_value_contains_null().into_py(py))
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    fn to_pyarrow(&self) -> PyResult<ArrowDataType> {
        (&SchemaDataType::map(self.inner_type.clone()))
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_pyarrow(data_type: ArrowDataType) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

#[pyclass(module = "deltalake.schema")]
#[derive(Clone)]
pub struct Field {
    inner: SchemaField,
}

#[pymethods]
impl Field {
    #[new]
    #[args(nullable = true)]
    fn new(
        name: String,
        ty: PyObject,
        nullable: bool,
        metadata: Option<PyObject>,
        py: Python,
    ) -> PyResult<Self> {
        let ty = python_type_to_schema(ty, py)?;

        // Serialize and de-serialize JSON (it needs to be valid JSON anyways)
        let metadata: HashMap<String, serde_json::Value> = if let Some(ref json) = metadata {
            let json_dumps = PyModule::import(py, "json")?.getattr("dumps")?;
            let metadata_json: String = json_dumps.call1((json,))?.extract()?;
            let metadata_json = Some(metadata_json)
                .filter(|x| x != "null")
                .unwrap_or_else(|| "{}".to_string());
            serde_json::from_str(&metadata_json)
                .map_err(|err| PyValueError::new_err(err.to_string()))?
        } else {
            HashMap::new()
        };

        Ok(Self {
            inner: SchemaField::new(name, ty, nullable, metadata),
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.get_name().to_string()
    }

    #[getter]
    fn get_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner.get_type().clone(), py)
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    #[getter]
    fn metadata(&self, py: Python) -> PyResult<PyObject> {
        let json_loads = PyModule::import(py, "json")?.getattr("loads")?;
        let metadata_json: String = serde_json::to_string(self.inner.get_metadata())
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(json_loads.call1((metadata_json,))?.to_object(py))
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let type_repr: String = schema_type_to_python(self.inner.get_type().clone(), py)?
            .call_method0(py, "__repr__")?
            .extract(py)?;

        let metadata = self.inner.get_metadata();
        let maybe_metadata = if metadata.is_empty() {
            "".to_string()
        } else {
            let metadata_repr: String = self
                .metadata(py)?
                .call_method0(py, "__repr__")?
                .extract(py)?;
            format!(", metadata={}", metadata_repr)
        };
        Ok(format!(
            "Field({}, {}, nullable={}{})",
            self.inner.get_name(),
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

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_json(field_json: String) -> PyResult<Self> {
        let field: SchemaField = serde_json::from_str(&field_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self { inner: field })
    }

    fn to_pyarrow(&self) -> PyResult<ArrowField> {
        (&self.inner)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_pyarrow(field: ArrowField) -> PyResult<Self> {
        Ok(Self {
            inner: SchemaField::try_from(&field)
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        })
    }
}

#[pyclass(
    subclass,
    module = "deltalake.schema",
    text_signature = "(fields: List[Field])"
)]
#[derive(Clone)]
pub struct StructType {
    inner_type: SchemaTypeStruct,
}

impl From<SchemaTypeStruct> for StructType {
    fn from(inner_type: SchemaTypeStruct) -> Self {
        Self { inner_type }
    }
}

impl From<StructType> for SchemaDataType {
    fn from(str: StructType) -> SchemaDataType {
        SchemaDataType::r#struct(str.inner_type)
    }
}

impl TryFrom<SchemaDataType> for StructType {
    type Error = PyErr;
    fn try_from(value: SchemaDataType) -> PyResult<Self> {
        match value {
            SchemaDataType::r#struct(inner_type) => Ok(Self { inner_type }),
            _ => Err(PyTypeError::new_err("Type is not a struct")),
        }
    }
}
#[pymethods]
impl StructType {
    #[new]
    fn new(fields: Vec<PyRef<Field>>) -> Self {
        let fields: Vec<SchemaField> = fields
            .into_iter()
            .map(|field| field.inner.clone())
            .collect();
        let inner_type = SchemaTypeStruct::new(fields);
        Self { inner_type }
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let inner_data: Vec<String> = self
            .inner_type
            .get_fields()
            .iter()
            .map(|field| {
                let field = Field { inner: field.clone() };
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

    #[getter]
    fn get_type(&self) -> String {
        "struct".to_string()
    }

    #[getter]
    fn fields(&self) -> Vec<Field> {
        self.inner_type
            .get_fields()
            .iter()
            .map(|field| Field {
                inner: field.clone(),
            })
            .collect::<Vec<Field>>()
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    fn to_pyarrow(&self) -> PyResult<ArrowDataType> {
        (&SchemaDataType::r#struct(self.inner_type.clone()))
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_pyarrow(data_type: ArrowDataType) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

pub fn schema_to_pyobject(schema: &Schema, py: Python) -> PyResult<PyObject> {
    let fields: Vec<Field> = schema
        .get_fields()
        .iter()
        .map(|field| Field {
            inner: field.clone(),
        })
        .collect();

    let py_schema = PyModule::import(py, "deltalake.schema")?.getattr("Schema")?;

    py_schema
        .call1((fields,))
        .map(|schema| schema.to_object(py))
}

#[pyclass(extends=StructType, name="Schema", module="deltalake.schema")]
pub struct PySchema;

#[pymethods]
impl PySchema {
    #[new]
    fn new(fields: Vec<PyRef<Field>>) -> PyResult<(Self, StructType)> {
        let fields: Vec<SchemaField> = fields
            .into_iter()
            .map(|field| field.inner.clone())
            .collect();
        let inner_type = SchemaTypeStruct::new(fields);
        Ok((Self {}, StructType { inner_type }))
    }

    fn __repr__(self_: PyRef<'_, Self>, py: Python) -> PyResult<String> {
        let super_ = self_.as_ref();
        let inner_data: Vec<String> = super_
            .inner_type
            .get_fields()
            .iter()
            .map(|field| {
                let field = Field { inner: field.clone() };
                field.__repr__(py)
            })
            .collect::<PyResult<_>>()?;
        Ok(format!("Schema([{}])", inner_data.join(", ")))
    }

    fn json(self_: PyRef<'_, Self>, py: Python) -> PyResult<PyObject> {
        let warnings_warn = PyModule::import(py, "warnings")?.getattr("warn")?;
        let deprecation_warning = PyModule::import(py, "builtins")?
            .getattr("DeprecationWarning")?
            .to_object(py);
        let kwargs: [(&str, PyObject); 2] = [
            ("category", deprecation_warning),
            ("stacklevel", 2.to_object(py)),
        ];
        warnings_warn.call(
            ("Schema.json() is deprecated. Use json.loads(Schema.to_json()) instead.",),
            Some(kwargs.into_py_dict(py)),
        )?;

        let super_ = self_.as_ref();
        let json = super_.to_json()?;
        let json_loads = PyModule::import(py, "json")?.getattr("loads")?;
        json_loads
            .call1((json.into_py(py),))
            .map(|obj| obj.to_object(py))
    }
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<ArrowSchema> {
        let super_ = self_.as_ref();
        (&super_.inner_type.clone())
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))
    }

    #[staticmethod]
    fn from_pyarrow(data_type: ArrowSchema, py: Python) -> PyResult<PyObject> {
        let inner_type: SchemaTypeStruct = (&data_type)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        schema_to_pyobject(&inner_type, py)
    }
}
