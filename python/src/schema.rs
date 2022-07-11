extern crate pyo3;

use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use deltalake::arrow::error::ArrowError;
use deltalake::schema::{
    Schema, SchemaDataType, SchemaField, SchemaTypeArray, SchemaTypeMap, SchemaTypeStruct,
};
use lazy_static::lazy_static;
use pyo3::exceptions::{
    PyAssertionError, PyException, PyNotImplementedError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::PyResult;
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
        SchemaDataType::r#struct(_) => todo!(),
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
    if let Ok(raw_primitive) = ob.extract::<String>(py) {
        // Pass through PrimitiveType::new() to do validation
        return PrimitiveType::new(raw_primitive)
            .map(|data_type| SchemaDataType::primitive(data_type.inner_type));
    }
    Err(PyValueError::new_err("Invalid data type"))
}

#[pyclass]
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
        Ok(format!("DataType({})", self.get_type()?))
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

#[pyclass]
#[derive(Clone)]
pub struct ArrayType {
    inner_type: SchemaTypeArray,
}

impl From<SchemaTypeArray> for ArrayType {
    fn from(inner_type: SchemaTypeArray) -> Self {
        Self { inner_type }
    }
}

impl Into<SchemaDataType> for ArrayType {
    fn into(self) -> SchemaDataType {
        SchemaDataType::array(self.inner_type)
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
    fn new(element_type: PyObject, contains_null: bool, py: Python) -> PyResult<Self> {
        let inner_type = SchemaTypeArray::new(
            Box::new(python_type_to_schema(element_type, py)?),
            contains_null,
        );
        Ok(Self { inner_type })
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DataType(array<{:?}, {:?}>)",
            self.inner_type.get_element_type(),
            self.inner_type.contains_null(),
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

#[pyclass]
#[derive(Clone)]
pub struct MapType {
    inner_type: SchemaTypeMap,
}

impl From<SchemaTypeMap> for MapType {
    fn from(inner_type: SchemaTypeMap) -> Self {
        Self { inner_type }
    }
}

impl Into<SchemaDataType> for MapType {
    fn into(self) -> SchemaDataType {
        SchemaDataType::map(self.inner_type)
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

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DataType(map<{:?}, {:?}, {:?}>)",
            self.inner_type.get_key_type(),
            self.inner_type.get_value_type(),
            self.inner_type.get_value_contains_null()
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

#[pyclass]
#[derive(Clone)]
pub struct StructType {
    inner_type: SchemaTypeStruct,
}

impl From<SchemaTypeStruct> for StructType {
    fn from(inner_type: SchemaTypeStruct) -> Self {
        Self { inner_type }
    }
}

impl Into<SchemaDataType> for StructType {
    fn into(self) -> SchemaDataType {
        SchemaDataType::r#struct(self.inner_type)
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

fn field_from_pyobject(obj: PyObject, py: Python) -> PyResult<SchemaField> {
    let name: String = obj.getattr(py, "name")?.extract(py)?;
    let data_type: SchemaDataType = python_type_to_schema(obj.getattr(py, "type")?, py)?;
    let nullable: bool = obj.getattr(py, "nullable")?.extract(py)?;

    // Best we can do is serialize and re-serialize as json
    let json_dumps = PyModule::import(py, "json")?.getattr("dumps")?;
    let metadata_json: String = json_dumps
        .call1((obj.getattr(py, "metadata")?,))?
        .extract()?;
    let metadata: HashMap<String, serde_json::Value> = serde_json::from_str(&metadata_json)
        .map_err(|err| PyValueError::new_err(err.to_string()))?;

    Ok(SchemaField::new(name, data_type, nullable, metadata))
}

fn field_as_pyobject(field: &SchemaField, py: Python) -> PyResult<PyObject> {
    let data_type = schema_type_to_python(field.get_type().clone(), py)?;

    // Best we can do is serialize and re-serialize as json
    let json_loads = PyModule::import(py, "json")?.getattr("loads")?;
    let metadata_json: String = serde_json::to_string(field.get_metadata())
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let metadata = json_loads.call1((metadata_json.into_py(py),))?;

    let args = (field.get_name(), data_type, field.is_nullable(), metadata);
    let py_field = PyModule::import(py, "deltalake.schema")?.getattr("Field")?;
    Ok(py_field.call1(args)?.to_object(py))
}

#[pymethods]
impl StructType {
    #[new]
    fn new(fields: Vec<PyObject>, py: Python) -> PyResult<Self> {
        let fields: Vec<SchemaField> = fields
            .into_iter()
            .map(|obj| field_from_pyobject(obj, py))
            .collect::<PyResult<_>>()?;
        let inner_type = SchemaTypeStruct::new(fields);
        Ok(Self { inner_type })
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let inner_data: Vec<String> = self
            .inner_type
            .get_fields()
            .iter()
            .map(|field| {
                let type_repr: String = schema_type_to_python(field.get_type().clone(), py)?
                    .call_method0(py, "__repr__")?
                    .extract(py)?;
                Ok(format!("{}: {}", field.get_name(), type_repr))
            })
            .collect::<PyResult<_>>()?;
        Ok(format!("DataType(struct<{}>)", inner_data.join(", ")))
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
    fn fields(&self, py: Python) -> PyResult<PyObject> {
        let fields: Vec<PyObject> = self
            .inner_type
            .get_fields()
            .iter()
            .map(|field| field_as_pyobject(field, py))
            .collect::<PyResult<_>>()?;
        Ok(fields.into_py(py))
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
