extern crate pyo3;

use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema,
};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::pyarrow::PyArrowType;
use deltalake::schema::{
    Schema, SchemaDataType, SchemaField, SchemaTypeArray, SchemaTypeMap, SchemaTypeStruct,
};
use lazy_static::lazy_static;
use pyo3::exceptions::{PyException, PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
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

/// A primitive datatype, such as a string or number.
///
/// Can be initialized with a string value:
///
/// >>> PrimitiveType("integer")
/// PrimitiveType("integer")
///
/// Valid primitive data types include:
///
///  * "string",
///  * "long",
///  * "integer",
///  * "short",
///  * "byte",
///  * "float",
///  * "double",
///  * "boolean",
///  * "binary",
///  * "date",
///  * "timestamp",
///  * "decimal(<precision>, <scale>)"
///
/// :param data_type: string representation of the data type
#[pyclass(module = "deltalake.schema", text_signature = "(data_type)")]
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
                    "invalid decimal type: {data_type}"
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

    /// The inner type
    ///
    /// :rtype: str
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

    /// Get the JSON string representation of the type.
    ///
    /// :rtype: str
    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        let inner_type = SchemaDataType::primitive(self.inner_type.clone());
        serde_json::to_string(&inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    /// Create a PrimitiveType from a JSON string
    ///
    /// The JSON representation for a primitive type is just a quoted string:
    ///
    /// >>> PrimitiveType.from_json('"integer"')
    /// PrimitiveType("integer")
    ///
    /// :param type_json: A JSON string
    /// :type type_json: str
    /// :rtype: PrimitiveType
    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    /// Get the equivalent PyArrow type.
    ///
    /// :rtype: pyarrow.DataType
    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        let inner_type = SchemaDataType::primitive(self.inner_type.clone());
        Ok(PyArrowType((&inner_type).try_into().map_err(
            |err: ArrowError| PyException::new_err(err.to_string()),
        )?))
    }

    /// Create a PrimitiveType from a PyArrow type
    ///
    /// Will raise ``TypeError`` if the PyArrow type is not a primitive type.
    ///
    /// :param data_type: A PyArrow DataType
    /// :type data_type: pyarrow.DataType
    /// :rtype: PrimitiveType
    #[pyo3(text_signature = "(data_type)")]
    #[staticmethod]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

/// An Array (List) DataType
///
/// Can either pass the element type explicitly or can pass a string
/// if it is a primitive type:
///
/// >>> ArrayType(PrimitiveType("integer"))
/// ArrayType(PrimitiveType("integer"), contains_null=True)
/// >>> ArrayType("integer", contains_null=False)
/// ArrayType(PrimitiveType("integer"), contains_null=False)
#[pyclass(
    module = "deltalake.schema",
    text_signature = "(element_type, contains_null=True)"
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
    #[pyo3(signature = (element_type, contains_null = true))]
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

    /// The string "array"
    ///
    /// :rtype: str
    #[getter]
    fn get_type(&self) -> String {
        "array".to_string()
    }

    /// The type of the element
    ///
    /// :rtype: Union[PrimitiveType, ArrayType, MapType, StructType]
    #[getter]
    fn element_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner_type.get_element_type().to_owned(), py)
    }

    /// Whether the arrays may contain null values
    ///
    /// :rtype: bool
    #[getter]
    fn contains_null(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.inner_type.contains_null().into_py(py))
    }

    /// Get the JSON string representation of the type.
    ///
    /// :rtype: str
    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    /// Create an ArrayType from a JSON string
    ///
    /// The JSON representation for an array type is an object with ``type`` (set to
    /// ``"array"``), ``elementType``, and ``containsNull``:
    ///
    /// >>> ArrayType.from_json("""{
    /// ...   "type": "array",
    /// ...   "elementType": "integer",
    /// ...   "containsNull": false
    /// ... }""")
    /// ArrayType(PrimitiveType("integer"), contains_null=False)
    ///
    /// :param type_json: A JSON string
    /// :type type_json: str
    /// :rtype: ArrayType
    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    /// Get the equivalent PyArrow type.
    ///
    /// :rtype: pyarrow.DataType
    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        Ok(PyArrowType(
            (&SchemaDataType::array(self.inner_type.clone()))
                .try_into()
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        ))
    }

    /// Create an ArrayType from a pyarrow.ListType.
    ///
    /// Will raise ``TypeError`` if a different PyArrow DataType is provided.
    ///
    /// :param data_type: The PyArrow datatype
    /// :type data_type: pyarrow.ListType
    /// :rtype: ArrayType
    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

/// A map data type
///
/// ``key_type`` and ``value_type`` should be :class PrimitiveType:, :class ArrayType:,
/// :class ListType:, or :class StructType:. A string can also be passed, which will be
/// parsed as a primitive type:
///
/// >>> MapType(PrimitiveType("integer"), PrimitiveType("string"))
/// MapType(PrimitiveType("integer"), PrimitiveType("string"), value_contains_null=True)
/// >>> MapType("integer", "string", value_contains_null=False)
/// MapType(PrimitiveType("integer"), PrimitiveType("string"), value_contains_null=False)
#[pyclass(
    module = "deltalake.schema",
    text_signature = "(key_type, value_type, value_contains_null=True)"
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
    #[pyo3(signature = (key_type, value_type, value_contains_null = true))]
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

    /// The string "map"
    ///
    /// :rtype: str
    #[getter]
    fn get_type(&self) -> String {
        "map".to_string()
    }

    /// The type of the keys
    ///
    /// :rtype: Union[PrimitiveType, ArrayType, MapType, StructType]
    #[getter]
    fn key_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner_type.get_key_type().to_owned(), py)
    }

    /// The type of the values
    ///
    /// :rtype: Union[PrimitiveType, ArrayType, MapType, StructType]
    #[getter]
    fn value_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner_type.get_value_type().to_owned(), py)
    }

    /// Whether the values in a map may be null
    ///
    /// :rtype: bool
    #[getter]
    fn value_contains_null(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.inner_type.get_value_contains_null().into_py(py))
    }

    /// Get JSON string representation of map type.
    ///
    /// :rtype: str
    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    /// Create a MapType from a JSON string
    ///
    /// The JSON representation for a map type is an object with ``type`` (set to ``map``),
    /// ``keyType``, ``valueType``, and ``valueContainsNull``:
    ///
    /// >>> MapType.from_json("""{
    /// ...   "type": "map",
    /// ...   "keyType": "integer",
    /// ...   "valueType": "string",
    /// ...   "valueContainsNull": true
    /// ... }""")
    /// MapType(PrimitiveType("integer"), PrimitiveType("string"), value_contains_null=True)
    ///
    /// :param type_json: A JSON string
    /// :type type_json: str
    /// :rtype: MapType
    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    /// Get the equivalent PyArrow data type.
    ///
    /// :rtype: pyarrow.MapType
    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        Ok(PyArrowType(
            (&SchemaDataType::map(self.inner_type.clone()))
                .try_into()
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        ))
    }

    /// Create a MapType from a PyArrow MapType.
    ///
    /// Will raise ``TypeError`` if passed a different type.
    ///
    /// :param data_type: the PyArrow MapType
    /// :type data_type: pyarrow.MapType
    /// :rtype: MapType
    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        inner_type.try_into()
    }
}

/// A field in a Delta StructType or Schema
///
/// Can create with just a name and a type:
///
/// >>> Field("my_int_col", "integer")
/// Field("my_int_col", PrimitiveType("integer"), nullable=True, metadata=None)
///
/// Can also attach metadata to the field. Metadata should be a dictionary with
/// string keys and JSON-serializable values (str, list, int, float, dict):
///
/// >>> Field("my_col", "integer", metadata={"custom_metadata": {"test": 2}})
/// Field("my_col", PrimitiveType("integer"), nullable=True, metadata={"custom_metadata": {"test": 2}})
#[pyclass(
    module = "deltalake.schema",
    text_signature = "(name, ty, nullable=True, metadata=None)"
)]
#[derive(Clone)]
pub struct Field {
    inner: SchemaField,
}

#[pymethods]
impl Field {
    #[new]
    #[pyo3(signature = (name, ty, nullable = true, metadata = None))]
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

    /// The name of the field
    ///
    /// :rtype: str
    #[getter]
    fn name(&self) -> String {
        self.inner.get_name().to_string()
    }

    /// The type of the field
    ///
    /// :rtype: Union[PrimitiveType, ArrayType, MapType, StructType]
    #[getter]
    fn get_type(&self, py: Python) -> PyResult<PyObject> {
        schema_type_to_python(self.inner.get_type().clone(), py)
    }

    /// Whether there may be null values in the field
    ///
    /// :rtype: bool
    #[getter]
    fn nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    /// The metadata of the field
    ///
    /// :rtype: dict
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
            format!(", metadata={metadata_repr}")
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

    /// Get the field as JSON string.
    ///
    /// >>> Field("col", "integer").to_json()
    /// '{"name":"col","type":"integer","nullable":true,"metadata":{}}'
    ///
    /// :rtype: str
    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).map_err(|err| PyException::new_err(err.to_string()))
    }

    /// Create a Field from a JSON string.
    ///
    /// >>> Field.from_json("""{
    /// ...  "name": "col",
    /// ...  "type": "integer",
    /// ...  "nullable": true,
    /// ...  "metadata": {}
    /// ... }""")
    /// Field(col, PrimitiveType("integer"), nullable=True)
    ///
    /// :param field_json: the JSON string.
    /// :type field_json: str
    /// :rtype: Field
    #[staticmethod]
    #[pyo3(text_signature = "(field_json)")]
    fn from_json(field_json: String) -> PyResult<Self> {
        let field: SchemaField = serde_json::from_str(&field_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self { inner: field })
    }

    /// Convert to an equivalent PyArrow field
    ///
    /// Note: This currently doesn't preserve field metadata.
    ///
    /// :rtype: pyarrow.Field
    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowField>> {
        Ok(PyArrowType((&self.inner).try_into().map_err(
            |err: ArrowError| PyException::new_err(err.to_string()),
        )?))
    }

    /// Create a Field from a PyArrow field
    ///
    /// Note: This currently doesn't preserve field metadata.
    ///
    /// :param field: a field
    /// :type: pyarrow.Field
    /// :rtype: Field
    #[staticmethod]
    #[pyo3(text_signature = "(field)")]
    fn from_pyarrow(field: PyArrowType<ArrowField>) -> PyResult<Self> {
        Ok(Self {
            inner: SchemaField::try_from(&field.0)
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        })
    }
}

/// A struct datatype, containing one or more subfields
///
/// Create with a list of :class:`Field`:
///
/// >>> StructType([Field("x", "integer"), Field("y", "string")])
/// StructType([Field(x, PrimitiveType("integer"), nullable=True), Field(y, PrimitiveType("string"), nullable=True)])
#[pyclass(subclass, module = "deltalake.schema", text_signature = "(fields)")]
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

    /// The fields within the struct
    ///
    /// :rtype: List[Field]
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

    /// Get the JSON representation of the type.
    ///
    /// >>> StructType([Field("x", "integer")]).to_json()
    /// '{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}'
    ///
    /// :rtype: str
    #[pyo3(text_signature = "($self)")]
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner_type).map_err(|err| PyException::new_err(err.to_string()))
    }

    /// Create a new StructType from a JSON string.
    ///
    /// >>> StructType.from_json("""{
    /// ...  "type": "struct",
    /// ...  "fields": [{"name": "x", "type": "integer", "nullable": true, "metadata": {}}]
    /// ...  }""")
    /// StructType([Field(x, PrimitiveType("integer"), nullable=True)])
    ///
    /// :param type_json: a JSON string
    /// :type type_json: str
    /// :rtype: StructType
    #[staticmethod]
    #[pyo3(text_signature = "(type_json)")]
    fn from_json(type_json: String) -> PyResult<Self> {
        let data_type: SchemaDataType = serde_json::from_str(&type_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        data_type.try_into()
    }

    /// Get the equivalent PyArrow StructType
    ///
    /// :rtype: pyarrow.StructType
    #[pyo3(text_signature = "($self)")]
    fn to_pyarrow(&self) -> PyResult<PyArrowType<ArrowDataType>> {
        Ok(PyArrowType(
            (&SchemaDataType::r#struct(self.inner_type.clone()))
                .try_into()
                .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?,
        ))
    }

    /// Create a new StructType from a PyArrow struct type.
    ///
    /// Will raise ``TypeError`` if a different data type is provided.
    ///
    /// :param data_type: a PyArrow struct type.
    /// :type data_type: pyarrow.StructType
    /// :rtype: StructType
    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowDataType>) -> PyResult<Self> {
        let inner_type: SchemaDataType = (&data_type.0)
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
#[pyclass(extends = StructType, name = "Schema", module = "deltalake.schema",
text_signature = "(fields)")]
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
                let field = Field {
                    inner: field.clone(),
                };
                field.__repr__(py)
            })
            .collect::<PyResult<_>>()?;
        Ok(format!("Schema([{}])", inner_data.join(", ")))
    }

    /// DEPRECATED: Convert to JSON dictionary representation
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

    /// Return equivalent PyArrow schema
    ///
    /// :param as_large_types: get schema with all variable size types (list,
    ///   binary, string) as large variants (with int64 indices). This is for
    ///   compatibility with systems like Polars that only support the large
    ///   versions of Arrow types.
    ///
    /// :rtype: pyarrow.Schema
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

    /// Create from a PyArrow schema
    ///
    /// :param data_type: a PyArrow schema
    /// :type data_type: pyarrow.Schema
    /// :rtype: Schema
    #[staticmethod]
    #[pyo3(text_signature = "(data_type)")]
    fn from_pyarrow(data_type: PyArrowType<ArrowSchema>, py: Python) -> PyResult<PyObject> {
        let inner_type: SchemaTypeStruct = (&data_type.0)
            .try_into()
            .map_err(|err: ArrowError| PyException::new_err(err.to_string()))?;

        schema_to_pyobject(&inner_type, py)
    }

    /// Get the JSON representation of the schema.
    ///
    /// A schema has the same JSON format as a StructType.
    ///
    /// >>> Schema([Field("x", "integer")]).to_json()
    /// '{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}'
    ///
    /// :rtype: str
    #[pyo3(text_signature = "($self)")]
    fn to_json(self_: PyRef<'_, Self>) -> PyResult<String> {
        let super_ = self_.as_ref();
        super_.to_json()
    }

    /// Create a new Schema from a JSON string.
    ///
    /// A schema has the same JSON format as a StructType.
    ///
    /// >>> Schema.from_json("""{
    /// ...  "type": "struct",
    /// ...  "fields": [{"name": "x", "type": "integer", "nullable": true, "metadata": {}}]
    /// ...  }""")
    /// Schema([Field(x, PrimitiveType("integer"), nullable=True)])
    ///
    /// :param schema_json: a JSON string
    /// :type schema_json: str
    /// :rtype: Schema
    #[staticmethod]
    #[pyo3(text_signature = "(schema_json)")]
    fn from_json(schema_json: String, py: Python) -> PyResult<Py<Self>> {
        let data_type: SchemaDataType = serde_json::from_str(&schema_json)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        if let SchemaDataType::r#struct(inner_type) = data_type {
            Py::new(py, (Self {}, StructType { inner_type }))
        } else {
            Err(PyTypeError::new_err("Type is not a struct"))
        }
    }

    /// The list of invariants on the table.
    ///
    /// :rtype: List[Tuple[str, str]]
    /// :return: a tuple of strings for each invariant. The first string is the
    ///          field path and the second is the SQL of the invariant.
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
