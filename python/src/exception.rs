use pyo3::create_exception;
use pyo3::exceptions::PyException;
use deltalake::arrow;

create_exception!(deltalake, PyDeltaTableError, PyException);

impl PyDeltaTableError {
    pub fn from_arrow(err: arrow::error::ArrowError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    pub fn from_data_catalog(err: deltalake::DataCatalogError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    pub fn from_raw(err: deltalake::DeltaTableError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    pub fn from_storage(err: deltalake::StorageError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    pub fn from_tokio(err: tokio::io::Error) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    pub fn from_chrono(err: chrono::ParseError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(format!("Parse date and time string failed: {}", err))
    }
}