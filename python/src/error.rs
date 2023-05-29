use deltalake::DeltaTableError as InnerDeltaTableError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(_internal, DeltaTableError, PyException);
create_exception!(_internal, DeltaNotATableError, PyException);

fn inner_to_py_err(err: InnerDeltaTableError) -> pyo3::PyErr {
    match err {
        InnerDeltaTableError::NotATable(msg) => DeltaNotATableError::new_err(msg),
        _ => DeltaTableError::new_err(err.to_string()),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PythonError {
    #[error("Error in delta table")]
    DeltaTable(#[from] InnerDeltaTableError),
}

impl From<PythonError> for pyo3::PyErr {
    fn from(value: PythonError) -> Self {
        match value {
            PythonError::DeltaTable(err) => inner_to_py_err(err),
        }
    }
}
