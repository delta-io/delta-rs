use arrow_schema::ArrowError;
use deltalake::{DeltaTableError, ObjectStoreError};
use pyo3::exceptions::{
    PyException, PyFileNotFoundError, PyIOError, PyNotImplementedError, PyValueError,
};
use pyo3::{create_exception, PyErr};

create_exception!(_internal, DeltaError, PyException);
create_exception!(_internal, TableNotFoundError, DeltaError);
create_exception!(_internal, DeltaProtocolError, DeltaError);
create_exception!(_internal, CommitFailedError, DeltaError);

fn inner_to_py_err(err: DeltaTableError) -> PyErr {
    match err {
        DeltaTableError::NotATable(msg) => TableNotFoundError::new_err(msg),
        DeltaTableError::InvalidTableLocation(msg) => TableNotFoundError::new_err(msg),

        // protocol errors
        DeltaTableError::InvalidJsonLog { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidStatsJson { .. } => DeltaProtocolError::new_err(err.to_string()),

        // commit erros
        DeltaTableError::Transaction { source } => CommitFailedError::new_err(source.to_string()),

        // python exceptions
        DeltaTableError::ObjectStore { source } => object_store_to_py(source),
        DeltaTableError::Io { source } => PyIOError::new_err(source.to_string()),

        DeltaTableError::Arrow { source } => arrow_to_py(source),

        // catach all
        _ => DeltaError::new_err(err.to_string()),
    }
}

fn object_store_to_py(err: ObjectStoreError) -> PyErr {
    match err {
        ObjectStoreError::NotFound { .. } => PyFileNotFoundError::new_err(err.to_string()),
        ObjectStoreError::Generic { source, .. }
            if source.to_string().contains("AWS_S3_ALLOW_UNSAFE_RENAME") =>
        {
            DeltaProtocolError::new_err(source.to_string())
        }
        _ => PyIOError::new_err(err.to_string()),
    }
}

fn arrow_to_py(err: ArrowError) -> PyErr {
    match err {
        ArrowError::IoError(msg) => PyIOError::new_err(msg.to_string()),
        ArrowError::InvalidArgumentError(_) | ArrowError::DivideByZero => {
            PyValueError::new_err("value error")
        }
        ArrowError::NotYetImplemented(msg) => PyNotImplementedError::new_err(msg),
        other => PyException::new_err(other.to_string()),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PythonError {
    #[error("Error in delta table")]
    DeltaTable(#[from] DeltaTableError),
    #[error("Error in object store")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("Error in arrow")]
    Arrow(#[from] ArrowError),
}

impl From<PythonError> for pyo3::PyErr {
    fn from(value: PythonError) -> Self {
        match value {
            PythonError::DeltaTable(err) => inner_to_py_err(err),
            PythonError::ObjectStore(err) => object_store_to_py(err),
            PythonError::Arrow(err) => arrow_to_py(err),
        }
    }
}
