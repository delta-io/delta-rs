use arrow_schema::ArrowError;
use deltalake::datafusion::error::DataFusionError;
use deltalake::{errors::DeltaTableError, ObjectStoreError};
use pyo3::exceptions::{
    PyException, PyFileNotFoundError, PyIOError, PyNotImplementedError, PyRuntimeError,
    PyValueError,
};
use pyo3::{create_exception, PyErr};
use std::error::Error;
use std::fmt::Display;

create_exception!(_internal, DeltaError, PyException);
create_exception!(_internal, TableNotFoundError, DeltaError);
create_exception!(_internal, DeltaProtocolError, DeltaError);
create_exception!(_internal, CommitFailedError, DeltaError);
create_exception!(_internal, SchemaMismatchError, DeltaError);

pub(crate) fn to_rt_err(msg: impl ToString) -> PyErr {
    PyRuntimeError::new_err(msg.to_string())
}

fn inner_to_py_err(err: DeltaTableError) -> PyErr {
    match err {
        DeltaTableError::NotATable(msg) => TableNotFoundError::new_err(msg),
        DeltaTableError::InvalidTableLocation(msg) => TableNotFoundError::new_err(msg),

        // protocol errors
        DeltaTableError::InvalidJsonLog { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidStatsJson { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidData { violations } => {
            DeltaProtocolError::new_err(format!("Invariant violations: {violations:?}"))
        }

        // commit errors
        DeltaTableError::Transaction { source } => CommitFailedError::new_err(source.to_string()),

        // python exceptions
        DeltaTableError::ObjectStore { source } => object_store_to_py(source, None),
        DeltaTableError::Kernel { source } => match source {
            deltalake::kernel::Error::ObjectStore(e) => object_store_to_py(e, Some("Kernel error")),
            other => DeltaError::new_err(DeltaTableError::Kernel { source: other }.to_string()),
        },
        // delta-kernel-rs error propagation
        DeltaTableError::KernelError(source) => match source {
            delta_kernel::Error::ObjectStore(e) => object_store_to_py(e, Some("Kernel error")),
            other => DeltaError::new_err(DeltaTableError::KernelError(other).to_string()),
        },
        DeltaTableError::Io { source } => PyIOError::new_err(source.to_string()),

        DeltaTableError::Arrow { source } => arrow_to_py(source),

        // catch all
        _ => DeltaError::new_err(err.to_string()),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DisplaySourceChain<T> {
    err: T,
    error_name: String,
}

impl<T: Error + 'static> Display for DisplaySourceChain<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // walk the source chain and collect error messages
        let mut err_msgs = Vec::new();
        let mut current_err = Some(&self.err as &(dyn Error + 'static));
        while let Some(err) = current_err {
            let err_msg = err.to_string();
            err_msgs.push(err_msg);
            current_err = err.source();
        }
        // produce output message parts from source error messages
        // message parts are delimited by the substring ": "
        let mut out_parts = Vec::with_capacity(err_msgs.capacity());
        for err_msg in &err_msgs {
            // not very clean but std lib doesn't easily support splitting on two substrings
            for err_part in err_msg.split(": ").flat_map(|s| s.split("\ncaused by\n")) {
                if !err_part.is_empty()
                    && !out_parts.contains(&err_part)
                    && !out_parts.iter().any(|p| p.contains(err_part))
                {
                    out_parts.push(err_part);
                }
            }
        }
        for (i, part) in out_parts.iter().enumerate() {
            if i == 0 {
                writeln!(f, "{part}")?;
            } else {
                writeln!(
                    f,
                    "{}\x1b[31m↳\x1b[0m {}",
                    " ".repeat(self.error_name.len() + ": ".len() + i),
                    part
                )?;
            }
        }
        Ok(())
    }
}

fn object_store_to_py(err: ObjectStoreError, source_error_name: Option<&str>) -> PyErr {
    match err {
        ObjectStoreError::NotFound { .. } => {
            let mut error = DisplaySourceChain {
                err,
                error_name: "FileNotFoundError".to_string(),
            }
            .to_string();
            if let Some(source_error_name) = source_error_name {
                error = format!("{} -> {}", source_error_name, error)
            }
            PyFileNotFoundError::new_err(error)
        }

        ObjectStoreError::Generic { source, .. }
            if source.to_string().contains("AWS_S3_ALLOW_UNSAFE_RENAME") =>
        {
            DeltaProtocolError::new_err(source.to_string())
        }
        _ => {
            let mut error = DisplaySourceChain {
                err,
                error_name: "IOError".to_string(),
            }
            .to_string();
            if let Some(source_error_name) = source_error_name {
                error = format!("{} -> {}", source_error_name, error)
            }
            PyIOError::new_err(error)
        }
    }
}

fn arrow_to_py(err: ArrowError) -> PyErr {
    match err {
        ArrowError::IoError(msg, _) => PyIOError::new_err(msg),
        ArrowError::DivideByZero => PyValueError::new_err("division by zero"),
        ArrowError::InvalidArgumentError(msg) => PyValueError::new_err(msg),
        ArrowError::NotYetImplemented(msg) => PyNotImplementedError::new_err(msg),
        ArrowError::SchemaError(msg) => SchemaMismatchError::new_err(msg),
        other => PyException::new_err(other.to_string()),
    }
}

fn datafusion_to_py(err: DataFusionError) -> PyErr {
    DeltaError::new_err(err.to_string())
}

#[derive(thiserror::Error, Debug)]
pub enum PythonError {
    #[error("Error in delta table")]
    DeltaTable(#[from] DeltaTableError),
    #[error("Error in object store")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("Error in arrow")]
    Arrow(#[from] ArrowError),
    #[error("Error in data fusion")]
    DataFusion(#[from] DataFusionError),
    #[error("Lock acquisition error")]
    ThreadingError(String),
}

impl<T> From<std::sync::PoisonError<T>> for PythonError {
    fn from(val: std::sync::PoisonError<T>) -> Self {
        PythonError::ThreadingError(val.to_string())
    }
}

impl From<PythonError> for pyo3::PyErr {
    fn from(value: PythonError) -> Self {
        match value {
            PythonError::DeltaTable(err) => inner_to_py_err(err),
            PythonError::ObjectStore(err) => object_store_to_py(err, None),
            PythonError::Arrow(err) => arrow_to_py(err),
            PythonError::DataFusion(err) => datafusion_to_py(err),
            PythonError::ThreadingError(err) => PyRuntimeError::new_err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DisplaySourceChain;
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct CustomError {
        msg: &'static str,
        source: Option<Box<dyn Error + 'static>>,
    }

    impl fmt::Display for CustomError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.msg)
        }
    }

    impl Error for CustomError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source.as_deref()
        }
    }

    #[test]
    fn test_display_source_chain() {
        let root_error = CustomError {
            msg: "Root IO error",
            source: None,
        };
        let middle_error = CustomError {
            msg: "Middle error",
            source: Some(Box::new(root_error)),
        };
        let generic_error = CustomError {
            msg: "Generic error",
            source: Some(Box::new(middle_error)),
        };

        let display_chain = DisplaySourceChain {
            err: generic_error,
            error_name: "IOError".to_string(),
        };

        let formatted_output = format!("{display_chain}");
        assert!(formatted_output.eq("Generic error\n          \u{1b}[31m↳\u{1b}[0m Middle error\n           \u{1b}[31m↳\u{1b}[0m Root IO error\n"));
    }
}
