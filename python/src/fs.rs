use std::collections::HashMap;

use crate::exception::PyDeltaTableError;
use deltalake::storage::{
    azure::AdlsGen2Backend,
    file::{FileStorageBackend},
    s3::{S3StorageBackend, S3StorageOptions},
};
use pyo3::{exceptions::PyValueError, prelude::*};

// Python supports only the backends provided by delta-rs, but there was never
// a way to pass a custom one anyways.
enum PySupportedStorageBackend {
    Local(FileStorageBackend),
    S3(S3StorageBackend),
    GCS(S3StorageBackend),
    ADLS(AdlsGen2Backend),
}

/// Delta filesystem
/// 
/// Provides a mapping from the Delta filesystems to PyArrow filesystems.
#[pyclass]
struct FileSystem {
    _backend: PySupportedStorageBackend,
    #[pyo3(get)]
    config: HashMap<String, String>
}

#[pymethods]
impl FileSystem {
    #[new]
    fn new(backend: String, config: HashMap<String, String>) -> PyResult<Self> {
        let backend = match backend.as_ref() {
            "S3" => PySupportedStorageBackend::S3(
                S3StorageBackend::new_from_options(S3StorageOptions::from_map(config))
                    .map_err(PyDeltaTableError::from_storage)?,
            ),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid backend {}",
                    backend
                )))
            }
        };
        Ok(FileSystem { _backend: backend, config })
    }

    #[classmethod]
    fn from_uri(uri: String, config: HashMap<String, String>) -> PyResult<Self> {
        let backend = match deltalake::parse_uri(&uri)
            .map_err(|err| PyDeltaTableError::from_storage(deltalake::StorageError::Uri{source: err} ))? {
            deltalake::Uri::LocalPath(root) => {
                if !config.is_empty() {
                    return Err(PyValueError::new_err("Local filesystem doesn't support config arguments."));
                }
                PySupportedStorageBackend::Local(FileStorageBackend::new(root))
            }
            #[cfg(any(feature = "s3", feature = "s3-rustls"))]
            deltalake::Uri::S3Object(_) => PySupportedStorageBackend::S3(
                S3StorageBackend::new_from_options(S3StorageOptions::from_map(config))
                    .map_err(PyDeltaTableError::from_storage)?,
            ),
            #[cfg(feature = "azure")]
            deltalake::Uri::AdlsGen2Object(obj) => PySupportedStorageBackend::ADLS(
                // TODO: Support passing in config options https://github.com/delta-io/delta-rs/issues/555
                AdlsGen2Backend::new(obj.file_system).map_err(PyDeltaTableError::from_raw)?,
            ),
            #[cfg(feature = "gcs")]
            deltalake::Uri::GCSObject(_) => PySupportedStorageBackend::GCS(
                // TODO: if credentials path is passed in config, use GCSStorageBackend::try_from(cred_path)
                GCSStorageBackend::new().map_err(PyDeltaTableError::from_raw)?,
            ),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Could not find valid backend for given uri"
                )))
            }
        };

        Ok(FileSystem { _backend: backend, config })
    }

    #[classmethod]
    fn from_pyarrow(py_fs: PyAny) -> PyResult<Self> {
        let fs_module = PyModule::import(py, "pyarrow.fs")?;
        let local_fs = fs_module.getattr("LocalFileSystem")?;
        let s3_fs = fs_module.getattr("S3FileSystem")?;
        if py_fs.is_instance(local_fs)? {
            let backend = PySupportedStorageBackend::Local(FileStorageBackend::new());
            Ok(FileSystem { _backend: backend, config: HashMap::new() })
        } else if py_fs.is_instance(s3_fs)? {
            let backend = PySupportedStorageBackend::S3(S3StorageBackend::new());
            // TODO: Parse config
            Ok(FileSystem { _backend: backend, config: HashMap::new() })
        } else {
            Err(PyValueError::new_err(format!(
                "Unsupported FileSystem"
            )))
        }
    }

    fn to_pyarrow(&self) -> PyResult<PyObject> {

        // Waiting on ABS impl: https://issues.apache.org/jira/browse/ARROW-2034

        // Waiting on GCS Arrow impl: https://issues.apache.org/jira/browse/ARROW-14892
    }

    #[getter]
    fn backend(&self) -> PyResult<String> {
        match self._backend {
            PySupportedStorageBackend::Local(_) => Ok("local".to_string()),
            PySupportedStorageBackend::S3(_) => Ok("S3".to_string()),
            PySupportedStorageBackend::GCS(_) => Ok("GCS".to_string()),
            PySupportedStorageBackend::ADLS(_) => Ok("ADLS".to_string()),
        }
    }
}
