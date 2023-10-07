use std::collections::HashMap;
use std::sync::Arc;

use deltalake::storage::{DynObjectStore, ListResult, MultipartId, ObjectStoreError, Path};
use deltalake::DeltaTableBuilder;
use pyo3::exceptions::{PyIOError, PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBytes};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::runtime::Runtime;

use crate::error::PythonError;
use crate::utils::{delete_dir, rt, walk_tree};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FsConfig {
    pub(crate) root_url: String,
    pub(crate) options: HashMap<String, String>,
}

#[pyclass(subclass, module = "deltalake._internal")]
#[derive(Debug, Clone)]
pub struct DeltaFileSystemHandler {
    pub(crate) inner: Arc<DynObjectStore>,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) config: FsConfig,
    pub(crate) known_sizes: Option<HashMap<String, i64>>,
}

impl DeltaFileSystemHandler {
    fn parse_path(path: &str) -> Path {
        // Path::from will percent-encode the input, while Path::parse won't. So
        // we should prefer Path::parse.
        match Path::parse(path) {
            Ok(path) => path,
            Err(_) => Path::from(path),
        }
    }
}

#[pymethods]
impl DeltaFileSystemHandler {
    #[new]
    #[pyo3(signature = (table_uri, options = None, known_sizes = None))]
    fn new(
        table_uri: &str,
        options: Option<HashMap<String, String>>,
        known_sizes: Option<HashMap<String, i64>>,
    ) -> PyResult<Self> {
        let storage = DeltaTableBuilder::from_uri(table_uri)
            .with_storage_options(options.clone().unwrap_or_default())
            .build_storage()
            .map_err(PythonError::from)?;
        Ok(Self {
            inner: storage,
            rt: Arc::new(rt()?),
            config: FsConfig {
                root_url: table_uri.into(),
                options: options.unwrap_or_default(),
            },
            known_sizes,
        })
    }

    fn get_type_name(&self) -> String {
        "object-store".into()
    }

    fn normalize_path(&self, path: String) -> PyResult<String> {
        let suffix = if path.ends_with('/') { "/" } else { "" };
        let path = Path::parse(path).unwrap();
        Ok(format!("{path}{suffix}"))
    }

    fn copy_file(&self, src: String, dest: String) -> PyResult<()> {
        let from_path = Self::parse_path(&src);
        let to_path = Self::parse_path(&dest);
        self.rt
            .block_on(self.inner.copy(&from_path, &to_path))
            .map_err(PythonError::from)?;
        Ok(())
    }

    fn create_dir(&self, _path: String, _recursive: bool) -> PyResult<()> {
        // TODO creating a dir should be a no-op with object_store, right?
        Ok(())
    }

    fn delete_dir(&self, path: String) -> PyResult<()> {
        let path = Self::parse_path(&path);
        self.rt
            .block_on(delete_dir(self.inner.as_ref(), &path))
            .map_err(PythonError::from)?;
        Ok(())
    }

    fn delete_file(&self, path: String) -> PyResult<()> {
        let path = Self::parse_path(&path);
        self.rt
            .block_on(self.inner.delete(&path))
            .map_err(PythonError::from)?;
        Ok(())
    }

    fn equals(&self, other: &DeltaFileSystemHandler) -> PyResult<bool> {
        Ok(format!("{self:?}") == format!("{other:?}"))
    }

    fn get_file_info<'py>(&self, paths: Vec<String>, py: Python<'py>) -> PyResult<Vec<&'py PyAny>> {
        let fs = PyModule::import(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info = |loc: &str, type_: &PyAny, kwargs: &HashMap<&str, i64>| {
            fs.call_method("FileInfo", (loc, type_), Some(kwargs.into_py_dict(py)))
        };

        let mut infos = Vec::new();
        for file_path in paths {
            let path = Self::parse_path(&file_path);
            let listed = py.allow_threads(|| {
                self.rt
                    .block_on(self.inner.list_with_delimiter(Some(&path)))
                    .map_err(PythonError::from)
            })?;

            // TODO is there a better way to figure out if we are in a directory?
            if listed.objects.is_empty() && listed.common_prefixes.is_empty() {
                let maybe_meta = py.allow_threads(|| self.rt.block_on(self.inner.head(&path)));
                match maybe_meta {
                    Ok(meta) => {
                        let kwargs = HashMap::from([
                            ("size", meta.size as i64),
                            (
                                "mtime_ns",
                                meta.last_modified.timestamp_nanos_opt().ok_or(
                                    PyValueError::new_err("last modified datetime out of range"),
                                )?,
                            ),
                        ]);
                        infos.push(to_file_info(
                            meta.location.as_ref(),
                            file_types.getattr("File")?,
                            &kwargs,
                        )?);
                    }
                    Err(ObjectStoreError::NotFound { .. }) => {
                        infos.push(to_file_info(
                            path.as_ref(),
                            file_types.getattr("NotFound")?,
                            &HashMap::new(),
                        )?);
                    }
                    Err(err) => {
                        return Err(PythonError::from(err).into());
                    }
                }
            } else {
                infos.push(to_file_info(
                    path.as_ref(),
                    file_types.getattr("Directory")?,
                    &HashMap::new(),
                )?);
            }
        }

        Ok(infos)
    }

    #[pyo3(signature = (base_dir, allow_not_found = false, recursive = false))]
    fn get_file_info_selector<'py>(
        &self,
        base_dir: String,
        allow_not_found: bool,
        recursive: bool,
        py: Python<'py>,
    ) -> PyResult<Vec<&'py PyAny>> {
        let fs = PyModule::import(py, "pyarrow.fs")?;
        let file_types = fs.getattr("FileType")?;

        let to_file_info = |loc: String, type_: &PyAny, kwargs: HashMap<&str, i64>| {
            fs.call_method("FileInfo", (loc, type_), Some(kwargs.into_py_dict(py)))
        };

        let path = Self::parse_path(&base_dir);
        let list_result = match self
            .rt
            .block_on(walk_tree(self.inner.clone(), &path, recursive))
        {
            Ok(res) => Ok(res),
            Err(ObjectStoreError::NotFound { path, source }) => {
                if allow_not_found {
                    Ok(ListResult {
                        common_prefixes: vec![],
                        objects: vec![],
                    })
                } else {
                    Err(ObjectStoreError::NotFound { path, source })
                }
            }
            Err(err) => Err(err),
        }
        .map_err(PythonError::from)?;

        let mut infos = vec![];
        infos.extend(
            list_result
                .common_prefixes
                .into_iter()
                .map(|p| {
                    to_file_info(
                        p.to_string(),
                        file_types.getattr("Directory")?,
                        HashMap::new(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        infos.extend(
            list_result
                .objects
                .into_iter()
                .map(|meta| {
                    let kwargs = HashMap::from([
                        ("size", meta.size as i64),
                        (
                            "mtime_ns",
                            meta.last_modified.timestamp_nanos_opt().ok_or(
                                PyValueError::new_err("last modified datetime out of range"),
                            )?,
                        ),
                    ]);
                    to_file_info(
                        meta.location.to_string(),
                        file_types.getattr("File")?,
                        kwargs,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        );

        Ok(infos)
    }

    fn move_file(&self, src: String, dest: String) -> PyResult<()> {
        let from_path = Self::parse_path(&src);
        let to_path = Self::parse_path(&dest);
        // TODO check the if not exists semantics
        self.rt
            .block_on(self.inner.rename(&from_path, &to_path))
            .map_err(PythonError::from)?;
        Ok(())
    }

    fn open_input_file(&self, path: String) -> PyResult<ObjectInputFile> {
        let size = match &self.known_sizes {
            Some(sz) => sz.get(&path),
            None => None,
        };

        let path = Self::parse_path(&path);
        let file = self
            .rt
            .block_on(ObjectInputFile::try_new(
                Arc::clone(&self.rt),
                self.inner.clone(),
                path,
                size.copied(),
            ))
            .map_err(PythonError::from)?;
        Ok(file)
    }

    #[pyo3(signature = (path, metadata = None))]
    fn open_output_stream(
        &self,
        path: String,
        #[allow(unused)] metadata: Option<HashMap<String, String>>,
    ) -> PyResult<ObjectOutputStream> {
        let path = Self::parse_path(&path);
        let file = self
            .rt
            .block_on(ObjectOutputStream::try_new(
                Arc::clone(&self.rt),
                self.inner.clone(),
                path,
            ))
            .map_err(PythonError::from)?;
        Ok(file)
    }

    pub fn __getnewargs__(&self) -> PyResult<(String, Option<HashMap<String, String>>)> {
        Ok((
            self.config.root_url.clone(),
            Some(self.config.options.clone()),
        ))
    }
}

// TODO the C++ implementation track an internal lock on all random access files, DO we need this here?
// TODO add buffer to store data ...
#[pyclass(weakref, module = "deltalake._internal")]
#[derive(Debug, Clone)]
pub struct ObjectInputFile {
    store: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    path: Path,
    content_length: i64,
    #[pyo3(get)]
    closed: bool,
    pos: i64,
    #[pyo3(get)]
    mode: String,
}

impl ObjectInputFile {
    pub async fn try_new(
        rt: Arc<Runtime>,
        store: Arc<DynObjectStore>,
        path: Path,
        size: Option<i64>,
    ) -> Result<Self, ObjectStoreError> {
        // If file size is not given, issue a HEAD Object to get the content-length and ensure any
        // errors (e.g. file not found) don't wait until the first read() call.
        let content_length = match size {
            Some(s) => s,
            None => {
                let meta = store.head(&path).await?;
                meta.size as i64
            }
        };

        // TODO make sure content length is valid
        // https://github.com/apache/arrow/blob/f184255cbb9bf911ea2a04910f711e1a924b12b8/cpp/src/arrow/filesystem/s3fs.cc#L1083
        Ok(Self {
            store,
            rt,
            path,
            content_length,
            closed: false,
            pos: 0,
            mode: "rb".into(),
        })
    }

    fn check_closed(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyIOError::new_err("Operation on closed stream"));
        }

        Ok(())
    }

    fn check_position(&self, position: i64, action: &str) -> PyResult<()> {
        if position < 0 {
            return Err(PyIOError::new_err(format!(
                "Cannot {action} for negative position."
            )));
        }
        if position > self.content_length {
            return Err(PyIOError::new_err(format!(
                "Cannot {action} past end of file."
            )));
        }
        Ok(())
    }
}

#[pymethods]
impl ObjectInputFile {
    fn close(&mut self) -> PyResult<()> {
        self.closed = true;
        Ok(())
    }

    fn isatty(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn readable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn seekable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn writable(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn tell(&self) -> PyResult<i64> {
        self.check_closed()?;
        Ok(self.pos)
    }

    fn size(&self) -> PyResult<i64> {
        self.check_closed()?;
        Ok(self.content_length)
    }

    #[pyo3(signature = (offset, whence = 0))]
    fn seek(&mut self, offset: i64, whence: i64) -> PyResult<i64> {
        self.check_closed()?;
        self.check_position(offset, "seek")?;
        match whence {
            // reference is start of the stream (the default); offset should be zero or positive
            0 => {
                self.pos = offset;
            }
            // reference is current stream position; offset may be negative
            1 => {
                self.pos += offset;
            }
            // reference is  end of the stream; offset is usually negative
            2 => {
                self.pos = self.content_length + offset;
            }
            _ => {
                return Err(PyValueError::new_err(
                    "'whence' must be between  0 <= whence <= 2.",
                ));
            }
        }
        Ok(self.pos)
    }

    #[pyo3(signature = (nbytes = None))]
    fn read(&mut self, nbytes: Option<i64>, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        self.check_closed()?;
        let range = match nbytes {
            Some(len) => {
                let end = i64::min(self.pos + len, self.content_length) as usize;
                std::ops::Range {
                    start: self.pos as usize,
                    end,
                }
            }
            _ => std::ops::Range {
                start: self.pos as usize,
                end: self.content_length as usize,
            },
        };
        let nbytes = (range.end - range.start) as i64;
        self.pos += nbytes;
        let data = if nbytes > 0 {
            py.allow_threads(|| {
                self.rt
                    .block_on(self.store.get_range(&self.path, range))
                    .map_err(PythonError::from)
            })?
        } else {
            "".into()
        };
        // TODO: PyBytes copies the buffer. If we move away from the limited CPython
        // API (the stable C API), we could implement the buffer protocol for
        // bytes::Bytes and return this zero-copy.
        Ok(PyBytes::new(py, data.as_ref()).into_py(py))
    }

    fn fileno(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'fileno' not implemented"))
    }

    fn truncate(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'truncate' not implemented"))
    }

    fn readline(&self, _size: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'readline' not implemented"))
    }

    fn readlines(&self, _hint: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "'readlines' not implemented",
        ))
    }
}

// TODO the C++ implementation track an internal lock on all random access files, DO we need this here?
// TODO add buffer to store data ...
#[pyclass(weakref, module = "deltalake._internal")]
pub struct ObjectOutputStream {
    store: Arc<DynObjectStore>,
    rt: Arc<Runtime>,
    path: Path,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    multipart_id: MultipartId,
    pos: i64,
    #[pyo3(get)]
    closed: bool,
    #[pyo3(get)]
    mode: String,
}

impl ObjectOutputStream {
    pub async fn try_new(
        rt: Arc<Runtime>,
        store: Arc<DynObjectStore>,
        path: Path,
    ) -> Result<Self, ObjectStoreError> {
        let (multipart_id, writer) = store.put_multipart(&path).await?;
        Ok(Self {
            store,
            rt,
            path,
            writer,
            multipart_id,
            pos: 0,
            closed: false,
            mode: "wb".into(),
        })
    }

    fn check_closed(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyIOError::new_err("Operation on closed stream"));
        }

        Ok(())
    }
}

#[pymethods]
impl ObjectOutputStream {
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        self.closed = true;
        py.allow_threads(|| match self.rt.block_on(self.writer.shutdown()) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.rt
                    .block_on(self.store.abort_multipart(&self.path, &self.multipart_id))
                    .map_err(PythonError::from)?;
                Err(PyIOError::new_err(err.to_string()))
            }
        })
    }

    fn isatty(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn readable(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn seekable(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn writable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn tell(&self) -> PyResult<i64> {
        self.check_closed()?;
        Ok(self.pos)
    }

    fn size(&self) -> PyResult<i64> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'size' not implemented"))
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (offset, whence = 0))]
    fn seek(&mut self, offset: i64, whence: i64) -> PyResult<i64> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'seek' not implemented"))
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (nbytes = None))]
    fn read(&mut self, nbytes: Option<i64>) -> PyResult<()> {
        self.check_closed()?;
        Err(PyNotImplementedError::new_err("'read' not implemented"))
    }

    fn write(&mut self, data: &PyBytes) -> PyResult<i64> {
        self.check_closed()?;
        let len = data.as_bytes().len() as i64;
        let py = data.py();
        let data = data.as_bytes();
        py.allow_threads(|| match self.rt.block_on(self.writer.write_all(data)) {
            Ok(_) => Ok(len),
            Err(err) => {
                self.rt
                    .block_on(self.store.abort_multipart(&self.path, &self.multipart_id))
                    .map_err(PythonError::from)?;
                Err(PyIOError::new_err(err.to_string()))
            }
        })
    }

    fn flush(&mut self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| match self.rt.block_on(self.writer.flush()) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.rt
                    .block_on(self.store.abort_multipart(&self.path, &self.multipart_id))
                    .map_err(PythonError::from)?;
                Err(PyIOError::new_err(err.to_string()))
            }
        })
    }

    fn fileno(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'fileno' not implemented"))
    }

    fn truncate(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'truncate' not implemented"))
    }

    fn readline(&self, _size: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("'readline' not implemented"))
    }

    fn readlines(&self, _hint: Option<i64>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "'readlines' not implemented",
        ))
    }
}
