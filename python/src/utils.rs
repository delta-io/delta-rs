use std::sync::{Arc, OnceLock};

use deltalake::logstore::object_store::{
    path::Path, Error as ObjectStoreError, ListResult, ObjectStore, Result as ObjectStoreResult,
};
use futures::future::{join_all, BoxFuture, FutureExt};
use futures::StreamExt;
use pyo3::types::{IntoPyDict, PyAnyMethods, PyModule};
use pyo3::{Bound, IntoPyObjectExt, PyAny, PyResult, Python};
use tokio::runtime::Runtime;

#[inline]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    static PID: OnceLock<u32> = OnceLock::new();
    let pid = std::process::id();
    let runtime_pid = *PID.get_or_init(|| pid);
    if pid != runtime_pid {
        panic!(
            "Forked process detected - current PID is {pid} but the tokio runtime was created by {runtime_pid}. The tokio \
            runtime does not support forked processes https://github.com/tokio-rs/tokio/issues/4301. If you are \
            seeing this message while using Python multithreading make sure to use the `spawn` or `forkserver` \
            mode.",
        );
    }
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}

/// walk the "directory" tree along common prefixes in object store
pub async fn walk_tree(
    storage: Arc<dyn ObjectStore>,
    path: &Path,
    recursive: bool,
) -> ObjectStoreResult<ListResult> {
    list_with_delimiter_recursive(storage, [path.clone()], recursive).await
}

fn list_with_delimiter_recursive(
    storage: Arc<dyn ObjectStore>,
    paths: impl IntoIterator<Item = Path>,
    recursive: bool,
) -> BoxFuture<'static, ObjectStoreResult<ListResult>> {
    let mut tasks = vec![];
    for path in paths {
        let store = storage.clone();
        let prefix = path.clone();
        let handle =
            tokio::task::spawn(async move { store.list_with_delimiter(Some(&prefix)).await });
        tasks.push(handle);
    }

    async move {
        let mut results = join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| ObjectStoreError::JoinError { source: err })?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .fold(
                ListResult {
                    common_prefixes: vec![],
                    objects: vec![],
                },
                |mut acc, res| {
                    acc.common_prefixes.extend(res.common_prefixes);
                    acc.objects.extend(res.objects);
                    acc
                },
            );

        if recursive && !results.common_prefixes.is_empty() {
            let more_result = list_with_delimiter_recursive(
                storage.clone(),
                results.common_prefixes.clone(),
                recursive,
            )
            .await?;
            results.common_prefixes.extend(more_result.common_prefixes);
            results.objects.extend(more_result.objects);
        }

        Ok(results)
    }
    .boxed()
}

pub async fn delete_dir(storage: &dyn ObjectStore, prefix: &Path) -> ObjectStoreResult<()> {
    // TODO batch delete would be really useful now...
    let mut stream = storage.list(Some(prefix));
    while let Some(maybe_meta) = stream.next().await {
        let meta = maybe_meta?;
        storage.delete(&meta.location).await?;
    }
    Ok(())
}

pub fn warn<'py>(
    py: Python<'py>,
    warning_type: &str,
    message: &str,
    stack_level: Option<u8>,
) -> PyResult<()> {
    let warnings_warn = PyModule::import(py, "warnings")?.getattr("warn")?;
    let warning_type = PyModule::import(py, "builtins")?.getattr(warning_type)?;
    let stack_level = stack_level.unwrap_or(1);
    let kwargs: [(&str, Bound<'py, PyAny>); 2] = [
        ("category", warning_type),
        ("stacklevel", stack_level.into_py_any(py)?.into_bound(py)),
    ];
    warnings_warn.call((message,), Some(&kwargs.into_py_dict(py)?))?;
    Ok(())
}
