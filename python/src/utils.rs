use std::sync::{Arc, OnceLock};

use deltalake::storage::{ListResult, ObjectStore, ObjectStoreError, ObjectStoreResult, Path};
use futures::future::{join_all, BoxFuture, FutureExt};
use futures::StreamExt;
use pyo3::types::{IntoPyDict, PyAnyMethods, PyModule};
use pyo3::{Bound, PyAny, PyResult, Python, ToPyObject};
use tokio::runtime::Runtime;

#[inline]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    static PID: OnceLock<u32> = OnceLock::new();
    match PID.get() {
        Some(pid) if pid == &std::process::id() => {} // Reuse the static runtime.
        Some(pid) => {
            panic!(
                "Forked process detected - current PID is {} but the tokio runtime was created by {}. The tokio \
                runtime does not support forked processes https://github.com/tokio-rs/tokio/issues/4301. If you are \
                seeing this message while using Python multithreading make sure to use the `spawn` or `forkserver` \
                mode.", 
                pid, std::process::id()
            );
        }
        None => {
            PID.set(std::process::id())
                .expect("Failed to record PID for tokio runtime.");
        }
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
    let warnings_warn = PyModule::import_bound(py, "warnings")?.getattr("warn")?;
    let warning_type = PyModule::import_bound(py, "builtins")?.getattr(warning_type)?;
    let stack_level = stack_level.unwrap_or(1);
    let kwargs: [(&str, Bound<'py, PyAny>); 2] = [
        ("category", warning_type),
        ("stacklevel", stack_level.to_object(py).into_bound(py)),
    ];
    warnings_warn.call((message,), Some(&kwargs.into_py_dict_bound(py)))?;
    Ok(())
}
