use std::sync::Arc;

use deltalake::storage::{ListResult, ObjectStore, ObjectStoreError, ObjectStoreResult, Path};
use futures::future::{join_all, BoxFuture, FutureExt};
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

#[inline]
pub fn rt() -> PyResult<tokio::runtime::Runtime> {
    Runtime::new().map_err(|_| PyRuntimeError::new_err("Couldn't start a new tokio runtime."))
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
