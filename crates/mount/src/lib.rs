use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::DeltaIOStorageBackend;
use deltalake_core::logstore::{
    config::str_is_truthy, default_logstore, logstore_factories, object_store_factories, LogStore,
    LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::local::LocalFileSystem;
use object_store::DynObjectStore;
use object_store::RetryConfig;
use tokio::runtime::Handle;
use url::Url;

mod config;
pub mod error;
mod file;

trait MountOptions {
    fn as_mount_options(&self) -> HashMap<config::MountConfigKey, String>;
}

impl MountOptions for HashMap<String, String> {
    fn as_mount_options(&self) -> HashMap<config::MountConfigKey, String> {
        self.iter()
            .filter_map(|(key, value)| {
                Some((
                    config::MountConfigKey::from_str(&key.to_ascii_lowercase()).ok()?,
                    value.clone(),
                ))
            })
            .collect()
    }
}

#[derive(Clone, Default, Debug)]
pub struct MountFactory {}

impl ObjectStoreFactory for MountFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        _retry: &RetryConfig,
        handle: Option<Handle>,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::MountConfigHelper::try_new(options.as_mount_options())?.build()?;

        let allow_unsafe_rename = str_is_truthy(
            config
                .get(&config::MountConfigKey::AllowUnsafeRename)
                .unwrap_or(&String::new()),
        );

        let (mut store, prefix) = match url.scheme() {
            "dbfs" => {
                if !allow_unsafe_rename {
                    // Just let the user know that they need to set the allow_unsafe_rename option
                    return Err(error::Error::AllowUnsafeRenameNotSpecified.into());
                }
                // We need to convert the dbfs url to a file url
                Url::parse(&format!("file:///dbfs{}", url.path())).unwrap();
                let store = Arc::new(file::MountFileStorageBackend::try_new()?) as ObjectStoreRef;
                Ok((store, Path::from("/")))
            }
            "file" => {
                if allow_unsafe_rename {
                    let store =
                        Arc::new(file::MountFileStorageBackend::try_new()?) as ObjectStoreRef;
                    let prefix = Path::from_filesystem_path(url.to_file_path().unwrap())?;
                    Ok((store, prefix))
                } else {
                    let store = Arc::new(LocalFileSystem::new()) as ObjectStoreRef;
                    let prefix = Path::from_filesystem_path(url.to_file_path().unwrap())?;
                    Ok((store, prefix))
                }
            }
            _ => Err(DeltaTableError::InvalidTableLocation(url.clone().into())),
        }?;

        if let Some(handle) = handle {
            store = Arc::new(DeltaIOStorageBackend::new(store, handle)) as Arc<DynObjectStore>;
        }
        Ok((store, prefix))
    }
}

impl LogStoreFactory for MountFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register an [ObjectStoreFactory] for common Mount [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(MountFactory {});
    for scheme in ["dbfs", "file"].iter() {
        let url = Url::parse(&format!("{scheme}://")).unwrap();
        object_store_factories().insert(url.clone(), factory.clone());
        logstore_factories().insert(url.clone(), factory.clone());
    }
}
