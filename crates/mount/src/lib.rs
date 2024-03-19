use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, str_is_truthy, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::local::LocalFileSystem;
use url::Url;

mod config;
pub mod error;
mod file;

trait MountOptions {
    fn as_mount_options(&self) -> HashMap<config::MountConfigKey, String>;
}

impl MountOptions for StorageOptions {
    fn as_mount_options(&self) -> HashMap<config::MountConfigKey, String> {
        self.0
            .iter()
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
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::MountConfigHelper::try_new(options.as_mount_options())?.build()?;

        let allow_unsafe_rename = str_is_truthy(
            config
                .get(&config::MountConfigKey::AllowUnsafeRename)
                .unwrap_or(&String::new()),
        );

        match url.scheme() {
            "dbfs" => {
                if !allow_unsafe_rename {
                    // Just let the user know that they need to set the allow_unsafe_rename option
                    return Err(error::Error::AllowUnsafeRenameNotSpecified.into());
                }
                // We need to convert the dbfs url to a file url
                let new_url = Url::parse(&format!("file:///dbfs{}", url.path())).unwrap();
                let store = Arc::new(file::MountFileStorageBackend::try_new(
                    new_url.to_file_path().unwrap(),
                )?) as ObjectStoreRef;
                Ok((store, Path::from("/")))
            }
            "file" => {
                if allow_unsafe_rename {
                    let store = Arc::new(file::MountFileStorageBackend::try_new(
                        url.to_file_path().unwrap(),
                    )?) as ObjectStoreRef;
                    Ok((store, Path::from("/")))
                } else {
                    let store = Arc::new(LocalFileSystem::new_with_prefix(
                        url.to_file_path().unwrap(),
                    )?) as ObjectStoreRef;
                    Ok((store, Path::from("/")))
                }
            }
            _ => Err(DeltaTableError::InvalidTableLocation(url.clone().into())),
        }
    }
}

impl LogStoreFactory for MountFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common Mount [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(MountFactory {});
    for scheme in ["dbfs", "file"].iter() {
        let url = Url::parse(&format!("{}://", scheme)).unwrap();
        factories().insert(url.clone(), factory.clone());
        logstores().insert(url.clone(), factory.clone());
    }
}
