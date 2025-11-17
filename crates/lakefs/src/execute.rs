use async_trait::async_trait;
use deltalake_core::{
    logstore::{LogStore as _, LogStoreRef},
    operations::CustomExecuteHandler,
    DeltaResult, DeltaTableError,
};
use tracing::debug;
use uuid::Uuid;

use crate::logstore::LakeFSLogStore;

pub struct LakeFSCustomExecuteHandler {}

#[async_trait]
impl CustomExecuteHandler for LakeFSCustomExecuteHandler {
    // LakeFS Log store pre execution of delta operation (create branch, logs object store and transaction)
    async fn pre_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()> {
        debug!("Running LakeFS pre execution inside delta operation");
        if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>() {
            lakefs_store.pre_execute(operation_id).await
        } else {
            Err(DeltaTableError::generic(
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
            ))
        }
    }
    // Not required for LakeFS
    async fn post_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()> {
        debug!("Running LakeFS post execution inside delta operation");
        if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>() {
            let (repo, _, _) = lakefs_store
                .client
                .decompose_url(lakefs_store.config().location.to_string());
            let result = lakefs_store
                .client
                .delete_branch(repo, lakefs_store.client.get_transaction(operation_id)?)
                .await
                .map_err(|e| DeltaTableError::Transaction { source: e });
            lakefs_store.client.clear_transaction(operation_id);
            result
        } else {
            Err(DeltaTableError::generic(
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
            ))
        }
    }

    // Execute arbitrary code at the start of the post commit hook
    async fn before_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operations: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()> {
        if file_operations {
            debug!("Running LakeFS pre execution inside post_commit_hook");
            if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>()
            {
                lakefs_store.pre_execute(operation_id).await
            } else {
                Err(DeltaTableError::generic(
                    "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
                ))
            }?;
        }
        Ok(())
    }

    // Execute arbitrary code at the end of the post commit hook
    async fn after_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operations: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()> {
        if file_operations {
            debug!("Running LakeFS post execution inside post_commit_hook");
            if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>()
            {
                lakefs_store.commit_merge(operation_id).await
            } else {
                Err(DeltaTableError::generic(
                    "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
                ))
            }?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::register_handlers;

    use super::*;
    use deltalake_core::logstore::{logstore_for, ObjectStoreRegistry, StorageConfig};
    use http::StatusCode;
    use maplit::hashmap;
    use std::sync::OnceLock;
    use tokio::runtime::Runtime;
    use url::Url;
    use uuid::Uuid;

    fn setup_env(server_url: String) -> LogStoreRef {
        register_handlers(None);
        let location = Url::parse("lakefs://repo/branch/table").unwrap();
        let raw_options = hashmap! {
            "ACCESS_KEY_ID".to_string() => "options_key".to_string(),
            "ENDPOINT_URL".to_string() => server_url,
            "SECRET_ACCESS_KEY".to_string() => "options_key".to_string(),
            "REGION".to_string() => "options_key".to_string()
        };

        let storage_config = StorageConfig::parse_options(raw_options).unwrap();
        logstore_for(location, storage_config).unwrap()
    }

    #[inline]
    fn rt() -> &'static Runtime {
        static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
        TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
    }

    #[test]
    fn test_pre_execute() {
        let handler = LakeFSCustomExecuteHandler {};
        let operation_id = Uuid::new_v4();

        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/api/v1/repositories/repo/branches")
            .with_status(StatusCode::CREATED.as_u16().into())
            .with_body("")
            .create();

        let lakefs_store = setup_env(server.url());

        let result =
            rt().block_on(async { handler.pre_execute(&lakefs_store, operation_id).await });
        mock.assert();
        assert!(result.is_ok());

        if let Some(lakefs_store) = lakefs_store
            .clone()
            .as_any()
            .downcast_ref::<LakeFSLogStore>()
        {
            assert!(lakefs_store
                .prefixed_registry
                .get_store(
                    &Url::parse(format!("lakefs://repo/delta-tx-{operation_id}/table").as_str())
                        .unwrap()
                )
                .is_ok());

            assert!(lakefs_store.client.get_transaction(operation_id).is_ok())
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_before_post_commit_hook() {
        let handler = LakeFSCustomExecuteHandler {};
        let operation_id = Uuid::new_v4();

        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/api/v1/repositories/repo/branches")
            .with_status(StatusCode::CREATED.as_u16().into())
            .with_body("")
            .create();

        let lakefs_store = setup_env(server.url());

        let result = rt().block_on(async {
            handler
                .before_post_commit_hook(&lakefs_store, true, operation_id)
                .await
        });
        mock.assert();
        assert!(result.is_ok());

        if let Some(lakefs_store) = lakefs_store
            .clone()
            .as_any()
            .downcast_ref::<LakeFSLogStore>()
        {
            assert!(lakefs_store
                .prefixed_registry
                .get_store(
                    &Url::parse(format!("lakefs://repo/delta-tx-{operation_id}/table").as_str())
                        .unwrap()
                )
                .is_ok());

            assert!(lakefs_store.client.get_transaction(operation_id).is_ok())
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_post_execute() {
        let handler = LakeFSCustomExecuteHandler {};
        let operation_id = Uuid::new_v4();

        let mut server = mockito::Server::new();
        let mock_1 = server
            .mock("POST", "/api/v1/repositories/repo/branches")
            .with_status(StatusCode::CREATED.as_u16().into())
            .with_body("")
            .create();
        let mock_2 = server
            .mock(
                "DELETE",
                format!("/api/v1/repositories/repo/branches/delta-tx-{operation_id}").as_str(),
            )
            .with_status(StatusCode::NO_CONTENT.as_u16().into())
            .create();

        let lakefs_store = setup_env(server.url());

        rt().block_on(async { handler.pre_execute(&lakefs_store, operation_id).await })
            .unwrap();
        mock_1.assert();

        let result =
            rt().block_on(async { handler.post_execute(&lakefs_store, operation_id).await });

        assert!(result.is_ok());
        mock_2.assert();

        if let Some(lakefs_store) = lakefs_store
            .clone()
            .as_any()
            .downcast_ref::<LakeFSLogStore>()
        {
            assert!(lakefs_store.client.get_transaction(operation_id).is_err())
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_after_post_commit_hook() {
        let handler = LakeFSCustomExecuteHandler {};
        let operation_id = Uuid::new_v4();

        let mut server = mockito::Server::new();
        let create_branch_mock = server
            .mock("POST", "/api/v1/repositories/repo/branches")
            .with_status(StatusCode::CREATED.as_u16().into())
            .with_body("")
            .create();

        let create_commit_mock = server
            .mock(
                "POST",
                format!("/api/v1/repositories/repo/branches/delta-tx-{operation_id}/commits")
                    .as_str(),
            )
            .with_status(StatusCode::CREATED.as_u16().into())
            .create();

        let diff_mock = server
            .mock(
                "GET",
                format!("/api/v1/repositories/repo/refs/branch/diff/delta-tx-{operation_id}")
                    .as_str(),
            )
            .with_status(StatusCode::OK.as_u16().into())
            .with_body(r#"{"results": [{"some": "change"}]}"#)
            .create();

        let merge_branch_mock = server
            .mock(
                "POST",
                format!("/api/v1/repositories/repo/refs/delta-tx-{operation_id}/merge/branch")
                    .as_str(),
            )
            .with_status(StatusCode::OK.as_u16().into())
            .create();

        let delete_branch_mock = server
            .mock(
                "DELETE",
                format!("/api/v1/repositories/repo/branches/delta-tx-{operation_id}").as_str(),
            )
            .with_status(StatusCode::NO_CONTENT.as_u16().into())
            .create();

        let lakefs_store = setup_env(server.url());

        let result = rt().block_on(async {
            handler
                .before_post_commit_hook(&lakefs_store, true, operation_id)
                .await
        });
        create_branch_mock.assert();
        assert!(result.is_ok());

        let result = rt().block_on(async {
            handler
                .after_post_commit_hook(&lakefs_store, true, operation_id)
                .await
        });

        create_commit_mock.assert();
        diff_mock.assert();
        merge_branch_mock.assert();
        delete_branch_mock.assert();
        assert!(result.is_ok());

        if let Some(lakefs_store) = lakefs_store
            .clone()
            .as_any()
            .downcast_ref::<LakeFSLogStore>()
        {
            assert!(lakefs_store.client.get_transaction(operation_id).is_err())
        } else {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_execute_error_with_invalid_log_store() {
        let location = Url::parse("memory:///table").unwrap();
        let invalid_default_store = logstore_for(location, StorageConfig::default()).unwrap();

        let handler = LakeFSCustomExecuteHandler {};
        let operation_id = Uuid::new_v4();

        let result = handler
            .post_execute(&invalid_default_store, operation_id)
            .await;
        assert!(result.is_err());
        if let Err(DeltaTableError::Generic(err)) = result {
            assert_eq!(
                err,
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found"
            );
        }

        let result = handler
            .pre_execute(&invalid_default_store, operation_id)
            .await;
        assert!(result.is_err());
        if let Err(DeltaTableError::Generic(err)) = result {
            assert_eq!(
                err,
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found"
            );
        }

        let result = handler
            .before_post_commit_hook(&invalid_default_store, true, operation_id)
            .await;
        assert!(result.is_err());
        if let Err(DeltaTableError::Generic(err)) = result {
            assert_eq!(
                err,
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found"
            );
        }

        let result = handler
            .after_post_commit_hook(&invalid_default_store, true, operation_id)
            .await;
        assert!(result.is_err());
        if let Err(DeltaTableError::Generic(err)) = result {
            assert_eq!(
                err,
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found"
            );
        }
    }

    #[tokio::test]
    async fn test_noop_commit_hook_executor() {
        // When file operations is false, the commit hook executor is a noop, since we don't need
        // to create any branches, or commit and merge them back.
        let location = Url::parse("memory:///table").unwrap();
        let invalid_default_store = logstore_for(location, StorageConfig::default()).unwrap();

        let handler = LakeFSCustomExecuteHandler {};
        let operation_id = Uuid::new_v4();

        let result = handler
            .before_post_commit_hook(&invalid_default_store, false, operation_id)
            .await;
        assert!(result.is_ok());

        let result = handler
            .after_post_commit_hook(&invalid_default_store, false, operation_id)
            .await;
        assert!(result.is_ok());
    }
}
