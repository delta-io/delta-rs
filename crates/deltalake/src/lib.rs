/**
 * The deltalake crate is currently just a meta-package shim for deltalake-core
 */
pub use deltalake_core::*;

#[cfg(not(any(feature = "rustls", feature = "native-tls")))]
compile_error!("You must enable at least one of the features: `rustls` or `native-tls`.");

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub use deltalake_aws as aws;
#[cfg(feature = "azure")]
pub use deltalake_azure as azure;
#[cfg(feature = "unity-experimental")]
pub use deltalake_catalog_unity as unity_catalog;
#[cfg(feature = "gcs")]
pub use deltalake_gcp as gcp;
#[cfg(feature = "hdfs")]
pub use deltalake_hdfs as hdfs;
#[cfg(feature = "lakefs")]
pub use deltalake_lakefs as lakefs;
